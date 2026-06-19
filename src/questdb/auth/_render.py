################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2024 QuestDB
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##  http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

"""
Presentation of the device-flow prompt.

Renders a clickable link + user code in Jupyter (via ``IPython.display``) and
falls back to plain text on a terminal. Nothing here is required for
``token()`` / ``headers()`` to work; ``IPython`` and ``qrcode`` are imported
lazily and only when actually used.
"""

from __future__ import annotations

import html
import re
import sys
import urllib.parse
from typing import Any, Dict, Optional, TextIO


def in_ipython_kernel() -> bool:
    """True when running inside an interactive Jupyter/ZMQ kernel."""
    try:
        from IPython import get_ipython  # type: ignore
    except Exception:
        return False
    ip = get_ipython()
    if ip is None:
        return False
    # ZMQInteractiveShell == notebook / qtconsole / lab; TerminalInteractive
    # Shell == ipython in a terminal (still interactive).
    return ip.__class__.__name__ in (
        'ZMQInteractiveShell', 'TerminalInteractiveShell')


def detect_interactive() -> bool:
    """
    Best-effort detection of whether a human can complete the sign-in.

    Interactive when attached to a TTY or running in an interactive IPython
    shell. This guards against hanging forever in a non-interactive context
    (papermill / cron / CI).
    """
    if in_ipython_kernel():
        return True
    try:
        return bool(sys.stdin and sys.stdin.isatty()
                    and sys.stdout and sys.stdout.isatty())
    except Exception:
        return False


def _verification_uri(resp: Dict[str, Any]) -> str:
    # RFC 8628 uses ``verification_uri``; some IdPs (older Google) use
    # ``verification_url``.
    return resp.get('verification_uri') or resp.get('verification_url') or ''


def _verification_uri_complete(resp: Dict[str, Any]) -> Optional[str]:
    return (resp.get('verification_uri_complete')
            or resp.get('verification_url_complete'))


def _safe_link_url(url: Optional[str]) -> Optional[str]:
    """
    Return ``url`` only if it uses an ``http(s)`` scheme, else ``None``.

    The verification URL comes from the IdP's device-authorization response,
    which is untrusted input. Embedding it in an HTML ``href`` without a scheme
    allowlist would let a malicious/MITM'd response inject a ``javascript:`` or
    ``data:`` URL that executes in the notebook DOM when clicked
    (``html.escape`` guards markup, not the URL scheme).
    """
    if not url:
        return None
    try:
        scheme = urllib.parse.urlparse(url).scheme.lower()
    except (ValueError, TypeError):
        return None
    return url if scheme in ('http', 'https') else None


def _render_link(url: Optional[str], *, text: Optional[str] = None) -> str:
    """
    Render ``url`` as a clickable link, or as inert escaped text if its scheme
    is not ``http(s)``.

    The visible label defaults to the URL itself. When the URL is rejected, the
    (escaped) URL is shown as plain text so the user can still see/copy it, but
    it is never turned into a clickable/executable link.
    """
    safe = _safe_link_url(url)
    label = html.escape(text if text is not None else (url or ''))
    if safe is None:
        return label
    return (f'<a href="{html.escape(safe)}" target="_blank" '
            f'rel="noopener noreferrer">{label}</a>')


# C0/C1 control chars (incl. ESC, which drives ANSI escape sequences) plus the
# Unicode bidi-control, zero-width and line/paragraph-separator ranges. All can
# spoof a terminal prompt: U+202E (RIGHT-TO-LEFT OVERRIDE) reverses displayed
# text to disguise a URL's host; U+2028/U+2029 inject fake lines; zero-width
# chars hide content. Stripped from untrusted device-response fields.
_CONTROL_CHARS = re.compile(
    r'[\x00-\x1f\x7f-\x9f\u200b-\u200f\u2028-\u202e\u2066-\u2069\ufeff]')


def _strip_control(text: Optional[str]) -> str:
    """
    Strip control / format characters from an untrusted string before it is
    written to a terminal.

    The verification URL, user code and IdP error strings come from the device-
    authorization response (untrusted). Writing them verbatim to a TTY would let
    a hostile or MITM'd response inject ANSI escape sequences (C0/C1 control
    chars — cursor moves, screen clears) or Unicode bidi overrides / zero-width
    / line separators to spoof the prompt or hide the real sign-in URL (e.g.
    U+202E visually reverses the displayed host). The Jupyter renderer
    html-escapes its output; the plain-text path needs this.
    """
    if not text:
        return ''
    return _CONTROL_CHARS.sub('', text)


def format_prompt(resp: Dict[str, Any]) -> str:
    """Plain-text sign-in prompt (also used as the notebook fallback)."""
    uri = _strip_control(_verification_uri(resp))
    code = _strip_control(str(resp.get('user_code', '')))
    complete = _strip_control(_verification_uri_complete(resp))
    lines = [
        '🔐 Sign in to QuestDB',
        f'   Open {uri}  and enter code:  {code}',
    ]
    if complete:
        lines.append(f'   (or open directly: {complete})')
    return '\n'.join(lines)


def _fmt_mmss(seconds: float) -> str:
    seconds = max(0, int(seconds))
    return f'{seconds // 60}:{seconds % 60:02d}'


class Renderer:
    """No-op renderer interface; subclasses present the prompt to the user."""

    def on_prompt(self, resp: Dict[str, Any]) -> None:
        pass

    def on_waiting(self, seconds_left: float) -> None:
        pass

    def on_success(self, identity: Optional[str], expires_in: float) -> None:
        pass

    def on_failure(self, message: str) -> None:
        pass


class TerminalRenderer(Renderer):
    """Plain-text rendering for terminals (writes to ``stderr`` by default)."""

    def __init__(self, stream: Optional[TextIO] = None, qr: bool = False):
        self._stream = stream if stream is not None else sys.stderr
        self._qr = qr
        self._countdown_active = False

    def _write(self, text: str) -> None:
        try:
            self._stream.write(text)
            self._stream.flush()
        except Exception:
            pass

    def on_prompt(self, resp: Dict[str, Any]) -> None:
        self._write(format_prompt(resp) + '\n')
        if self._qr:
            target = _verification_uri_complete(resp) or _verification_uri(resp)
            art = _qr_ascii(target)
            if art:
                self._write(art + '\n')

    def on_waiting(self, seconds_left: float) -> None:
        self._countdown_active = True
        self._write(f'\r   ⏳ waiting for authorization… ({_fmt_mmss(seconds_left)} left)   ')

    def on_success(self, identity: Optional[str], expires_in: float) -> None:
        if self._countdown_active:
            self._write('\n')
            self._countdown_active = False
        who = f' as {_strip_control(identity)}' if identity else ''
        mins = max(1, int(round(expires_in / 60)))
        self._write(f'✅ Signed in{who} — token cached, expires in {mins} min\n')

    def on_failure(self, message: str) -> None:
        if self._countdown_active:
            self._write('\n')
            self._countdown_active = False
        self._write(f'❌ {_strip_control(message)}\n')


class JupyterRenderer(Renderer):
    """Rich rendering for Jupyter using an updatable display handle."""

    def __init__(self, qr: bool = False):
        self._qr = qr
        self._handle = None
        self._resp: Dict[str, Any] = {}

    def _display(self, html_str: str):
        from IPython.display import HTML, display  # type: ignore
        if self._handle is None:
            self._handle = display(HTML(html_str), display_id=True)
        else:
            self._handle.update(HTML(html_str))

    def _panel(self, body: str) -> str:
        return (
            '<div style="border:1px solid #ccc;border-radius:8px;'
            'padding:12px 16px;font-family:sans-serif;max-width:520px">'
            + body + '</div>')

    def on_prompt(self, resp: Dict[str, Any]) -> None:
        self._resp = resp
        uri = _verification_uri(resp)
        code = html.escape(str(resp.get('user_code', '')))
        complete = _verification_uri_complete(resp)
        body = [
            '<div style="font-size:1.05em;font-weight:600;margin-bottom:6px">'
            '🔐 Sign in to QuestDB</div>',
            f'<div>Open {_render_link(uri)} and enter code:</div>',
            f'<div style="font-size:1.6em;font-family:monospace;'
            f'letter-spacing:2px;margin:6px 0">{code}</div>',
        ]
        if _safe_link_url(complete):
            body.append(
                '<div>' + _render_link(
                    complete, text='Click here to authorize directly →')
                + '</div>')
        if self._qr:
            qr_target = _safe_link_url(complete) or _safe_link_url(uri)
            data_uri = _qr_data_uri(qr_target) if qr_target else None
            if data_uri:
                body.append(
                    f'<img alt="QR code" src="{data_uri}" '
                    'style="margin-top:8px;width:160px;height:160px"/>')
        body.append(
            '<div id="qdb-oidc-status" style="color:#888;margin-top:8px">'
            '⏳ waiting for authorization…</div>')
        self._display(self._panel(''.join(body)))

    def on_waiting(self, seconds_left: float) -> None:
        # Re-render the whole panel (cheap) with an updated countdown.
        if not self._resp:
            return
        self._resp = dict(self._resp)
        self._render_with_status(
            f'⏳ waiting for authorization… ({_fmt_mmss(seconds_left)} left)',
            color='#888')

    def on_success(self, identity: Optional[str], expires_in: float) -> None:
        who = html.escape(identity) if identity else ''
        mins = max(1, int(round(expires_in / 60)))
        suffix = f' as <b>{who}</b>' if who else ''
        self._render_with_status(
            f'✅ Signed in{suffix} — token cached, expires in {mins} min',
            color='#2e7d32')

    def on_failure(self, message: str) -> None:
        self._render_with_status('❌ ' + html.escape(message), color='#c62828')

    def _render_with_status(self, status_html: str, color: str) -> None:
        resp = self._resp
        uri = _verification_uri(resp)
        code = html.escape(str(resp.get('user_code', '')))
        complete = _verification_uri_complete(resp)
        body = [
            '<div style="font-size:1.05em;font-weight:600;margin-bottom:6px">'
            '🔐 Sign in to QuestDB</div>',
            f'<div>Open {_render_link(uri)} and enter code:</div>',
            f'<div style="font-size:1.6em;font-family:monospace;'
            f'letter-spacing:2px;margin:6px 0">{code}</div>',
        ]
        if _safe_link_url(complete):
            body.append(
                '<div>' + _render_link(
                    complete, text='Click here to authorize directly →')
                + '</div>')
        body.append(
            f'<div style="color:{color};margin-top:8px">{status_html}</div>')
        self._display(self._panel(''.join(body)))


def make_renderer(qr: bool = False) -> Renderer:
    """Pick a renderer appropriate for the current environment."""
    if in_ipython_kernel():
        try:
            import IPython.display  # noqa: F401  # type: ignore
            return JupyterRenderer(qr=qr)
        except Exception:
            pass
    return TerminalRenderer(qr=qr)


def _qr_ascii(data: str) -> Optional[str]:
    if not data:
        return None
    try:
        import qrcode  # type: ignore
    except Exception:
        return None
    try:
        qr = qrcode.QRCode(border=1)
        qr.add_data(data)
        qr.make(fit=True)
        import io
        buf = io.StringIO()
        qr.print_ascii(out=buf, invert=True)
        return buf.getvalue()
    except Exception:
        return None


def _qr_data_uri(data: str) -> Optional[str]:
    if not data:
        return None
    try:
        import qrcode  # type: ignore
    except Exception:
        return None
    try:
        import base64
        import io
        img = qrcode.make(data)
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        b64 = base64.b64encode(buf.getvalue()).decode('ascii')
        return f'data:image/png;base64,{b64}'
    except Exception:
        return None
