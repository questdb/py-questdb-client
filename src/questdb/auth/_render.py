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

Renders a clickable link + user code in Jupyter (via ``IPython.display``),
falling back to plain text on a terminal. Not required for ``token()`` /
``headers()``; ``IPython`` and ``qrcode`` are imported lazily.
"""

from __future__ import annotations

import html
import math
import re
import sys
import unicodedata
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
    # ZMQInteractiveShell == notebook/qtconsole/lab; TerminalInteractiveShell
    # == ipython in a terminal.
    return ip.__class__.__name__ in (
        'ZMQInteractiveShell', 'TerminalInteractiveShell')


def _kernel_allows_stdin() -> bool:
    """
    True if the live IPython kernel can prompt a human for input.

    A real Jupyter frontend (Lab / Notebook / VS Code / qtconsole / jupyter
    console) issues each ``execute_request`` with ``allow_stdin=True``. A
    notebook *executor* — papermill, ``nbclient``, ``jupyter nbconvert
    --execute`` / ``jupyter run`` — runs the same kind of kernel (so
    :func:`in_ipython_kernel` is ``True``) but sends ``allow_stdin=False``: there
    is no human to authorize, and an input request would raise
    ``StdinNotImplementedError``. ipykernel records the current request's value
    on the kernel as ``_allow_stdin``; read it so the device flow fails fast with
    :class:`~questdb.auth.OidcInteractionRequired` instead of polling to
    the device-code deadline. papermill sets no environment variable (its
    ``PAPERMILL_*_PATH`` values are opt-in *notebook parameters*, not
    ``os.environ`` entries), so the kernel's stdin flag — not an env var — is the
    authoritative signal.

    Defaults to ``True`` (assume a human is present) whenever the signal can't be
    read: a terminal IPython shell has no ``kernel`` attribute, and on an
    unexpected ipykernel layout it is safer to let a present user sign in than to
    wrongly refuse one.
    """
    try:
        from IPython import get_ipython  # type: ignore
        kernel = getattr(get_ipython(), 'kernel', None)
        if kernel is None:
            return True  # e.g. TerminalInteractiveShell — a human at the REPL
        allow = getattr(kernel, '_allow_stdin', None)
        return True if allow is None else bool(allow)
    except Exception:
        return True


def detect_interactive() -> bool:
    """
    Best-effort detection of whether a human can complete the sign-in.

    Interactive when attached to a TTY, or inside an IPython kernel whose
    frontend accepts stdin. A notebook executor (papermill / ``nbclient`` /
    ``nbconvert --execute``) runs a real kernel — so :func:`in_ipython_kernel`
    is ``True`` — but with no human to authorize; it executes with
    ``allow_stdin=False``, which :func:`_kernel_allows_stdin` detects, so the
    device flow fails fast in those contexts instead of hanging until the
    device code expires.
    """
    if in_ipython_kernel():
        return _kernel_allows_stdin()
    try:
        return bool(sys.stdin and sys.stdin.isatty()
                    and sys.stdout and sys.stdout.isatty())
    except Exception:
        return False


def _verification_uri(resp: Dict[str, Any]) -> str:
    # RFC 8628 uses ``verification_uri``; some IdPs (older Google) use
    # ``verification_url``. Coerce to str: the device response is untrusted, and
    # a non-string (e.g. a JSON number) would crash the renderer.
    uri = resp.get('verification_uri') or resp.get('verification_url') or ''
    return uri if isinstance(uri, str) else ''


def _verification_uri_complete(resp: Dict[str, Any]) -> Optional[str]:
    # Coerce to str/None for the same untrusted-input reason as _verification_uri.
    uri = (resp.get('verification_uri_complete')
           or resp.get('verification_url_complete'))
    return uri if isinstance(uri, str) else None


# A host safe to make clickable / auto-open: plain ASCII letters-digits-hyphen
# (a DNS name or punycode ``xn--`` label), dots, and the ``:`` an IPv6 literal
# carries once urlparse has stripped its brackets. Anything else — a non-ASCII
# confusable (e.g. a Cyrillic look-alike, or the fullwidth solidus ``U+FF0F``),
# a stray control char, or a ``%`` (percent-encoding, or an IPv6 zone-id —
# neither of which a remote verification host legitimately needs, matching the
# hygiene in ``_adapters._ILLEGAL_HOST_CHARS``) — can misrepresent the real
# destination host, so such a URL is never made clickable/auto-opened.
_SAFE_HOST_RE = re.compile(r'\A[a-z0-9._:-]+\Z')


def _safe_link_url(url: Optional[str]) -> Optional[str]:
    """
    Return ``url`` only if it is safe to make clickable / auto-open, else
    ``None``.

    The verification URL is untrusted (from the IdP's device-authorization
    response). Three checks, all of which a tampered/MITM'd response could
    otherwise abuse to send the user somewhere other than the prompt suggests
    (``html.escape`` guards markup, not any of these):

    * **scheme** — only ``http(s)``, so a ``javascript:`` / ``data:`` href can't
      execute in the notebook DOM;
    * **no userinfo** — ``https://login.questdb.io@evil.example/`` connects to
      ``evil.example`` while *reading* as the trusted host; the device-flow
      verification URL never legitimately carries credentials;
    * **plain host** — a host with non-ASCII/confusable or control characters
      (a homograph, a fullwidth solidus) can spoof the destination.

    A URL that fails these is still shown as inert, escaped text (visible and
    copyable) — it is just never turned into a live link, opened in a browser,
    or encoded into a QR.
    """
    if not url or not isinstance(url, str):
        # A non-string has no scheme to vet and would make urlparse raise.
        return None
    # urlparse() ignores surrounding whitespace when parsing the scheme, so
    # "  https://idp/..." parses as https; trim it so the value we vet is the
    # value we return (and hand to the href / webbrowser.open()), not the
    # untrimmed original.
    url = url.strip()
    # urlparse() also silently REMOVES tab/newline/CR from anywhere in the URL
    # before parsing, so a value carrying them would be vetted as its stripped
    # form yet returned (→ the href / webbrowser.open() / QR) with them intact —
    # the value vetted would not equal the value returned. Reject such a URL so
    # the invariant holds even when this is called directly. (Production always
    # passes a _strip_control'd value via _safe_target, which removes these
    # already, so this never fires there; it closes the standalone footgun.)
    if any(c in url for c in '\t\n\r'):
        return None
    try:
        parts = urllib.parse.urlparse(url)
        scheme = (parts.scheme or '').lower()
        # `.username`/`.password`/`.hostname` parse the authority; `.port` (read
        # indirectly via a malformed netloc) can raise ValueError — catch it.
        userinfo = parts.username is not None or parts.password is not None
        host = parts.hostname
    except (ValueError, TypeError):
        return None
    if scheme not in ('http', 'https'):
        return None
    if userinfo:
        return None
    if not host or not _SAFE_HOST_RE.match(host):
        return None
    return url


def _safe_target(value: Optional[str]) -> Optional[str]:
    """
    The single control-stripped, scheme/userinfo/host-vetted URL to click, open
    in a browser, or encode as a QR — or ``None`` if it can't be trusted.

    One value feeds the displayed link's ``href``, :func:`webbrowser.open` and
    both QR encoders, so a control / zero-width char stripped from the on-screen
    link can never survive into the URL actually opened or scanned: the displayed
    link and the real target cannot diverge. (Earlier the browser/QR paths vetted
    the *raw* response value while the display was control-stripped.)
    """
    return _safe_link_url(_strip_control(value))


def _ascii_visible(text: str) -> str:
    """
    Escape every non-ASCII char to a visible ``\\uXXXX`` so a confusable /
    homoglyph can't slip through a display path unchanged. ASCII is left intact.

    Used wherever :func:`_display_url` can't normalize the host — a netloc urllib
    refuses to parse (a confusable that NFKC-folds to a URL delimiter), a
    non-``http(s)`` / hostless value, or an IDNA-unencodable label — so the raw
    value is never echoed verbatim.
    """
    return text.encode('ascii', 'backslashreplace').decode('ascii')


def _display_url(url: Optional[str]) -> str:
    """
    A verification URL rendered safe to *show* as text.

    Control / bidi / zero-width chars are stripped, then the host is rebuilt in
    its IDNA / punycode (ASCII) form and any userinfo is dropped, so neither a
    homoglyph host (e.g. a fullwidth ``U+FF0E`` that IDNA folds to a real ``.``,
    making the true registrable domain ``evil.com``) nor a ``user@host`` trick
    can visually masquerade as a trusted host in the prompt — the user reads the
    host the browser would actually resolve. Clickability is decided
    independently by :func:`_safe_target` (which rejects a non-ASCII host
    outright); this governs only the visible text. A non-``http(s)`` / hostless
    value — or one whose authority urllib refuses to parse (a confusable that
    NFKC-folds to a URL delimiter, e.g. a fullwidth solidus ``U+FF0F``) — is
    returned control-stripped with any non-ASCII escaped to a visible
    ``\\uXXXX`` (:func:`_ascii_visible`), so a homoglyph can't masquerade as a
    trusted host even on this fail-open path.
    """
    text = _strip_control(url)
    if not text:
        return ''
    try:
        parts = urllib.parse.urlparse(text)
        scheme = (parts.scheme or '').lower()
        host = parts.hostname
    except ValueError:
        # The authority carries a confusable that NFKC-folds to a URL delimiter
        # (fullwidth solidus U+FF0F -> '/', U+FF20 -> '@', ...), so urlparse
        # refuses it and the host can't be normalized. Echoing it raw would show
        # a host that reads as trusted while a browser resolves the real one
        # after the fold; make the non-ASCII visible instead.
        return _ascii_visible(text)
    if scheme not in ('http', 'https') or not host:
        # Nothing host-like to normalize (opaque / relative); still neutralize any
        # non-ASCII so a confusable can't pass through this path unchanged.
        return _ascii_visible(text)
    if host.isascii():
        ascii_host = host
    else:
        try:
            # The stdlib idna codec splits on the homoglyph dots too
            # (``. 。 ． ｡``) and ToASCII-encodes each label, so a fullwidth-dot
            # host resolves to its real ASCII registrable domain here.
            ascii_host = host.encode('idna').decode('ascii')
        except (UnicodeError, ValueError):
            # IDNA can't encode it (an illegal label); make the bytes visible
            # rather than let an invisible homoglyph through unchanged.
            ascii_host = _ascii_visible(host)
    host_part = f'[{ascii_host}]' if ':' in ascii_host else ascii_host  # IPv6
    # Read the port separately and defensively: parts.port raises ValueError for
    # a malformed (non-integer / out-of-range) port. That must NOT abort host
    # normalization — otherwise a homoglyph host paired with a junk port would be
    # shown raw (the very spoof this reveals). A junk port can't be rendered, so
    # omit it; the host (what matters for spoofing) is still IDNA-normalized.
    try:
        port = parts.port
    except ValueError:
        port = None
    netloc = f'{host_part}:{port}' if port is not None else host_part
    return urllib.parse.urlunparse(parts._replace(netloc=netloc))


def _render_link(url: Optional[str], *, text: Optional[str] = None) -> str:
    """
    Render ``url`` as a clickable link, or as inert escaped text if its scheme
    is not ``http(s)``.

    The label defaults to the URL with its host shown in IDNA/punycode form
    (:func:`_display_url`) so a homoglyph / userinfo host can't masquerade as a
    trusted one; a rejected URL is shown as that escaped plain text (still
    visible/copyable) but never made clickable. The ``href`` is the single vetted
    target (:func:`_safe_target`), so the link points exactly where it reads.
    """
    safe = _safe_target(url)
    label = html.escape(text if text is not None else _display_url(url))
    if safe is None:
        return label
    return (f'<a href="{html.escape(safe)}" target="_blank" '
            f'rel="noopener noreferrer">{label}</a>')


# Untrusted device-response fields are echoed to a TTY / notebook DOM, where a
# control, bidi-override (e.g. U+202E reverses a URL's host) or zero-width char
# could spoof the prompt or hide the real sign-in URL (html.escape guards
# markup, not these). Strip by Unicode general category so a newly-assigned
# format codepoint is covered automatically, rather than an enumerated regex
# that silently misses additions: control (Cc), format (Cf: bidi / zero-width /
# soft hyphen / tag chars / the deprecated U+206x), unassigned (Cn),
# private-use (Co), surrogates (Cs) and line/paragraph separators (Zl/Zp).
# Spaces (Zs) and combining marks (Mn, e.g. accents) are kept so a legitimate
# URL/identity still renders.
_STRIP_CATEGORIES = frozenset({'Cc', 'Cf', 'Cn', 'Co', 'Cs', 'Zl', 'Zp'})
# Invisible characters Unicode classifies as letters (category Lo), so the rule
# above won't catch them, but they render as nothing and are used to hide/spoof
# text: the Hangul fillers. Stripped explicitly.
_STRIP_EXTRA = frozenset('\u115f\u1160\u3164\uffa0')


def _strip_control(text: Optional[str]) -> str:
    """
    Strip control / format characters from an untrusted string before display.

    The verification URL, user code and IdP error strings are untrusted; raw
    ANSI escapes or bidi/zero-width/line-separator chars could spoof the prompt
    or hide the real sign-in URL. Needed on both paths — ``html.escape`` does
    not catch bidi/zero-width spoofing.

    Total by design: a truthy non-``str`` (e.g. a JSON object/number a hostile
    IdP put in an ``error`` field) is coerced through ``str()`` rather than
    raising. This sanitizer runs on untrusted input from several sites and must
    never raise — a ``TypeError`` here would escape the module's typed-error
    contract (see :class:`~questdb.auth.OidcDeviceFlowError`).
    """
    if not text:
        return ''
    if not isinstance(text, str):
        text = str(text)
    return ''.join(
        ch for ch in text
        if ch not in _STRIP_EXTRA
        and unicodedata.category(ch) not in _STRIP_CATEGORIES)


def format_prompt(resp: Dict[str, Any]) -> str:
    """Plain-text sign-in prompt (also used as the notebook fallback)."""
    # _display_url shows the IDNA/punycode host (and drops userinfo) so a
    # homoglyph / user@host can't spoof the host in the plain-text prompt either.
    uri = _display_url(_verification_uri(resp))
    code = _strip_control(str(resp.get('user_code', '')))
    complete = _display_url(_verification_uri_complete(resp))
    lines = [
        '🔐 Sign in to QuestDB',
        f'   Open {uri}  and enter code:  {code}',
    ]
    if complete:
        lines.append(f'   (or open directly: {complete})')
    return '\n'.join(lines)


def _fmt_mmss(seconds: float) -> str:
    # A non-finite input (inf/nan) would make int() raise (OverflowError /
    # ValueError); treat it as 0 — it can't be a real countdown. Callers pass a
    # clamped, finite remaining time today, so this is defense-in-depth.
    if not math.isfinite(seconds):
        seconds = 0
    seconds = max(0, int(seconds))
    return f'{seconds // 60}:{seconds % 60:02d}'


def _fmt_minutes(seconds: float) -> int:
    # Minutes for the "expires in N min" success line, with the same non-finite
    # guard as _fmt_mmss: a hostile/garbage lifetime (inf/nan) would otherwise
    # make int(round(...)) raise (OverflowError/ValueError) inside on_success and
    # break the sign-in at the last step. Callers pass a clamped, finite value
    # today (see _device._display_lifetime), so this is defense-in-depth.
    if not math.isfinite(seconds):
        return 1
    return max(1, int(round(seconds / 60)))


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
            try:
                self._stream.write(text)
            except UnicodeEncodeError:
                # The stream's encoding can't represent some chars (e.g. the
                # emoji on a legacy Windows console or ascii PYTHONIOENCODING).
                # Degrade only those, so the URL/code don't vanish and look like
                # a silent hang.
                enc = getattr(self._stream, 'encoding', None) or 'ascii'
                self._stream.write(
                    text.encode(enc, 'replace').decode(enc, 'replace'))
            self._stream.flush()
        except Exception:
            pass

    def on_prompt(self, resp: Dict[str, Any]) -> None:
        self._write(format_prompt(resp) + '\n')
        if self._qr:
            # Encode the SAME _strip_control'd, vetted target the prompt displays
            # (via _safe_target), not the raw response value — so a char stripped
            # from the on-screen URL can't survive into the scanned QR, and a
            # javascript:/data: scheme is never encoded.
            target = (_safe_target(_verification_uri_complete(resp))
                      or _safe_target(_verification_uri(resp)))
            art = _qr_ascii(target) if target else None
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
        mins = _fmt_minutes(expires_in)
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
        # Cached QR <img> tag. None = not built yet; '' = built but unavailable
        # (no scheme-valid target / qrcode not installed). Built once per prompt
        # so every re-render (countdown ticks, success/failure) keeps the QR
        # instead of dropping it, and the PNG isn't regenerated each tick.
        self._qr_html: Optional[str] = None

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

    def _prompt_head(self):
        """Header + sanitized verification link, user code, and QR (if enabled).

        Shared by :meth:`on_prompt` and :meth:`_render_with_status` so the QR and
        the sanitized fields appear on EVERY render. The countdown re-renders go
        through here too, so building the QR only in ``on_prompt`` would drop it
        on the first tick. The untrusted device-response fields are stripped of
        control/bidi/zero-width chars (which ``html.escape`` does NOT remove)
        before rendering; ``_render_link`` also html-escapes and scheme-vets the
        URL. Returns ``(body, uri, complete)``.
        """
        resp = self._resp
        # _render_link / _safe_target / _qr_img each strip + vet internally, so
        # pass the raw fields and let the single canonical target drive the href,
        # the QR and the displayed (IDNA-normalized) label uniformly.
        raw_uri = _verification_uri(resp)
        raw_complete = _verification_uri_complete(resp)
        code = html.escape(_strip_control(str(resp.get('user_code', ''))))
        body = [
            '<div style="font-size:1.05em;font-weight:600;margin-bottom:6px">'
            '🔐 Sign in to QuestDB</div>',
            f'<div>Open {_render_link(raw_uri)} and enter code:</div>',
            f'<div style="font-size:1.6em;font-family:monospace;'
            f'letter-spacing:2px;margin:6px 0">{code}</div>',
        ]
        if _safe_target(raw_complete):
            body.append(
                '<div>' + _render_link(
                    raw_complete, text='Click here to authorize directly →')
                + '</div>')
        if self._qr:
            qr_html = self._qr_img(raw_complete, raw_uri)
            if qr_html:
                body.append(qr_html)
        return body, raw_uri, raw_complete

    def _qr_img(self, complete: Optional[str], uri: str) -> str:
        """The QR ``<img>`` for the verification URL, built once and cached.

        Returns ``''`` when there is no scheme-valid target or ``qrcode`` is not
        installed. Generated lazily on the first render and reused thereafter, so
        the countdown re-renders neither drop the QR nor regenerate the PNG.
        """
        if self._qr_html is None:
            target = _safe_target(complete) or _safe_target(uri)
            data_uri = _qr_data_uri(target) if target else None
            self._qr_html = (
                f'<img alt="QR code" src="{data_uri}" '
                'style="margin-top:8px;width:160px;height:160px"/>'
            ) if data_uri else ''
        return self._qr_html

    def on_prompt(self, resp: Dict[str, Any]) -> None:
        self._resp = resp
        # Start a FRESH display for this sign-in. Without resetting the handle, a
        # second sign-in on the same renderer (e.g. after clear() then token())
        # would .update() the previous sign-in's output area instead of the cell
        # the user just ran. Rebuild the QR too (a re-sign-in has a fresh
        # user_code, so the cached image from a previous prompt would be stale).
        self._handle = None
        self._qr_html = None
        body, _uri, _complete = self._prompt_head()
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
        # identity comes from untrusted JWT claims: strip then html-escape.
        who = html.escape(_strip_control(identity)) if identity else ''
        mins = _fmt_minutes(expires_in)
        suffix = f' as <b>{who}</b>' if who else ''
        self._render_with_status(
            f'✅ Signed in{suffix} — token cached, expires in {mins} min',
            color='#2e7d32')

    def on_failure(self, message: str) -> None:
        # message may interpolate the IdP's untrusted error_description.
        self._render_with_status(
            '❌ ' + html.escape(_strip_control(message)), color='#c62828')

    def _render_with_status(self, status_html: str, color: str) -> None:
        body, _uri, _complete = self._prompt_head()
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
