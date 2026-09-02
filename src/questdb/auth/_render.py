################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2026 QuestDB
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
    # A ZMQ kernel shell carries the live kernel; TerminalInteractiveShell does
    # not. This is the same signal _kernel_allows_stdin reads, and unlike an
    # exact class-name test it holds for the subclasses real frontends ship:
    # Google Colab (google.colab._shell.Shell) and Spyder
    # (spyder_kernels.console.shell.SpyderShell) both subclass
    # ZMQInteractiveShell, so a name match reported False for them and refused
    # sign-in outright. Fall back to walking the MRO by name for an exotic
    # frontend that leaves `kernel` unset.
    if getattr(ip, 'kernel', None) is not None:
        return True
    return any(
        base.__name__ == 'ZMQInteractiveShell' for base in type(ip).__mro__)


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

    Interactive when stderr is a TTY — the stream the prompt is written to —
    or inside an IPython kernel whose frontend accepts stdin. A notebook executor (papermill / ``nbclient`` /
    ``nbconvert --execute``) runs a real kernel — so :func:`in_ipython_kernel`
    is ``True`` — but with no human to authorize; it executes with
    ``allow_stdin=False``, which :func:`_kernel_allows_stdin` detects, so the
    device flow fails fast in those contexts instead of hanging until the
    device code expires.
    """
    if in_ipython_kernel():
        return _kernel_allows_stdin()
    try:
        # stderr, not stdout: that is where TerminalRenderer writes the prompt,
        # and what the native auto-detect this overrides checks. Gating on
        # stdout refused sign-in for `python job.py > results.csv` run at a real
        # terminal, where the prompt would have been perfectly visible. stdin is
        # not required either -- the user authorizes in a browser, and nothing
        # here reads from it.
        return bool(sys.stderr and sys.stderr.isatty())
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
# hygiene in ``_adapters._LEGAL_HOST_RE``) — can misrepresent the real
# destination host, so such a URL is never made clickable/auto-opened.
_SAFE_HOST_RE = re.compile(r'\A[a-z0-9._:-]+\Z')

# Mirrors the native vetter's MAX_DISPLAY_FIELD_CHARS. Keep the two in step: a
# URL native refuses to vet must not become clickable here either.
_MAX_ACTIONABLE_URL_CHARS = 256


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
    # value we return (and hand to the href / QR encoders), not the
    # untrimmed original.
    url = url.strip()
    # urlparse() also silently REMOVES tab/newline/CR from anywhere in the URL
    # before parsing, so a value carrying them would be vetted as its stripped
    # form yet returned (→ the href / QR encoders) with them intact —
    # the value vetted would not equal the value returned. Reject such a URL so
    # the invariant holds even when this is called directly. (Production always
    # passes a _strip_control'd value via _safe_target, which removes these
    # already, so this never fires there; it closes the standalone footgun.)
    if any(c in url for c in '\t\n\r'):
        return None
    # Bound the length exactly as the native vetter does (its
    # MAX_DISPLAY_FIELD_CHARS). Every other untrusted display field is capped
    # and marked with an ellipsis, but an actionable URL is rejected instead of
    # truncated: a cut URL can still parse as a perfectly valid *different* URL,
    # whose host passes the check below because truncation leaves the authority
    # intact. A genuine verification URL is far below this bound. Native applies
    # this to `browser_target`; applying it here too means a custom renderer
    # that builds its own response dict (no `browser_target` key, so the
    # fallback path below runs) gets the same protection.
    if len(url) > _MAX_ACTIONABLE_URL_CHARS:
        return None
    try:
        parts = urllib.parse.urlparse(url)
        scheme = (parts.scheme or '').lower()
        # `.username`/`.password`/`.hostname`/`.port` parse the authority; a
        # non-integer or out-of-range `.port` raises ValueError, caught below.
        # Reading `.port` is essential, not incidental: without it a URL with a
        # junk port (e.g. "https://host:70000/…") is returned verbatim for the
        # href / QR encoders, while `_display_url` DROPS that port
        # from the shown text — so the displayed link and the real target would
        # diverge, the exact spoof this vetting (via `_safe_target`) exists to
        # prevent. Rejecting it here keeps them identical: the URL is then shown
        # as inert, port-stripped text and never made clickable/opened/scanned.
        # A portless URL yields `.port is None` without raising.
        userinfo = parts.username is not None or parts.password is not None
        host = parts.hostname
        _ = parts.port
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
    The single control-stripped, scheme/userinfo/host-vetted URL to click or
    encode as a QR — or ``None`` if it can't be trusted.

    One value feeds the displayed link's ``href`` and both QR encoders, so a
    control / zero-width char stripped from the on-screen link can never survive
    into the URL actually scanned: the displayed link and the real target cannot
    diverge. (Browser opening is performed natively from the vetted
    ``browser_target``; earlier the QR path vetted the *raw* response value while
    the display was control-stripped.)
    """
    return _safe_link_url(_strip_control(value))


def _effective_port(parts) -> Optional[int]:
    """The URL's port, folding in the scheme default (443 ``https`` / 80
    ``http``) when it is omitted, so an explicit ``:443``/``:80`` and an implicit
    port compare equal. Inputs are ``_safe_target``-vetted ``http(s)``, so a
    default always exists.
    """
    if parts.port is not None:
        return parts.port
    return {'http': 80, 'https': 443}.get((parts.scheme or '').lower())


def _same_origin(a: str, b: str) -> bool:
    """True if two already-``_safe_target``-vetted URLs share an origin
    (scheme + host + port).

    Both are guaranteed ``http(s)`` with an ASCII host, no userinfo, and a
    parseable in-range port (:func:`_safe_link_url` rejects anything else), so a
    plain origin comparison is meaningful and cannot raise on ``.port``. The
    scheme's default port is normalized (:func:`_effective_port`) so a URL that
    spells out ``:443``/``:80`` and one that omits it still match — RFC 8628's
    ``verification_uri_complete`` is ``verification_uri`` plus the user code, so
    a legitimate pair shares an origin even when one side writes the port.
    """
    pa = urllib.parse.urlparse(a)
    pb = urllib.parse.urlparse(b)
    return (
        pa.scheme.lower() == pb.scheme.lower()
        and (pa.hostname or '').lower() == (pb.hostname or '').lower()
        and _effective_port(pa) == _effective_port(pb))


def _matched_complete(resp: Dict[str, Any]) -> Optional[str]:
    """The vetted ``verification_uri_complete`` when it shares
    ``verification_uri``'s origin, else ``None``.

    ``complete`` is a target the user does NOT read character-by-character: it is
    auto-opened, encoded into the QR, and (in Jupyter) backs the fixed-label
    "Click here to authorize directly" link, whose host is never shown.
    ``verification_uri`` is the host shown IDNA-normalized in the primary prompt
    line — the host the user is meant to read and trust. RFC 8628 §3.3.1 makes
    ``complete`` the same URL as ``verification_uri`` with the user code added,
    so a legitimate pair shares an origin. A tampered/hostile device response
    could instead pair a trusted-looking ``verification_uri`` with a ``complete``
    on a DIFFERENT host, silently steering the opened/scanned/clicked target off
    to the attacker while the displayed host still looks right. So a ``complete``
    whose origin diverges is treated as absent everywhere (not shown, not made
    actionable), and only ``verification_uri`` is used.
    """
    safe_uri = _safe_target(_verification_uri(resp))
    safe_complete = _safe_target(_verification_uri_complete(resp))
    if (safe_complete is not None and safe_uri is not None
            and _same_origin(safe_complete, safe_uri)):
        return safe_complete
    return None


def _native_adjudicated(resp: Dict[str, Any]) -> bool:
    """Whether this response came from a native device-flow event.

    Native always sets the ``browser_target`` key on a prompt event — to the
    vetted URL, or to ``None`` when it refused to vet one. Key *presence* is
    therefore the signal that the native side has already adjudicated these
    URLs; a custom pure-Python renderer builds a dict without it.
    """
    return 'browser_target' in resp


def _verification_target(resp: Dict[str, Any]) -> Optional[str]:
    """The single URL to auto-open, encode as a QR, or make a one-click link.

    On a native event the native verdict is final: ``browser_target`` is the
    vetted URL, and ``None`` means native *refused* to vet one — this returns
    ``None`` rather than falling back, because the values it would fall back to
    are display strings, not targets. Native rejects an over-long URL rather
    than truncating it, precisely because a truncated URL can still parse as a
    valid *different* URL; falling back to the truncated display string would
    hand the user exactly the destination native declined to offer.

    Only when the key is absent — a custom renderer supplying its own response
    dict — does the origin-matching fallback apply: the origin-matched
    ``verification_uri_complete`` (see :func:`_matched_complete`), else the
    vetted ``verification_uri``. For those branches the target cannot diverge
    from the host shown in the link the user reads, and Python enforces it:
    :func:`_matched_complete` drops an off-origin ``complete``, and
    ``verification_uri`` *is* the shown host.
    """
    if _native_adjudicated(resp):
        return _safe_target(resp.get('browser_target'))
    return (_matched_complete(resp)
            or _safe_target(_verification_uri(resp)))


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
    # A host that still carries a URL-structural character after normalization —
    # backslash, slash, '@', '?' or '#' — is ambiguous: RFC 3986 keeps it in the
    # authority while a WHATWG/browser parser folds '\' to '/' and ends the host
    # early, so the host shown here would not be the one a browser resolves (the
    # very divergence this function exists to close). The IDNA fold can MINT one:
    # a fullwidth reverse solidus U+FF3C (or small reverse solidus U+FE68) is
    # category Po, so it survives _strip_control, passes urlparse (whose NFKC
    # delimiter-reject covers '/ @ :' but not '\'), and nameprep folds it to a
    # literal '\'. Don't render a clean-looking but ambiguous URL — fall back to
    # the control-stripped text with every non-ASCII char escaped to a visible
    # \uXXXX, so the confusable is shown as e.g. '＼', not as a bare '\'.
    # (':' is excluded: an IPv6 literal legitimately carries it and is bracketed
    # just below.)
    if any(c in ascii_host for c in '\\/@?#'):
        return _ascii_visible(text)
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
# private-use (Co), surrogates (Cs), line/paragraph separators (Zl/Zp), and
# ENCLOSING combining marks (Me, e.g. U+20E0 / U+0489) — which overlay the
# preceding glyph (a circle/slash/keycap) and are never part of a legitimate
# identity / URL / user_code.
# The ordinary ASCII space (U+0020, itself category Zs), non-enclosing combining
# marks (Mn accents — capped below — and Mc spacing marks, e.g. Indic vowel
# signs) are kept so a legitimate identity still renders; every OTHER space
# separator (NBSP U+00A0, ideographic space U+3000, ...) is folded to a plain
# space below, since an invisible-as-space char is a known phishing primitive
# (it can hide trailing text in a user_code / identity / error).
_STRIP_CATEGORIES = frozenset({'Cc', 'Cf', 'Cn', 'Co', 'Cs', 'Me', 'Zl', 'Zp'})
# Invisible characters the category rule above does NOT catch, stripped
# explicitly:
#  - the Hangul fillers (category Lo) — render as nothing, used to hide/spoof;
#  - variation selectors VS1–VS16 (U+FE00–U+FE0F) and the supplement
#    (U+E0100–U+E01EF) — category Mn (so the "keep accents" rule below would keep
#    them), invisible, and able to carry hidden payload through a user_code / URL
#    / identity or flip an adjacent glyph's text/emoji presentation.
#  - the remaining invisible Default_Ignorable non-spacing marks (also category
#    Mn, so likewise kept by the "keep accents" rule): the combining grapheme
#    joiner (U+034F), the Mongolian free variation selectors (U+180B–U+180D and
#    U+180F) and the Khmer inherent vowels (U+17B4, U+17B5) — same hazard class
#    as the variation selectors above, invisible and able to hide payload in a
#    user_code / URL / identity. (The Cf/Cn/Lo Default_Ignorables — soft hyphen,
#    U+180E, the zero-width/bidi runs, the tag chars — are already dropped by the
#    category rule.)
#  - U+2800 BRAILLE PATTERN BLANK (category So, so neither the category rule nor
#    the Zs space-fold below catches it) renders as a blank, cell-width glyph and
#    is a known invisible-padding primitive that can hide trailing text in a
#    user_code / identity / error, the same hazard class as the Hangul fillers.
_STRIP_EXTRA = frozenset(
    '\u115f\u1160\u3164\uffa0'
    + ''.join(chr(c) for c in (
        0x2800, 0x034F, 0x17B4, 0x17B5, 0x180B, 0x180C, 0x180D, 0x180F))
    + ''.join(chr(c) for c in range(0xFE00, 0xFE10))
    + ''.join(chr(c) for c in range(0xE0100, 0xE01F0)))

# Cap consecutive non-spacing marks (category Mn) kept on one base character.
# Mn marks stack vertically on the preceding glyph; a long run is a "Zalgo"
# overrun that smears across adjacent prompt lines and can obscure the real
# sign-in URL / code. A legitimate accented identity never needs more than a
# couple (Hebrew nikud+cantillation, decomposed Vietnamese ≈ 2), so this is
# generous for real text while neutralising a runaway stack.
_MAX_COMBINING_RUN = 4


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
    out = []
    combining_run = 0
    for ch in text:
        if ch in _STRIP_EXTRA:
            # Stripped chars are transparent to the combining-run count below, so
            # an attacker can't reset the cap by interleaving zero-width /
            # variation-selector chars between stacked marks.
            continue
        category = unicodedata.category(ch)
        if category in _STRIP_CATEGORIES:
            continue
        if category == 'Mn':
            # Non-spacing marks stack on the preceding base; a long run is a
            # "Zalgo" overrun that smears across adjacent prompt lines. Keep a
            # short legitimate run (accents / diacritics), drop the overflow.
            # (Enclosing marks Me are stripped above; variation selectors are in
            # _STRIP_EXTRA — neither reaches here.)
            combining_run += 1
            if combining_run > _MAX_COMBINING_RUN:
                continue
            out.append(ch)
            continue
        combining_run = 0
        # Fold an exotic space separator (NBSP, ideographic space, ...) to a
        # plain ASCII space: it renders invisible-as-space and can hide trailing
        # text, but the ordinary U+0020 of a legitimate identity must survive.
        if category == 'Zs' and ch != ' ':
            out.append(' ')
        else:
            out.append(ch)
    return ''.join(out)


def sanitize_display_text(text: Optional[str]) -> str:
    """Strip control, bidi and zero-width characters from untrusted text.

    A custom :class:`Renderer` receives identity-provider fields verbatim --
    the verification URL, the user code, error strings -- and is responsible
    for sanitising them for its own output sink. This is the same routine the
    built-in renderers use, exported so a custom one does not have to
    reimplement it or reach for a private name.

    ``html.escape`` is not a substitute: it guards markup, not the ANSI escapes
    and bidi overrides that can spoof a prompt or hide the real sign-in URL.
    """
    return _strip_control(text)



def format_prompt(resp: Dict[str, Any]) -> str:
    """Plain-text sign-in prompt (also used as the notebook fallback)."""
    # _display_url shows the IDNA/punycode host (and drops userinfo) so a
    # homoglyph / user@host can't spoof the host in the plain-text prompt either.
    uri = _display_url(_verification_uri(resp))
    code = _strip_control(str(resp.get('user_code') or ''))
    # Only offer the pre-filled "open directly" URL when it shares the shown
    # link's origin (see _matched_complete); a complete on a different host is
    # dropped rather than shown, so the convenience URL can't point somewhere the
    # primary link does not.
    complete = _matched_complete(resp)
    lines = [
        '🔐 Sign in to QuestDB',
        f'   Open {uri}  and enter code:  {code}',
    ]
    if complete:
        lines.append(f'   (or open directly: {_display_url(complete)})')
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
    # today from the native engine, so this is defense-in-depth.
    if not math.isfinite(seconds):
        return 1
    return max(1, int(round(seconds / 60)))


class Renderer:
    """No-op renderer interface; subclasses present the prompt to the user.

    Pass an instance as ``renderer=`` to :class:`~questdb.auth.OidcDeviceAuth`
    to customise how the device-flow sign-in is shown (the built-ins are a
    plain-text terminal renderer and a rich Jupyter one). Every callback is
    optional — the base class no-ops each, so a subclass may override only the
    ones it cares about.

    **The callbacks receive untrusted, MITM-tamperable IdP fields**
    (``verification_uri``, ``user_code``, error strings, a JWT-derived
    identity). A custom renderer that writes them to a terminal or a notebook
    DOM must sanitise them itself (the built-ins strip control/bidi/zero-width
    characters and vet the verification host); echoing them raw re-opens the
    prompt-spoofing surface the built-in renderers close.
    :func:`~questdb.auth.sanitize_display_text` is the routine they use, exported
    for exactly this.

    **Concurrency.** The callbacks run while ``OidcDeviceAuth`` holds its
    (non-reentrant) acquisition lock, so a callback must not call back into the
    same instance's :meth:`~questdb.auth.OidcDeviceAuth.sign_in`,
    :meth:`~questdb.auth.OidcDeviceAuth.token` or
    :meth:`~questdb.auth.OidcDeviceAuth.clear` — each raises rather than
    deadlocking. :meth:`~questdb.auth.OidcDeviceAuth.close` is the exception and
    is safe here: it is how a renderer offers a "cancel" affordance, and how
    another thread aborts a running device flow. Called from inside a callback
    it publishes the close and drops the in-memory credential, but returns
    without waiting for the flow to leave the critical section — waiting there
    would deadlock against the callback itself.

    The rejection applies to any thread, not only the callback's own, because a
    callback may hand work to another thread and wait for it — so a blocking
    call there would deadlock just the same. A concurrent
    :meth:`~questdb.auth.OidcDeviceAuth.token` elsewhere — a pooled PG-wire
    checkout, say — therefore succeeds only from a *valid cached token*, which
    needs no lock the callback holds; if a fresh acquisition would be required
    it is refused until the prompt finishes. Sign in before putting a provider
    behind a connection pool, so pooled checkouts hit that cache.

    Callbacks are best-effort: an exception raised by one is logged and never
    aborts an otherwise-successful sign-in. ``KeyboardInterrupt`` and
    ``SystemExit`` are the exceptions to *that* — they cancel the flow and are
    re-raised from :meth:`~questdb.auth.OidcDeviceAuth.sign_in`, so Ctrl-C
    works.
    """

    def on_prompt(self, resp: Dict[str, Any]) -> None:
        """Show the sign-in prompt at the start of the device flow.

        ``resp`` describes the device-authorization challenge. Its text remains
        untrusted even though the native layer makes it display-safe; the
        verification URI and user code live under ``verification_uri`` /
        ``verification_uri_complete`` / ``user_code``. ``expires_in`` and
        ``interval`` are the bounded device-code lifetime and initial polling
        interval, in seconds, used by the native polling loop. Native events
        also carry ``browser_target`` — the single URL the native side has
        vetted for opening / linkifying / QR-encoding; the built-in renderers
        prefer it, and a custom renderer should use it (rather than the raw
        ``verification_uri``) as the actionable target.
        """

    def on_waiting(self, seconds_left: float) -> None:
        """Report progress while polling; ``seconds_left`` is the time
        remaining before the device code expires."""

    def on_success(self, identity: Optional[str], expires_in: float) -> None:
        """Report a completed sign-in. ``identity`` is a best-effort,
        unverified display name from the token's claims (or ``None``);
        ``expires_in`` is the token's remaining lifetime in seconds."""

    def on_failure(self, message: str) -> None:
        """Report a failed or expired sign-in with a human-readable
        ``message`` (which may interpolate an untrusted IdP error string)."""


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
            # Encode the SAME single vetted target the prompt displays (see
            # _verification_target), not the raw response value — so a char
            # stripped from the on-screen URL can't survive into the scanned QR,
            # a javascript:/data: scheme is never encoded, and a
            # verification_uri_complete on a different host than the shown link is
            # never scanned.
            target = _verification_target(resp)
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
        # Guard the suffix on the POST-strip value: an all-control identity
        # strips to '' and must not render a dangling ' as ' with no name.
        stripped = _strip_control(identity) if identity else ''
        who = f' as {stripped}' if stripped else ''
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
        URL. Returns the list of body HTML fragments.
        """
        resp = self._resp
        # _render_link / _safe_target / _qr_img each strip + vet internally, so
        # pass the raw fields and let the single canonical target drive the href,
        # the QR and the displayed (IDNA-normalized) label uniformly.
        raw_uri = _verification_uri(resp)
        # When native adjudicated these URLs and refused to vet one, nothing on
        # this panel becomes actionable: the URL is still shown (inert, escaped,
        # copyable) but carries no href, and the one-click affordance below is
        # dropped. Falling back to the displayed value would hand the user the
        # very destination native declined to offer -- and since native rejects
        # an over-long URL rather than truncating it, that value may be a
        # truncated string that parses as a valid *different* URL.
        native_refused = (_native_adjudicated(resp)
                          and _verification_target(resp) is None)
        href = None if native_refused else raw_uri
        code = html.escape(_strip_control(str(resp.get('user_code') or '')))
        body = [
            '<div style="font-size:1.05em;font-weight:600;margin-bottom:6px">'
            '🔐 Sign in to QuestDB</div>',
            f'<div>Open {_render_link(href, text=_display_url(raw_uri))} '
            f'and enter code:</div>',
            f'<div style="font-size:1.6em;font-family:monospace;'
            f'letter-spacing:2px;margin:6px 0">{code}</div>',
        ]
        # Offer the one-click "authorize directly" link only when the pre-filled
        # complete shares the shown link's origin (see _matched_complete): its
        # label is fixed text, so its host is never shown, and a complete on a
        # different host would silently send the click to the attacker while the
        # primary link above still reads as the trusted host.
        matched = None if native_refused else _matched_complete(resp)
        if matched:
            body.append(
                '<div>' + _render_link(
                    matched, text='Click here to authorize directly →')
                + '</div>')
        if self._qr:
            qr_html = self._qr_img(_verification_target(resp))
            if qr_html:
                body.append(qr_html)
        return body

    def _qr_img(self, target: Optional[str]) -> str:
        """The QR ``<img>`` for the verification URL, built once and cached.

        ``target`` is the single vetted URL to encode (see _verification_target).
        Returns ``''`` when there is no scheme-valid target or ``qrcode`` is not
        installed. Generated lazily on the first render and reused thereafter, so
        the countdown re-renders neither drop the QR nor regenerate the PNG.
        """
        if self._qr_html is None:
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
        body = self._prompt_head()
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
        body = self._prompt_head()
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
