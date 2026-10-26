"""
pytchat_oembed_patch.py

Patches pytchat's channel-ID lookup to try YouTube's public oEmbed endpoint
first, before falling back to pytchat's normal HTML-scraping routes.

Why: pytchat finds a video's channel ID by scraping
https://www.youtube.com/embed/{video_id} and, on failure,
https://m.youtube.com/watch?v={video_id}. From some cloud/datacenter IPs
(Render, Heroku, etc.), YouTube serves a placeholder/spinner page for those
URLs instead of the real page for certain videos, so pytchat can't find a
channel ID and raises InvalidVideoIdException — even though the video ID
is completely valid.

The oEmbed endpoint (https://www.youtube.com/oembed) is a lightweight JSON
API meant for embedding, and tends to keep working when the scraped HTML
routes don't.

Last resort: if BOTH of those lookups fail (e.g. the creator turned off
embedding AND the scrape is blocked from a cloud IP), a channel ID entered
manually on the bot's /live dashboard is used instead. The bot registers
those with set_manual_channel_id(); it is only consulted after the two
automatic lookups fail.

v0.1.2 adds helpers for the bot's "Select Channel" list on /live:
  - parse_channel_input(text): classifies what was typed as a channel ID,
    an @handle, or a youtube.com URL containing either.
  - resolve_handle_to_channel_id(handle): turns an @handle into a channel ID
    (and the channel's display name) using the same "/about" page +
    "externalId" technique the oEmbed path uses for @handle author URLs.
    The oEmbed endpoint itself needs a video URL, so it can't look up a bare
    handle. Returns None on any failure so the caller can fall back (the bot
    then tries the YouTube Data API, then asks for the UC... ID).
Nothing in the pytchat patching logic itself changed.

Usage: import and call apply_patch() *before* calling pytchat.create()
or constructing any pytchat chat object.
"""

import html as _html
import re
import threading
import httpx
from urllib.parse import quote, unquote

import pytchat.util as _pytchat_util
from pytchat import config
from pytchat.exceptions import InvalidVideoIdException

_CHANNEL_ID_RE = re.compile(r"(UC[0-9A-Za-z_-]{22})")

# Keep a reference to pytchat's original implementation as a last-resort fallback.
_original_get_channelid = _pytchat_util.get_channelid

_CHANNEL_ID_FULL_RE = re.compile(r"UC[0-9A-Za-z_-]{22}")

# Manual channel IDs entered on the /live dashboard, keyed by video ID.
_manual_channel_ids = {}
_manual_lock = threading.Lock()

# Optional hook so the bot can log when a manual ID actually gets used.
# Called as callback(video_id, channel_id); any exception from it is ignored.
_manual_used_callback = None


def is_valid_channel_id(channel_id) -> bool:
    """True if the value looks like a YouTube channel ID: 'UC' + 22 URL-safe characters."""
    return isinstance(channel_id, str) and bool(_CHANNEL_ID_FULL_RE.fullmatch(channel_id))


def set_manual_channel_id(video_id: str, channel_id) -> None:
    """
    Register a manual channel ID for a video. A blank value — or one that
    isn't a well-formed channel ID — clears it instead, so a malformed ID
    can never reach pytchat no matter who calls this.
    """
    with _manual_lock:
        if is_valid_channel_id(channel_id):
            _manual_channel_ids[video_id] = channel_id
        else:
            _manual_channel_ids.pop(video_id, None)


def get_manual_channel_id(video_id: str):
    with _manual_lock:
        return _manual_channel_ids.get(video_id)


def set_manual_used_callback(callback) -> None:
    global _manual_used_callback
    _manual_used_callback = callback


def _channelid_via_oembed(video_id: str) -> str | None:
    """Try to resolve a channel ID using YouTube's oEmbed endpoint."""
    oembed_url = (
        "https://www.youtube.com/oembed"
        f"?url=https://www.youtube.com/watch?v={quote(video_id)}&format=json"
    )
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(oembed_url)
            if resp.status_code != 200:
                return None

            author_url = resp.json().get("author_url", "")
            if not author_url:
                return None

            # author_url is usually .../channel/UCxxxxxxxx
            match = _CHANNEL_ID_RE.search(author_url)
            if match:
                return match.group(1)

            # Handle newer @handle-style author_url by resolving the about page.
            if "/@" in author_url:
                about_resp = client.get(author_url + "/about", follow_redirects=True)
                match2 = re.search(r'"externalId":"(UC[0-9A-Za-z_-]{22})"', about_resp.text)
                if match2:
                    return match2.group(1)
    except Exception:
        return None

    return None


def patched_get_channelid(client, video_id):
    """
    Drop-in replacement for pytchat.util.get_channelid.
    Order: oEmbed -> pytchat's original HTML scrape -> manual channel ID.
    """
    channel_id = _channelid_via_oembed(video_id)
    if channel_id:
        return channel_id

    # Fall back to pytchat's original scraping logic (handles edge cases
    # oEmbed can't, e.g. some unlisted/members-only streams).
    try:
        return _original_get_channelid(client, video_id)
    except Exception as original_error:
        # Both automatic lookups failed. Last resort: a channel ID the owner
        # entered manually on the /live dashboard, if there is one.
        manual_channel_id = get_manual_channel_id(video_id)
        if manual_channel_id:
            callback = _manual_used_callback
            if callback is not None:
                try:
                    callback(video_id, manual_channel_id)
                except Exception:
                    pass
            return manual_channel_id

        if isinstance(original_error, InvalidVideoIdException):
            # Re-raise with a clearer message since every strategy failed.
            raise InvalidVideoIdException(
                f"Cannot find the channel ID for video ID: {video_id}. "
                "Both the oEmbed and HTML-scrape lookups failed. "
                "Try adding the channel ID in the /live dashboard."
            )
        raise


# ---------------------------------------------------------------------------
# v0.1.2: handle / channel-ID input helpers for the bot's saved-channel list
# ---------------------------------------------------------------------------

# YouTube handles: 3-30 characters of letters, digits, '.', '_' or '-'.
_HANDLE_RE = re.compile(r"^@?([A-Za-z0-9._-]{3,30})$")

# A desktop-style request plus a consent cookie, so Google's EU-style consent
# interstitial is less likely to replace the channel page.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cookie": "CONSENT=YES+1; SOCS=CAI",
}

_CANONICAL_RE = re.compile(
    r'<link\s+rel="canonical"\s+href="https?://www\.youtube\.com/channel/(UC[0-9A-Za-z_-]{22})"'
)
_ITEMPROP_RE = re.compile(
    r'<meta\s+itemprop="(?:identifier|channelId)"\s+content="(UC[0-9A-Za-z_-]{22})"'
)
_EXTERNAL_ID_RE = re.compile(r'"externalId":"(UC[0-9A-Za-z_-]{22})"')
_OG_TITLE_RE = re.compile(r'<meta\s+property="og:title"\s+content="([^"]*)"')


def parse_channel_input(text):
    """
    Classify what the user typed. Returns (kind, value):
      ("id", "UC...")      a well-formed channel ID, or a /channel/UC... URL
      ("handle", "@name")  an @handle, a bare handle, or a youtube.com/@name URL
      (None, "")           unrecognised / malformed
    """
    s = (text or "").strip()
    if not s:
        return (None, "")

    if is_valid_channel_id(s):
        return ("id", s)

    lowered = s.lower()
    if "youtube.com" in lowered or "youtu.be" in lowered:
        m = re.search(r"/channel/(UC[0-9A-Za-z_-]{22})", s)
        if m:
            return ("id", m.group(1))
        m = re.search(r"/@([^/?#\s]+)", s)
        if m:
            handle = unquote(m.group(1))
            if _HANDLE_RE.match(handle):
                return ("handle", "@" + handle.lstrip("@"))
        return (None, "")

    # Something that looks like a (malformed) channel ID shouldn't be
    # mistaken for a handle and sent off to be looked up.
    if "@" not in s and s.startswith("UC") and len(s) >= 20:
        return (None, "")

    m = _HANDLE_RE.match(s)
    if m:
        return ("handle", "@" + m.group(1))
    return (None, "")


def resolve_handle_to_channel_id(handle: str):
    """
    Resolve an @handle to its channel ID by reading the channel's /about page,
    the same technique used for @handle author URLs in the oEmbed path.

    Returns {"channel_id": "UC...", "name": "Display Name"} (name may be "")
    or None if the page couldn't be fetched or no ID was found in it.
    """
    handle = "@" + (handle or "").lstrip("@")
    if not _HANDLE_RE.match(handle):
        return None

    url = f"https://www.youtube.com/{quote(handle, safe='@')}/about"
    try:
        with httpx.Client(timeout=10, follow_redirects=True, headers=_BROWSER_HEADERS) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            page = resp.text
    except Exception:
        return None

    channel_id = None
    for pattern in (_CANONICAL_RE, _ITEMPROP_RE, _EXTERNAL_ID_RE):
        m = pattern.search(page)
        if m:
            channel_id = m.group(1)
            break
    if not channel_id:
        return None

    name = ""
    m = _OG_TITLE_RE.search(page)
    if m:
        name = _html.unescape(m.group(1)).strip()

    return {"channel_id": channel_id, "name": name}


def apply_patch():
    """Apply the monkey-patch. Call once, before using pytchat."""
    _pytchat_util.get_channelid = patched_get_channelid
