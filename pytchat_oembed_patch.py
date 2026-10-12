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

Usage: import and call apply_patch() *before* calling pytchat.create()
or constructing any pytchat chat object.
"""

import re
import threading
import httpx
from urllib.parse import quote

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


def apply_patch():
    """Apply the monkey-patch. Call once, before using pytchat."""
    _pytchat_util.get_channelid = patched_get_channelid
