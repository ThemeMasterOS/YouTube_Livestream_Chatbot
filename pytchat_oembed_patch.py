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

Usage: import and call apply_patch() *before* calling pytchat.create()
or constructing any pytchat chat object.
"""

import re
import httpx
from urllib.parse import quote

import pytchat.util as _pytchat_util
from pytchat import config
from pytchat.exceptions import InvalidVideoIdException

_CHANNEL_ID_RE = re.compile(r"(UC[0-9A-Za-z_-]{22})")

# Keep a reference to pytchat's original implementation as a last-resort fallback.
_original_get_channelid = _pytchat_util.get_channelid


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
    """Drop-in replacement for pytchat.util.get_channelid with an oEmbed-first strategy."""
    channel_id = _channelid_via_oembed(video_id)
    if channel_id:
        return channel_id

    # Fall back to pytchat's original scraping logic (handles edge cases
    # oEmbed can't, e.g. some unlisted/members-only streams).
    try:
        return _original_get_channelid(client, video_id)
    except InvalidVideoIdException:
        # Re-raise with a clearer message since both strategies failed.
        raise InvalidVideoIdException(
            f"Cannot find channel id for video id:{video_id}. "
            "Both oEmbed and HTML-scrape lookups failed."
        )


def apply_patch():
    """Apply the monkey-patch. Call once, before using pytchat."""
    _pytchat_util.get_channelid = patched_get_channelid
                                        
