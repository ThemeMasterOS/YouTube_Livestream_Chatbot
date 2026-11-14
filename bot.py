import os
import copy
import json
import math
import re
import signal
import time
import random
import threading
import urllib.parse
import urllib.request
import urllib.error
import base64
import html as html_lib
import hmac
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import deque
from contextlib import contextmanager
from http.cookies import SimpleCookie
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
load_dotenv()
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from auth import Authorize

# --- Apply pytchat manual-channel patch BEFORE importing/using pytchat ---
import pytchat_manual_channel_patch
pytchat_manual_channel_patch.apply_patch()
import pytchat
# ------------------------------------------------------------------


# --- Connection queue (v0.2.7.1) ----------------------------------------
# Limits how many streams may attempt to connect / reconnect to live chat at
# the same time (getLiveChatId + pytchat.create). Waiting streams are served
# strictly first-come-first-served. A slot is held ONLY while the connect call
# itself runs, never during sleeps, retry delays or normal chat listening.
def _env_number(name, default, cast, minimum):
    try:
        return max(minimum, cast(os.getenv(name, default)))
    except (TypeError, ValueError):
        return default

MAX_CONCURRENT_CONNECTS = _env_number("MAX_CONCURRENT_CONNECTS", 2, int, 1)
CONNECT_MIN_SPACING = _env_number("CONNECT_MIN_SPACING", 2.0, float, 0.0)  # seconds between attempt starts
CONNECT_QUEUE_LOG_THRESHOLD = 5  # seconds; log "got a slot" only if the wait was longer than this


class ConnectionQueueCancelled(Exception):
    """Raised when a stream is removed (stop flag set) while waiting in the connection queue."""


class ConnectionGate:
    def __init__(self, max_concurrent, min_spacing):
        self._max = max_concurrent
        self._spacing = min_spacing
        self._cond = threading.Condition()
        self._queue = deque()      # tickets, oldest first
        self._active = 0
        self._next_start = 0.0     # earliest time the next attempt may start

    @staticmethod
    def _cancelled(stream_id):
        flag = stop_flags.get(stream_id)   # popped by stop_stream_listener() on Remove
        return flag is None or flag.is_set()

    def _leave_queue(self, ticket):
        with self._cond:
            try:
                self._queue.remove(ticket)
            except ValueError:
                pass
            self._cond.notify_all()

    def acquire(self, stream_id):
        ticket = object()
        queued_at = time.time()
        with self._cond:
            must_wait = self._active >= self._max or bool(self._queue)
            self._queue.append(ticket)
            position = len(self._queue)
        if must_wait:
            add_log(f"⏳ '{stream_id}' queued for live chat connection (position {position}).")

        with self._cond:
            while True:
                if self._cancelled(stream_id):
                    try:
                        self._queue.remove(ticket)
                    except ValueError:
                        pass
                    self._cond.notify_all()
                    raise ConnectionQueueCancelled(f"'{stream_id}' stopped while waiting in the connection queue")
                if self._queue[0] is ticket and self._active < self._max:
                    break
                self._cond.wait(1.0)
            self._queue.popleft()
            self._active += 1
            now = time.time()
            start_at = max(now, self._next_start)
            self._next_start = start_at + self._spacing + random.uniform(0, 0.5)
            self._cond.notify_all()

        # Spacing delay (outside the lock, still holding the slot), stop-aware
        try:
            while True:
                remaining = start_at - time.time()
                if remaining <= 0:
                    break
                if self._cancelled(stream_id):
                    raise ConnectionQueueCancelled(f"'{stream_id}' stopped while waiting in the connection queue")
                time.sleep(min(remaining, 0.5))
        except BaseException:
            self.release()
            raise

        waited = time.time() - queued_at
        if must_wait and waited >= CONNECT_QUEUE_LOG_THRESHOLD:
            add_log(f"🔌 '{stream_id}' got a live chat connection slot after {waited:.0f} s.")

    def release(self):
        with self._cond:
            self._active = max(0, self._active - 1)
            self._cond.notify_all()

    @contextmanager
    def slot(self, stream_id):
        self.acquire(stream_id)
        try:
            yield
        finally:
            self.release()


CONNECTION_GATE = ConnectionGate(MAX_CONCURRENT_CONNECTS, CONNECT_MIN_SPACING)


def safe_pytchat_create(video_id):
    """Queued entry point: waits for a connection slot, then creates the pytchat chat."""
    with CONNECTION_GATE.slot(video_id):
        return _safe_pytchat_create_unqueued(video_id)


def _safe_pytchat_create_unqueued(video_id):
    """
    pytchat.create() internally calls signal.signal(signal.SIGINT, ...) to
    handle Ctrl+C cleanup. That call raises ValueError("signal only works
    in main thread of the main interpreter") whenever it's not run on the
    main thread — which is exactly what happens here, since every stream
    listener runs in its own worker thread to support multiple simultaneous
    streams.

    Fix: temporarily replace signal.signal with a no-op only for the
    duration of pytchat.create(), then restore the real signal.signal
    immediately after. This only matters off the main thread — the
    no-op just swallows the registration attempt pytchat doesn't
    actually need here (there's no Ctrl+C to catch inside a worker thread).
    """
    if threading.current_thread() is threading.main_thread():
        return pytchat.create(video_id=video_id)

    original_signal = signal.signal
    signal.signal = lambda *args, **kwargs: None
    try:
        return pytchat.create(video_id=video_id)
    finally:
        signal.signal = original_signal


# --- pytchat stop-reason helper ---------------------------------------
# is_alive() only says "stopped", not why. raise_for_status() re-raises the
# stored exception, so we use it to explain every is_alive() == False.
# Names are matched via the exception's MRO (by name, not import), so a
# pytchat version that lacks one of them can't break the bot. The labels
# are hints for the log only; they never change what the bot does.
# NoContents ("Chat data stream is empty") is what pytchat raises once a stream's
# chat has gone empty/offline, so it is treated like ChatDataFinished.
_PYTCHAT_ENDED_NAMES = {"ChatDataFinished", "FailedExtractContinuation", "NoContents"}
_PYTCHAT_NETWORK_NAMES = {
    "RetryExceedMaxCount", "UnknownConnectionError",
    # httpx / stdlib network errors (e.g. ReadError "[Errno 11] Resource
    # temporarily unavailable", RemoteProtocolError "ConnectionTerminated")
    "ReadError", "WriteError", "ConnectError", "RemoteProtocolError",
    "ReadTimeout", "WriteTimeout", "ConnectTimeout", "PoolTimeout",
    "NetworkError", "TransportError", "TimeoutException",
    "ConnectionError", "TimeoutError",
}

# Restore-loop guard: if a freshly restored pytchat dies again within this many
# seconds, it counts as a "rapid death". After PYTCHAT_MAX_RAPID_DEATHS in a row
# the bot stops restoring and lets the YouTube API decide (or stops the listener
# when no backup API exists).
PYTCHAT_RAPID_DEATH_WINDOW = 30
PYTCHAT_MAX_RAPID_DEATHS = 3
_PYTCHAT_UNAVAILABLE_NAMES = {"InvalidVideoIdException", "VideoInfoParseError", "PatternUnmatchError"}


def _classify_pytchat_exception(exc):
    names = {cls.__name__ for cls in type(exc).__mro__}
    if names & _PYTCHAT_ENDED_NAMES:
        return "stream likely ended"
    if names & _PYTCHAT_NETWORK_NAMES:
        return "network problem"
    if names & _PYTCHAT_UNAVAILABLE_NAMES:
        return "video unavailable, not live, or lookup failed"
    return "other"


def describe_pytchat_stop(chat):
    """Return a readable reason for why a pytchat chat object is not alive."""
    if chat is None:
        return "no pytchat object exists"
    try:
        chat.raise_for_status()
    except Exception as e:
        return f"{_classify_pytchat_exception(e)} - {type(e).__name__}: {e}"
    return "no error reported"


def pytchat_stop_means_ended(chat):
    """True if pytchat's stored exception says the chat data finished.
    Only a hint: the YouTube API gets the final say before a listener stops."""
    if chat is None:
        return False
    try:
        chat.raise_for_status()
    except Exception as e:
        return bool({cls.__name__ for cls in type(e).__mro__} & _PYTCHAT_ENDED_NAMES)
    return False


# --- YouTube API "chat is gone" reasons -------------------------------
# (HTTP status, API error reason) -> text for the log. Any of these means the
# live chat cannot be read, so the listener stops immediately (no 3-strike wait).
_CHAT_GONE_REASONS = {
    (403, "liveChatEnded"): "YouTube reports the live chat has ended (liveChatEnded)",
    (403, "liveChatDisabled"): "the live chat is disabled (liveChatDisabled)",
    (404, "liveChatNotFound"): "the live chat no longer exists (liveChatNotFound)",
}


def _http_error_has_reason(e, reason):
    """Check Google's structured error details first, then fall back to the text."""
    details = getattr(e, "error_details", None)
    if isinstance(details, list):
        for d in details:
            if isinstance(d, dict) and d.get("reason") == reason:
                return True
    return reason in str(e)


def chat_gone_description(e):
    """Return a log description if this HttpError means the chat is gone, else None."""
    status = getattr(getattr(e, "resp", None), "status", None)
    for (code, reason), description in _CHAT_GONE_REASONS.items():
        if status == code and _http_error_has_reason(e, reason):
            return description
    return None

# --- Global Log Storage (Holds last 100 entries) ---
chat_logs = deque(maxlen=100)   # holds (id, text) tuples
_log_lock = threading.Lock()
_log_counter = 0                # monotonically increasing log ID

# --- Live Streams Storage ---
STREAMS_FILE = "streams_config.json"

# --- Saved Channels Storage (v0.2.7) ---
CHANNELS_FILE = "channels_config.json"

# --- NeilCoins Storage ---
COINS_FILE = "coins_config.json"
DEFAULT_STARTING_COINS = 100
RESETCOINS_GRANT = 25
RESETCOINS_MAX_USES = 5
ANGRYREVERTICAL_PENALTY = 1000

# --- Moderation Storage ---
MODERATION_FILE = "moderation_config.json"
DEFAULT_SPAM_THRESHOLD = 3
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_TIMEOUT_MESSAGE = "{userName}, you have been timed out for {duration} for spamming."
DEFAULT_REMINDER_MESSAGE = "{userName}, reminder: please don't spam."
DEFAULT_BLOCKLIST_TIMEOUT_MESSAGE = "{userName}, you have been timed out for {duration} for saying bad words."
DEFAULT_BLOCKLIST_REMINDER_MESSAGE = "{userName}, reminder: don't say bad words."
MOD_REQUEST_MESSAGE = "Hey, can you add @ThemeMasterBot as a moderator? I can time out viewers who spam or use blocked words."

# --- GitHub-backed persistence ---
# Render's free tier wipes local disk on every cold-start and redeploy.
# Instead of Persistent Disk (needs a credit card) or a database (costs money),
# we use the repo itself as free storage: read/write JSON files directly via
# GitHub's Contents API. Local disk is still used as a fast cache during a
# running session; GitHub is the durable backing store. Generic enough to
# back both streams_config.json and coins_config.json.
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")            # Personal access token, "repo" scope
GITHUB_REPO = os.getenv("GITHUB_REPO")              # e.g. "ThemeMasterEXE/youtube-livestream-chatbot"
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
GITHUB_API_BASE = "https://api.github.com"

# Cache each file's current GitHub blob SHA (keyed by filename) — required
# by GitHub's API to update an existing file (proves you're not overwriting
# someone else's more recent change).
_github_file_shas = {}


def _github_configured():
    return bool(GITHUB_TOKEN and GITHUB_REPO)


def _github_request(method, url, body=None):
    """Minimal GitHub API request helper using only the standard library."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "youtube-livestream-chatbot",
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def github_load_json(file_path):
    """Fetch a JSON file straight from GitHub (source of truth)."""
    url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO}/contents/{file_path}?ref={GITHUB_BRANCH}"
    try:
        result = _github_request("GET", url)
        _github_file_shas[file_path] = result["sha"]
        content = base64.b64decode(result["content"]).decode("utf-8")
        return json.loads(content)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            add_log(f"No {file_path} in GitHub repo yet — starting fresh.")
            _github_file_shas[file_path] = None
            return {}
        add_log(f"GitHub load error ({file_path}): {e}")
        return None  # Signals "couldn't reach GitHub", distinct from "empty"
    except Exception as e:
        add_log(f"GitHub load error ({file_path}): {e}")
        return None


def github_save_json(file_path, data, description):
    """Push a JSON file to GitHub as a commit."""
    url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO}/contents/{file_path}"
    content_str = json.dumps(data, indent=2)
    encoded = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")

    body = {
        "message": f"Update {file_path} ({description})",
        "content": encoded,
        "branch": GITHUB_BRANCH,
    }
    existing_sha = _github_file_shas.get(file_path)
    if existing_sha:
        body["sha"] = existing_sha  # Required when updating an existing file

    try:
        result = _github_request("PUT", url, body)
        _github_file_shas[file_path] = result["content"]["sha"]
        add_log(f"{file_path} backed up to GitHub.")
        return True
    except Exception as e:
        add_log(f"GitHub save error for {file_path} (local save still succeeded): {e}")
        return False


def load_json_with_fallback(file_path, label):
    """
    Load a JSON file. GitHub is the source of truth (survives cold-starts
    and redeploys); local disk is a fallback if GitHub is unreachable or
    not configured (no GITHUB_TOKEN/GITHUB_REPO set).
    """
    if _github_configured():
        data = github_load_json(file_path)
        if data is not None:
            add_log(f"Loaded {len(data)} {label} from GitHub")
            # Keep a local cache too, in case GitHub is briefly unreachable later
            try:
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
            except Exception:
                pass
            return data
        add_log(f"GitHub unreachable — falling back to local disk cache for {file_path}.")

    # Local disk fallback (also the only path if GitHub isn't configured at all)
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                add_log(f"Loaded {len(data)} {label} from local {file_path}")
                return data
        except Exception as e:
            add_log(f"Error loading local {file_path}: {e}")
            return {}
    else:
        add_log(f"{file_path} not found locally. Starting with empty {label}.")
        return {}


def save_json_with_backup(file_path, data, description):
    """Save JSON locally (fast, always works) and back up to GitHub (durable)."""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        add_log(f"{file_path} saved to local disk.")
    except Exception as e:
        add_log(f"Error saving local {file_path}: {e}")

    if _github_configured():
        github_save_json(file_path, data, description)
    else:
        add_log(f"GITHUB_TOKEN/GITHUB_REPO not set — skipping GitHub backup for {file_path} (local only, won't survive cold-start).")


def load_streams():
    return load_json_with_fallback(STREAMS_FILE, "stream(s)")


def save_streams(streams):
    save_json_with_backup(STREAMS_FILE, streams, f"{len(streams)} stream(s)")


# --- Saved Channels (v0.2.7) ---
# A reusable list behind the "Select Channel" dropdown on /live, kept in
# channels_config.json (GitHub-backed like the other config files):
#   {"imported_stream_channels": true,
#    "channels": {"UCxxxxxxxxxxxxxxxxxxxxxx": {"name": "...", "handle": "@..."}}}
# Each stream keeps its OWN copy of channel_id in streams_config.json, so
# editing or deleting a saved channel can never break a running stream.
# The list is cached in memory (loaded once) and written through on every change.
_channels_lock = threading.RLock()
_channels_state = None


def _short_channel_id(channel_id):
    return f"{channel_id[:6]}…{channel_id[-6:]}" if len(channel_id) > 14 else channel_id


def channel_display_name(channel_id, entry=None):
    """Name if set, else the handle, else a shortened channel ID."""
    entry = entry or {}
    return entry.get("name") or entry.get("handle") or _short_channel_id(channel_id)


def _channels_state_locked():
    """The cached saved-channel state. Caller must hold _channels_lock."""
    global _channels_state
    if _channels_state is None:
        raw = load_json_with_fallback(CHANNELS_FILE, "channel config section(s)")
        if not isinstance(raw, dict):
            raw = {}
        clean = {}
        stored = raw.get("channels")
        for cid, entry in (stored.items() if isinstance(stored, dict) else []):
            if pytchat_manual_channel_patch.is_valid_channel_id(cid) and isinstance(entry, dict):
                clean[cid] = {
                    "name": str(entry.get("name") or ""),
                    "handle": str(entry.get("handle") or ""),
                }
        _channels_state = {
            "imported_stream_channels": bool(raw.get("imported_stream_channels")),
            "channels": clean,
        }
        add_log(f"Saved channel list ready: {len(clean)} channel(s).")
    return _channels_state


def _persist_channels_locked():
    state = _channels_state
    save_json_with_backup(CHANNELS_FILE, state, f"{len(state['channels'])} saved channel(s)")


def list_saved_channels():
    """Saved channels as a list of {id, name, handle}, sorted by display name."""
    with _channels_lock:
        channels = _channels_state_locked()["channels"]
        items = [{"id": cid, "name": e["name"], "handle": e["handle"]} for cid, e in channels.items()]
    items.sort(key=lambda c: channel_display_name(c["id"], c).lower())
    return items


def get_saved_channel(channel_id):
    with _channels_lock:
        entry = _channels_state_locked()["channels"].get(channel_id)
        return {"id": channel_id, "name": entry["name"], "handle": entry["handle"]} if entry else None


def upsert_saved_channel(channel_id, name, handle, replace_id=None):
    """Add or update a channel. If replace_id differs from channel_id (an edit that
    changed the ID), the old entry is removed so the channel is re-keyed."""
    with _channels_lock:
        channels = _channels_state_locked()["channels"]
        if replace_id and replace_id != channel_id:
            channels.pop(replace_id, None)
        channels[channel_id] = {"name": name, "handle": handle}
        _persist_channels_locked()


def delete_saved_channel(channel_id):
    with _channels_lock:
        channels = _channels_state_locked()["channels"]
        if channel_id not in channels:
            return False
        del channels[channel_id]
        _persist_channels_locked()
        return True


def ensure_channel_saved(channel_id):
    """Make sure a channel ID that was just put on a stream is in the saved list
    (as an unnamed entry if it wasn't). Returns True if it was added."""
    if not pytchat_manual_channel_patch.is_valid_channel_id(channel_id):
        return False
    with _channels_lock:
        channels = _channels_state_locked()["channels"]
        if channel_id in channels:
            return False
        channels[channel_id] = {"name": "", "handle": ""}
        _persist_channels_locked()
        return True


def import_stream_channels_once(streams):
    """
    One-time import for people upgrading from v0.2.6: channel IDs already saved
    on streams are added to the list as unnamed entries. Runs only while the
    flag is unset AND the list is empty, and only writes when it actually adds
    something, so a failed/blank load can never overwrite a real list.
    """
    with _channels_lock:
        state = _channels_state_locked()
        if state["imported_stream_channels"] or state["channels"]:
            return 0
        added = 0
        for data in (streams or {}).values():
            cid = data.get("channel_id") if isinstance(data, dict) else None
            if cid and pytchat_manual_channel_patch.is_valid_channel_id(cid) and cid not in state["channels"]:
                state["channels"][cid] = {"name": "", "handle": ""}
                added += 1
        if added:
            state["imported_stream_channels"] = True
            _persist_channels_locked()
        return added


def lookup_handle_via_api(handle):
    """
    YouTube Data API fallback for @handle lookups (channels.list?forHandle=,
    about 1 quota unit), run through the usual Main -> Backup 1 -> Backup 2
    quota failover. Returns {"channel_id", "name"} or None.
    """
    handle = "@" + handle.lstrip("@")
    try:
        resp = call_with_quota_failover(
            lambda client: client.channels().list(part="id,snippet", forHandle=handle).execute(),
            "channel handle lookup",
        )
    except AllQuotasExhausted:
        add_log(f"Handle lookup for {handle} skipped: all API quotas are exhausted.")
        return None
    except TypeError as e:
        add_log(f"Handle lookup for {handle} via API failed: the installed google-api-python-client doesn't support forHandle (upgrade it). {e}")
        return None
    except Exception as e:
        add_log(f"Handle lookup for {handle} via API failed: {e}")
        return None

    items = resp.get("items") or []
    if not items:
        return None
    channel_id = items[0].get("id", "")
    if not pytchat_manual_channel_patch.is_valid_channel_id(channel_id):
        return None
    return {"channel_id": channel_id, "name": (items[0].get("snippet") or {}).get("title", "")}


def resolve_channel_input(text):
    """
    Turn whatever was typed in the Add Channel dialog into a channel.
    Returns ({"channel_id", "handle", "name"}, None) on success or
    (None, error_message). Order for a handle: /about page scrape (free),
    then the YouTube Data API, then ask for the UC... ID.
    """
    kind, value = pytchat_manual_channel_patch.parse_channel_input(text)

    if kind == "id":
        return {"channel_id": value, "handle": "", "name": ""}, None

    if kind == "handle":
        found = pytchat_manual_channel_patch.resolve_handle_to_channel_id(value)
        if not found:
            add_log(f"Page lookup for {value} found nothing — trying the YouTube API.")
            found = lookup_handle_via_api(value)
        if found:
            return {"channel_id": found["channel_id"], "handle": value, "name": found.get("name", "")}, None
        return None, (
            f"Couldn't look up {value} from the server (YouTube may be blocking the lookup, or the handle doesn't exist). "
            "Paste the channel's ID instead: it starts with UC and is 24 characters long."
        )

    return None, (
        "Enter a handle like @name, a youtube.com/@name or /channel/UC... link, "
        "or a channel ID (starts with UC, 24 characters)."
    )


# --- NeilCoins helpers ---

# Coins are read and written on every !gamble/!giftpoint, so they need two
# things at once: protection from hammering GitHub (secondary rate limits),
# and always-correct balances.
#
# Local disk is saved immediately on every save_coins(), but the GitHub
# backup is debounced: the first save in a quiet period starts a 30-second
# timer, and further saves before it fires just update what that timer will
# send (the clock isn't restarted, so sustained spamming is still backed up
# at least every 30 seconds).
#
# Reads come from an in-memory copy that save_coins() updates immediately
# (write-through), so the in-memory copy is the source of truth while the
# bot is running. Reading GitHub first on every load — which load_coins()
# used to do — is wrong once GitHub can lag up to 30 seconds behind: a
# gamble would read the stale GitHub balance, compute from it, and write
# the wrong result over the correct local copy.
#
# GitHub is only re-read when the cached copy is older than
# COINS_CACHE_TTL_SECONDS (to pick up manual edits made on GitHub) and no
# backup is pending, so stale GitHub data can never overwrite newer unsent
# changes. On a cold start the cache is empty, so the first load comes from
# GitHub as before.
#
# Tradeoff accepted: if Render redeploys/crashes inside the 30-second window,
# changes that only reached local disk are lost, since Render wipes it.
COINS_GITHUB_BACKUP_DELAY_SECONDS = 30
COINS_CACHE_TTL_SECONDS = 60
_coins_backup_timer = None
_pending_coins_data = None
_coins_cache = None
_coins_cache_time = 0
_coins_version = 0  # bumped on every save, so a slow GitHub fetch can't clobber a newer save
_coins_backup_lock = threading.Lock()


def load_coins():
    """
    Returns a private COPY of the coin data, never the shared cache — several
    stream threads use coins at once, and one thread mutating a dict while
    another serializes it would corrupt a save or crash it.
    """
    global _coins_cache, _coins_cache_time

    with _coins_backup_lock:
        if _coins_cache is not None:
            fresh = (time.time() - _coins_cache_time) < COINS_CACHE_TTL_SECONDS
            backup_pending = _pending_coins_data is not None
            if fresh or backup_pending:
                return copy.deepcopy(_coins_cache)
        version_before_fetch = _coins_version

    # Cache missing or stale, and nothing unsent: refresh from GitHub
    # (network call made outside the lock so it never blocks other threads).
    fetched = load_json_with_fallback(COINS_FILE, "coin balance(s)")

    with _coins_backup_lock:
        if _coins_version != version_before_fetch and _coins_cache is not None:
            # A save landed while we were fetching — the cache is newer
            # than what we just downloaded, so keep the cache.
            return copy.deepcopy(_coins_cache)
        _coins_cache = fetched
        _coins_cache_time = time.time()
        return copy.deepcopy(_coins_cache)


def _flush_coins_to_github():
    global _coins_backup_timer, _pending_coins_data
    with _coins_backup_lock:
        data_to_send = _pending_coins_data
        _coins_backup_timer = None
        _pending_coins_data = None

    if data_to_send is not None:
        if _github_configured():
            github_save_json(COINS_FILE, data_to_send, f"{len(data_to_send)} user(s)")
        else:
            add_log(f"GITHUB_TOKEN/GITHUB_REPO not set — skipping GitHub backup for {COINS_FILE} (local only, won't survive cold-start).")


def save_coins(coins):
    global _coins_backup_timer, _pending_coins_data, _coins_cache, _coins_cache_time, _coins_version

    snapshot = copy.deepcopy(coins)  # later changes by the caller must not leak in

    with _coins_backup_lock:
        # Memory first: the very next load_coins() sees this immediately.
        _coins_cache = snapshot
        _coins_cache_time = time.time()
        _coins_version += 1

        # Local disk: always immediate. Done inside the lock so two threads
        # can't interleave writes to the same file.
        try:
            with open(COINS_FILE, 'w') as f:
                json.dump(snapshot, f, indent=2)
            add_log(f"{COINS_FILE} saved to local disk.")
        except Exception as e:
            add_log(f"Error saving local {COINS_FILE}: {e}")

        # GitHub: debounced.
        _pending_coins_data = snapshot
        if _coins_backup_timer is None:
            _coins_backup_timer = threading.Timer(COINS_GITHUB_BACKUP_DELAY_SECONDS, _flush_coins_to_github)
            _coins_backup_timer.daemon = True
            _coins_backup_timer.start()


def get_user_record(coins, user_key, display_name=None):
    """
    Get (creating if needed) a user's coin record. Keyed by channel ID when
    available (stable even if display name changes), falling back to a
    lowercased username key when no channel ID is known.
    Record shape: {"name": display_name, "balance": int, "resetcoins_uses": int}

    display_name, when provided, is the user's actual current YouTube name —
    always stored as "name" (never the raw key) so leaderboards/messages
    show a real name instead of a channel ID. Kept up to date on every call
    in case the user's display name has changed since their last message.
    """
    if user_key not in coins:
        coins[user_key] = {
            "name": display_name or user_key,
            "balance": DEFAULT_STARTING_COINS,
            "resetcoins_uses": 0
        }
    elif display_name:
        coins[user_key]["name"] = display_name  # Keep name fresh on every interaction
    return coins[user_key]


# --- Moderation helpers ---

def _default_stream_moderation():
    return {
        "enabled": False,
        "spam_threshold": DEFAULT_SPAM_THRESHOLD,
        "timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
        "timeout_message": DEFAULT_TIMEOUT_MESSAGE,
        "reminder_message": DEFAULT_REMINDER_MESSAGE,
    }


def _default_blocklist_category(name=""):
    return {
        "name": name,
        "words": [],
        "timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
        "timeout_message": DEFAULT_BLOCKLIST_TIMEOUT_MESSAGE,
        "reminder_message": DEFAULT_BLOCKLIST_REMINDER_MESSAGE,
    }


# run_moderation_check() calls load_moderation() on every single chat
# message across every stream — with no cache, that meant one fresh
# GitHub API read per message, which can trip GitHub's secondary rate
# limiting under normal chat traffic (confirmed via GitHub's own docs: a
# 403 from an endpoint that worked moments earlier is usually the rate
# limit, not a permission change). This short cache keeps reads local for
# MODERATION_CACHE_TTL_SECONDS at a time; save_moderation() below updates
# the cache immediately on every write, so edits made through /moderation
# still apply the instant they're saved, with no perceptible delay — the
# TTL only matters for catching an edit made directly on GitHub, outside
# the bot, which would take up to the TTL to be picked up.
MODERATION_CACHE_TTL_SECONDS = 60
_moderation_cache = None
_moderation_cache_time = 0
_moderation_cache_lock = threading.Lock()


def load_moderation():
    global _moderation_cache, _moderation_cache_time

    with _moderation_cache_lock:
        if _moderation_cache is not None and (time.time() - _moderation_cache_time) < MODERATION_CACHE_TTL_SECONDS:
            return _moderation_cache

    config = load_json_with_fallback(MODERATION_FILE, "moderation setting(s)")
    # Backfill structure for a fresh/empty file so callers can rely on these keys existing.
    if "blocklist_categories" not in config:
        config["blocklist_categories"] = {}
    if "streams" not in config:
        config["streams"] = {}
    if "all_streams" not in config:
        config["all_streams"] = _default_stream_moderation()

    with _moderation_cache_lock:
        _moderation_cache = config
        _moderation_cache_time = time.time()

    return config


def save_moderation(config):
    save_json_with_backup(MODERATION_FILE, config, "moderation settings")

    global _moderation_cache, _moderation_cache_time
    with _moderation_cache_lock:
        _moderation_cache = config
        _moderation_cache_time = time.time()


def get_stream_moderation_settings(config, video_id):
    """
    Returns the effective spam-moderation settings for a given stream: its
    per-stream config if one exists and is enabled, otherwise the
    "All streams" config (only if enabled), otherwise None (spam detection
    inactive for this stream). Blocklist categories are checked separately
    via check_blocklist_categories() since they're global and independent
    of this enabled/disabled toggle.
    """
    per_stream = config.get("streams", {}).get(video_id)
    if per_stream and per_stream.get("enabled"):
        return per_stream

    all_streams = config.get("all_streams", {})
    if all_streams.get("enabled"):
        return all_streams

    return None


def check_blocklist_categories(config, lower_msg):
    """
    Checks a lowercased message against every blocklist category (global,
    applies to all streams). Returns the first matching category's settings
    dict, or None if no category matches. Categories are checked in
    insertion order; the first match wins if a message happens to match
    words from more than one category.

    Matches on whole-word boundaries (\\b) rather than a plain substring
    check — a plain "bad_word in lower_msg" test would false-positive on
    innocent words that merely CONTAIN a blocked word, e.g. "ass" matching
    inside "passeando" or "class". \\b treats each blocked entry as its own
    word (or exact phrase, for multi-word entries), bounded by whitespace,
    punctuation, or the start/end of the message.
    """
    for category in config.get("blocklist_categories", {}).values():
        for bad_word in category.get("words", []):
            bad_word = bad_word.strip().lower()
            if not bad_word:
                continue
            pattern = r'\b' + re.escape(bad_word) + r'\b'
            if re.search(pattern, lower_msg):
                return category
    return None


def make_user_key(userName, userChannelId):
    """Prefer the stable channel ID; fall back to lowercased name if absent."""
    if userChannelId:
        return str(userChannelId)
    return userName.strip().lower()




def add_log(message):
    """Formats logs with a 24-hour UTC timestamp and pushes to memory and stdout."""
    utc_timestamp = time.strftime("[%H:%M:%S UTC]", time.gmtime())
    entry = f"{utc_timestamp} {message}"
    global _log_counter
    with _log_lock:
        _log_counter += 1
        chat_logs.append((_log_counter, entry))
    print(entry, flush=True)


def seconds_until_next_midnight_pt():
    """
    Returns (seconds_to_wait, utc_offset_label) for the next 00:00 Pacific
    Time — i.e. YouTube's daily quota reset. Uses zoneinfo so the correct
    PDT (UTC-7) vs PST (UTC-8) offset is derived from today's actual date,
    not hardcoded — this stays correct automatically across DST transitions.
    """
    pt_zone = ZoneInfo("America/Los_Angeles")
    now_pt = datetime.now(pt_zone)

    next_midnight_pt = (now_pt + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    seconds_to_wait = (next_midnight_pt - now_pt).total_seconds()

    # UTC offset for Pacific Time at that moment (-7 during PDT, -8 during PST)
    utc_offset_hours = int(-next_midnight_pt.utcoffset().total_seconds() / 3600)
    utc_offset_label = f"{utc_offset_hours:02d}:00"

    return seconds_to_wait, utc_offset_label


# --- Global thread-safe tracking of which video IDs currently have an
# active listener thread running, plus a "stop" flag per stream so the
# Remove button can cleanly signal a listener thread to shut down. ---
active_streams_lock = threading.Lock()
active_stream_ids = set()      # video_ids with a running listener thread
stop_flags = {}                # video_id -> threading.Event(), set() to stop that listener


# When a stream's channel ID is added/edited on /live while that stream is
# already running in API-fallback mode, pytchat would otherwise not be retried
# until the next PYTCHAT_RETRY_INTERVAL (10 minutes). This set lets the API
# handler ask a running listener to retry pytchat right away instead.
_force_pytchat_retry = set()
_force_pytchat_retry_lock = threading.Lock()


def request_pytchat_retry(video_id):
    with _force_pytchat_retry_lock:
        _force_pytchat_retry.add(video_id)


def _consume_pytchat_retry_request(video_id):
    """True (once) if a retry was requested for this stream; clears the request."""
    with _force_pytchat_retry_lock:
        if video_id in _force_pytchat_retry:
            _force_pytchat_retry.discard(video_id)
            return True
        return False


def _clear_pytchat_retry_request(video_id):
    with _force_pytchat_retry_lock:
        _force_pytchat_retry.discard(video_id)


# v0.3.0.3: live label registry. A listener thread used to keep the label it was started with
# for its whole life, so renaming a stream on /live never reached the running thread's logs.
# The thread now re-reads its label from here, and a rename updates it.
stream_labels = {}
stream_labels_lock = threading.Lock()


def set_stream_label(video_id, label):
    """Record the current display label for a stream (blank falls back to the video ID on read)."""
    with stream_labels_lock:
        stream_labels[video_id] = (label or "").strip()


def get_stream_label(video_id, fallback=None):
    """Current display label for a stream: the registry value, else fallback, else the video ID."""
    with stream_labels_lock:
        label = stream_labels.get(video_id)
    return label or fallback or video_id


def is_stream_running(video_id):
    """True if a listener thread is currently registered for this video ID."""
    with active_streams_lock:
        return video_id in active_stream_ids


def start_stream_listener(video_id, label):
    """Start a listener thread for a video ID if one isn't already running."""
    with active_streams_lock:
        if video_id in active_stream_ids:
            add_log(f"'{label}' ({video_id}) already has an active listener — skipping duplicate start.")
            return False
        active_stream_ids.add(video_id)
        stop_flags[video_id] = threading.Event()
    set_stream_label(video_id, label)

    thread = threading.Thread(
        target=listen_to_stream,
        args=(video_id, label, stop_flags[video_id]),
        daemon=True
    )
    thread.start()
    return True


def stop_stream_listener(video_id):
    """Signal a running listener thread for this video_id to stop (used by the /live Remove button)."""
    with active_streams_lock:
        flag = stop_flags.get(video_id)
        if flag:
            flag.set()
        active_stream_ids.discard(video_id)
        stop_flags.pop(video_id, None)
    with stream_labels_lock:
        stream_labels.pop(video_id, None)


def mark_stream_stopped(video_id):
    """
    Called by a listener thread on itself when it exits naturally (stream
    ended, too many API errors) rather than via the /live Remove button.
    Removes it from active_stream_ids so /live correctly shows ⚪ Stopped
    instead of leaving it stuck on 🟢 Listening for a thread that's already
    dead. Does NOT touch stop_flags — Remove button cleanup already
    handles that path separately, and this only runs for self-ended threads.
    """
    with active_streams_lock:
        active_stream_ids.discard(video_id)



# --- Bulk stream delete (v0.2.8.0) -----------------------------------------
BULK_DELETE_MAX = 500   # safety cap on one Delete Selected request


def _remove_stream_record(streams, video_id):
    """
    Same cleanup the single-stream Remove (DELETE /api/streams/<id>) performs,
    minus the save: drops the record from the in-memory `streams` dict, stops
    its listener, and clears its manual channel ID and pending pytchat retry.
    Returns (existed, was_running). The caller saves `streams` once.
    """
    existed = video_id in streams
    if existed:
        del streams[video_id]
    with active_streams_lock:
        was_running = video_id in active_stream_ids
    stop_stream_listener(video_id)
    pytchat_manual_channel_patch.set_manual_channel_id(video_id, None)
    _clear_pytchat_retry_request(video_id)
    return existed, was_running


def delete_streams_bulk(video_ids=None, only_stopped=False):
    """
    Delete many streams with ONE load and ONE save (saves can sync to GitHub).
    video_ids: explicit IDs for Delete Selected.
    only_stopped: delete every saved stream that has no running listener right
    now (decided here, on the server, so a stale /live page can't delete a
    stream that has since gone live).
    """
    streams = load_streams()
    if only_stopped:
        video_ids = list(streams.keys())
    deleted = stopped_running = not_found = 0
    for video_id in (video_ids or []):
        if only_stopped:
            with active_streams_lock:
                if video_id in active_stream_ids:
                    continue  # went live since the page loaded: leave it alone
        existed, was_running = _remove_stream_record(streams, video_id)
        if existed:
            deleted += 1
            if was_running:
                stopped_running += 1
        else:
            not_found += 1
    if deleted:
        save_streams(streams)
    kind = "stopped streams" if only_stopped else "selected streams"
    add_log(f"Bulk delete ({kind}): removed {deleted} stream(s) ({stopped_running} were listening and got stopped).")
    return {"deleted": deleted, "stopped_running": stopped_running, "not_found": not_found}


def get_logs_since(since_id):
    """Return (latest_id, [{'id':..,'text':..}, ...]) for entries newer than since_id, oldest first."""
    with _log_lock:
        snapshot = list(chat_logs)
        latest = _log_counter
    return latest, [{"id": i, "text": t} for i, t in snapshot if i > since_id]


def get_leaderboard_rows():
    """Top 50 users as plain dicts for /api/leaderboard. Ranking uses the stored integers; "balance" is returned as a decimal string."""
    coins = load_coins()
    ranked = sorted(coins.values(), key=lambda r: r.get('balance', 0), reverse=True)[:50]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    return [
        {
            "rank": medals.get(i, f"#{i}"),
            "name": r.get('name', 'Unknown'),
            "balance": str(r.get('balance', 0)),   # decimal string: JSON numbers above 2**53 lose digits in browsers
            "resets": f"{r.get('resetcoins_uses', 0)}/{RESETCOINS_MAX_USES}",
        }
        for i, r in enumerate(ranked, start=1)
    ]


# ---------------------------------------------------------------------------
# Sudo mode: /live and /moderation are read-only unless the browser carries a
# valid signed cookie. The password lives in the ADMIN_PASSWORD env var (never
# in the code or repo). SESSION_SECRET is optional; if unset, the signing key
# is derived from the password, so changing the password logs everyone out.
# ---------------------------------------------------------------------------
SUDO_COOKIE_NAME = "sudo_session"
SUDO_SESSION_SECONDS = 30 * 24 * 60 * 60      # 30 days
SUDO_MAX_FAILS = 5
SUDO_LOCKOUT_SECONDS = 15 * 60                # 15 minutes
ADMIN_PASSWORD = (os.getenv("ADMIN_PASSWORD") or "").strip()
_SESSION_KEY = (os.getenv("SESSION_SECRET") or ("neilcoins-sudo:" + ADMIN_PASSWORD)).encode("utf-8")
_login_attempts = {}
_login_lock = threading.Lock()
SUDO_AUTH_ROUTES = ("/api/auth/login", "/api/auth/logout")

if not ADMIN_PASSWORD:
    print("⚠️ ADMIN_PASSWORD is not set: sudo mode is disabled and /live + /moderation stay read-only.", flush=True)


def _sudo_sign(expiry):
    return hmac.new(_SESSION_KEY, f"sudo:{expiry}".encode("utf-8"), hashlib.sha256).hexdigest()


def make_sudo_token():
    expiry = int(time.time()) + SUDO_SESSION_SECONDS
    return f"{expiry}.{_sudo_sign(expiry)}"


def verify_sudo_token(token):
    if not ADMIN_PASSWORD or not token:
        return False
    try:
        expiry_text, signature = token.split(".", 1)
        expiry = int(expiry_text)
    except ValueError:
        return False
    if expiry < time.time():
        return False
    return hmac.compare_digest(signature, _sudo_sign(expiry))


def sudo_lockout_remaining(ip):
    """Seconds left on this IP's lockout, or 0 if it may try."""
    with _login_lock:
        record = _login_attempts.get(ip)
        if record and record["locked_until"] > time.time():
            return int(record["locked_until"] - time.time()) + 1
    return 0


def sudo_register_failure(ip):
    now = time.time()
    with _login_lock:
        record = _login_attempts.setdefault(ip, {"fails": 0, "locked_until": 0})
        if record["locked_until"] and record["locked_until"] <= now:
            record["fails"] = 0
            record["locked_until"] = 0
        record["fails"] += 1
        if record["fails"] >= SUDO_MAX_FAILS:
            record["locked_until"] = now + SUDO_LOCKOUT_SECONDS
        if len(_login_attempts) > 500:   # keep the table small
            for key in [k for k, v in _login_attempts.items() if v["locked_until"] <= now and v["fails"] < SUDO_MAX_FAILS]:
                del _login_attempts[key]


def sudo_clear_failures(ip):
    with _login_lock:
        _login_attempts.pop(ip, None)


# ---------------------------------------------------------------------------
# v0.2.7: "Select Channel" dropdown + Add/Edit Channel dialog for /live.
# Plain (non-f) strings so CSS/JS braces need no escaping. v0.3.0.0: the CSS is themed
# and added to the dashboard shell; {CHANNEL_UI_HTML} and {CHANNEL_UI_JS} go into the
# /_view/live fragment. Saved channels reach it as window.__SAVED_CHANNELS__.
# ---------------------------------------------------------------------------
CHANNEL_UI_CSS = r"""
                    .picker-btn { display: inline-flex; align-items: center; justify-content: space-between; gap: 10px; min-width: 240px; max-width: 100%; box-sizing: border-box; text-align: left; padding: 8px 12px; margin: 0 8px 8px 0; background: #0d1117; color: #8b949e; border: 1px solid #30363d; border-radius: 4px; }
                    .picker-btn:hover { background: #0d1117; border-color: #58a6ff; }
                    .picker-btn.has-value { color: #58a6ff; border-color: #1f6feb; }
                    .picker-btn .picker-label { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
                    .picker-btn .caret { font-size: 11px; color: #8b949e; }
                    .chback[hidden], .chpanel[hidden], .chmodalback[hidden] { display: none !important; }
                    .chback { position: fixed; inset: 0; z-index: 900; background: transparent; }
                    .chpanel { position: fixed; z-index: 910; display: flex; flex-direction: column; background: #161b22; border: 1px solid #30363d; border-radius: 8px; box-shadow: 0 8px 24px rgba(1, 4, 9, 0.6); overflow: hidden; }
                    .chlist { overflow-y: auto; flex: 1; padding: 4px 0; }
                    .chrow { display: flex; align-items: center; gap: 8px; padding: 8px 8px 8px 12px; cursor: pointer; border-left: 3px solid transparent; }
                    .chrow:hover { background: #21262d; }
                    .chrow:focus { outline: none; background: #21262d; }
                    .chrow:focus-visible { outline: 2px solid #58a6ff; outline-offset: -2px; }
                    .chrow.sel { border-left-color: #58a6ff; background: #1c2128; }
                    .chmain { flex: 1; min-width: 0; }
                    .chname { display: block; color: #c9d1d9; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
                    .chrow.none .chname { color: #8b949e; font-style: italic; }
                    .chsub { display: block; margin-top: 2px; font-size: 12px; color: #8b949e; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
                    .chedit { margin: 0; padding: 4px 10px; flex: none; }
                    .chfoot { border-top: 1px solid #30363d; padding: 8px; }
                    .chfoot button { width: 100%; margin: 0; }
                    .chempty { padding: 10px 12px; font-size: 13px; color: #8b949e; }
                    .chmodalback { position: fixed; inset: 0; z-index: 1000; display: flex; align-items: center; justify-content: center; padding: 16px; background: rgba(1, 4, 9, 0.72); }
                    .chmodal { width: 100%; max-width: 440px; max-height: 100%; overflow-y: auto; box-sizing: border-box; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; }
                    .chmodal h3 { margin: 0 0 6px 0; color: #58a6ff; }
                    .chmodal label { display: block; margin: 14px 0 4px 0; font-size: 14px; color: #c9d1d9; }
                    .chmodal .chhint { font-size: 12px; color: #8b949e; }
                    .chmodal input { width: 100%; box-sizing: border-box; padding: 8px; background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; border-radius: 4px; }
                    .chmodal .chhelp { margin: 10px 0 0 0; font-size: 12px; color: #8b949e; }
                    .chmodal .chstatus { min-height: 18px; margin: 8px 0 0 0; font-size: 13px; color: #8b949e; }
                    .chmodal .chstatus.err { color: #f85149; }
                    .chactions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
                    .chactions button { margin: 0; }
                    .chactions .chdanger { margin-left: auto; color: #f85149; border-color: #6e2a2d; }
                    @media (max-width: 700px) {
                        .picker-btn { width: 100%; margin-right: 0; }
                        .chback { background: rgba(1, 4, 9, 0.55); }
                        .chpanel.sheet { top: auto !important; left: 0 !important; right: 0 !important; bottom: 0 !important; width: auto !important; max-height: 70vh !important; border-radius: 12px 12px 0 0; }
                        .chmodal { padding: 16px; }
                    }
"""

CHANNEL_UI_HTML = r"""
                <div class="chmodalback" id="chDlgBack" hidden>
                    <div class="chmodal" role="dialog" aria-modal="true" aria-labelledby="chDlgTitle">
                        <h3 id="chDlgTitle">Add Channel ID</h3>
                        <label for="chDlgName">Display name <span class="chhint">(Optional but Recommended to prevent confusion)</span></label>
                        <input type="text" id="chDlgName" maxlength="60" autocomplete="off" placeholder="e.g. Neil's main channel">
                        <label for="chDlgInput">Handle or Channel ID</label>
                        <input type="text" id="chDlgInput" autocomplete="off" autocapitalize="off" spellcheck="false" placeholder="@handle or UC... (24 characters)">
                        <p class="chhelp">
                            A handle (like @name) or a youtube.com link is looked up automatically. If that lookup fails
                            from the server, paste the channel ID instead (YouTube Studio, Settings, Channel, Advanced settings).
                            Use the stream creator's channel, not your bot's.
                        </p>
                        <p class="chstatus" id="chDlgStatus" role="status" aria-live="polite"></p>
                        <div class="chactions">
                            <button type="button" id="chDlgSave" class="add-btn">Save</button>
                            <button type="button" id="chDlgCancel">Cancel</button>
                            <button type="button" id="chDlgDelete" class="chdanger" hidden>Delete</button>
                        </div>
                    </div>
                </div>
"""

CHANNEL_UI_JS = r"""
(function () {
    var channels = [];
    var selectedId = '';      // the Add Live Stream box's choice ('' = none)
    var panelState = null;    // {anchor, selectedId, onPick} while the dropdown is open
    var dlgState = null;      // {editingId, lastFocus} while the dialog is open
    var $ = function (id) { return document.getElementById(id); };

    function shortId(id) { return id.length > 14 ? id.slice(0, 6) + '\u2026' + id.slice(-6) : id; }
    function labelOf(c) { return c.name || c.handle || shortId(c.id); }
    function subOf(c) {
        if (c.name && c.handle) return c.handle + ' (' + shortId(c.id) + ')';
        if (c.name || c.handle) return shortId(c.id);
        return '';
    }
    function findChannel(id) {
        for (var i = 0; i < channels.length; i++) { if (channels[i].id === id) return channels[i]; }
        return null;
    }
    function parseJson(r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); }

    function setChannels(list) {
        channels = (list || []).slice();
        if (selectedId && !findChannel(selectedId)) selectedId = '';
        updateTrigger();
        if (panelState) renderPanel();
    }

    function updateTrigger() {
        var btn = $('channelPickerBtn');
        if (!btn) return;
        var c = selectedId ? findChannel(selectedId) : null;
        btn.querySelector('.picker-label').textContent = c ? labelOf(c) : 'Select Channel (optional)';
        btn.classList.toggle('has-value', !!c);
    }

    // ---- dropdown panel ----
    var backdrop = document.createElement('div');
    backdrop.className = 'chback';
    backdrop.hidden = true;
    var panel = document.createElement('div');
    panel.className = 'chpanel';
    panel.hidden = true;
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-label', 'Select channel');
    document.body.appendChild(backdrop);
    document.body.appendChild(panel);
    backdrop.addEventListener('click', function () { closePicker(); });

    function makeRow(c, st) {
        var isNone = !c;
        var isSel = isNone ? !st.selectedId : c.id === st.selectedId;
        var row = document.createElement('div');
        row.className = 'chrow' + (isNone ? ' none' : '') + (isSel ? ' sel' : '');
        row.setAttribute('role', 'option');
        row.setAttribute('aria-selected', isSel ? 'true' : 'false');
        row.tabIndex = 0;

        var main = document.createElement('div');
        main.className = 'chmain';
        var nm = document.createElement('span');
        nm.className = 'chname';
        nm.textContent = isNone ? 'None (automatic lookup)' : labelOf(c);
        main.appendChild(nm);
        if (!isNone) {
            var s = subOf(c);
            if (s) {
                var sub = document.createElement('span');
                sub.className = 'chsub';
                sub.textContent = s;
                main.appendChild(sub);
            }
        }
        row.appendChild(main);

        if (!isNone) {
            var ed = document.createElement('button');
            ed.type = 'button';
            ed.className = 'chedit';
            ed.textContent = '\u270F\uFE0F';
            ed.title = 'Edit';
            ed.setAttribute('aria-label', 'Edit ' + labelOf(c));
            ed.addEventListener('click', function (e) { e.stopPropagation(); openDialog(c); });
            row.appendChild(ed);
        }

        row.addEventListener('click', function () { pick(isNone ? '' : c.id); });
        row.addEventListener('keydown', function (e) {
            if (e.target !== row) return;
            if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); pick(isNone ? '' : c.id); return; }
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                var rows = Array.prototype.slice.call(panel.querySelectorAll('.chrow'));
                var next = rows[rows.indexOf(row) + (e.key === 'ArrowDown' ? 1 : -1)];
                if (next) next.focus();
            }
        });
        return row;
    }

    function renderPanel() {
        var st = panelState;
        if (!st) return;
        panel.textContent = '';
        var list = document.createElement('div');
        list.className = 'chlist';
        list.setAttribute('role', 'listbox');
        list.appendChild(makeRow(null, st));
        if (!channels.length) {
            var empty = document.createElement('div');
            empty.className = 'chempty';
            empty.textContent = 'No saved channels yet. Use Add Channel ID below.';
            list.appendChild(empty);
        }
        channels.forEach(function (c) { list.appendChild(makeRow(c, st)); });
        panel.appendChild(list);

        var foot = document.createElement('div');
        foot.className = 'chfoot';
        var add = document.createElement('button');
        add.type = 'button';
        add.className = 'add-btn';
        add.textContent = '\u2795 Add Channel ID';
        add.addEventListener('click', function () { openDialog(null); });
        foot.appendChild(add);
        panel.appendChild(foot);
    }

    function positionPanel() {
        if (!panelState) return;
        if (window.innerWidth <= 700) {
            panel.classList.add('sheet');
            panel.style.top = panel.style.left = panel.style.bottom = panel.style.width = panel.style.maxHeight = '';
            return;
        }
        panel.classList.remove('sheet');
        var r = panelState.anchor.getBoundingClientRect();
        var w = Math.min(380, window.innerWidth - 16);
        var left = Math.max(8, Math.min(r.left, window.innerWidth - w - 8));
        var below = window.innerHeight - r.bottom - 12;
        var above = r.top - 12;
        panel.style.width = w + 'px';
        panel.style.left = left + 'px';
        if (below < 240 && above > below) {
            panel.style.top = 'auto';
            panel.style.bottom = (window.innerHeight - r.top + 4) + 'px';
            panel.style.maxHeight = Math.max(160, above) + 'px';
        } else {
            panel.style.bottom = 'auto';
            panel.style.top = (r.bottom + 4) + 'px';
            panel.style.maxHeight = Math.max(160, below) + 'px';
        }
    }
    window.addEventListener('resize', positionPanel);
    window.addEventListener('scroll', positionPanel, true);

    function openPicker(anchor, opts) {
        closePicker(true);
        panelState = { anchor: anchor, selectedId: opts.selectedId || '', onPick: opts.onPick };
        renderPanel();
        backdrop.hidden = false;
        panel.hidden = false;
        anchor.setAttribute('aria-expanded', 'true');
        positionPanel();
        var first = panel.querySelector('.chrow.sel') || panel.querySelector('.chrow');
        if (first) first.focus();
    }

    function closePicker(noRefocus) {
        if (!panelState) return;
        var a = panelState.anchor;
        panelState = null;
        panel.hidden = true;
        backdrop.hidden = true;
        panel.textContent = '';
        if (a) {
            a.setAttribute('aria-expanded', 'false');
            if (!noRefocus && a.focus) a.focus();
        }
    }

    function pick(id) {
        var st = panelState;
        if (!st) return;
        closePicker(true);
        st.onPick(id);
    }

    // ---- add / edit dialog ----
    var dlgBack = $('chDlgBack');
    function setStatus(msg, isErr) {
        var el = $('chDlgStatus');
        el.textContent = msg || '';
        el.classList.toggle('err', !!isErr);
    }
    function setBusy(busy) {
        ['chDlgSave', 'chDlgCancel', 'chDlgDelete'].forEach(function (id) { $(id).disabled = busy; });
    }

    function openDialog(c) {
        dlgState = { editingId: c ? c.id : '', lastFocus: document.activeElement };
        $('chDlgTitle').textContent = c ? 'Edit Channel' : 'Add Channel ID';
        $('chDlgName').value = c ? c.name : '';
        $('chDlgInput').value = c ? (c.handle || c.id) : '';
        $('chDlgDelete').hidden = !c;
        setStatus('');
        setBusy(false);
        dlgBack.hidden = false;
        // Add: start on the display name (recommended). Edit: same.
        setTimeout(function () { $('chDlgName').focus(); }, 30);
    }

    function closeDialog() {
        if (!dlgState) return;
        var back = dlgState.lastFocus;
        dlgState = null;
        dlgBack.hidden = true;
        if (back && back.focus && document.body.contains(back)) back.focus();
    }

    function saveDialog() {
        if (!dlgState) return;
        var editedId = dlgState.editingId;
        var name = $('chDlgName').value.trim();
        var input = $('chDlgInput').value.trim();
        if (!input) { setStatus('Enter a handle or a channel ID.', true); return; }

        setBusy(true);
        setStatus(/^@|youtube\.com/i.test(input) || !/^UC/.test(input) ? 'Looking up the channel\u2026' : 'Saving\u2026');
        fetch('/api/channels', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: editedId, name: name, input: input })
        }).then(parseJson).then(function (res) {
            setBusy(false);
            var d = res.d || {};
            if (!res.ok) {
                if (d.channels) setChannels(d.channels);
                setStatus(d.message || 'Could not save.', true);
                return;
            }
            // Editing can change the ID (re-keyed): keep any selection pointing at it.
            if (editedId && d.channel && d.channel.id !== editedId) {
                if (selectedId === editedId) selectedId = d.channel.id;
                if (panelState && panelState.selectedId === editedId) panelState.selectedId = d.channel.id;
            }
            setChannels(d.channels);
            closeDialog();
            // Newly added channel: select it right away.
            if (!editedId && d.channel && panelState) pick(d.channel.id);
        }).catch(function (e) {
            setBusy(false);
            setStatus('Error: ' + e, true);
        });
    }

    function deleteFromDialog() {
        if (!dlgState || !dlgState.editingId) return;
        var id = dlgState.editingId;
        var c = findChannel(id);
        if (!confirm('Delete "' + (c ? labelOf(c) : id) + '" from the saved list?\nStreams already using it keep their channel ID.')) return;
        setBusy(true);
        setStatus('Deleting\u2026');
        fetch('/api/channels/' + encodeURIComponent(id), { method: 'DELETE' })
            .then(parseJson).then(function (res) {
                setBusy(false);
                var d = res.d || {};
                if (d.channels) setChannels(d.channels);
                if (!res.ok) { setStatus(d.message || 'Could not delete.', true); return; }
                if (panelState && panelState.selectedId === id) panelState.selectedId = '';
                closeDialog();
                if (panelState) renderPanel();
            }).catch(function (e) {
                setBusy(false);
                setStatus('Error: ' + e, true);
            });
    }

    $('chDlgSave').addEventListener('click', saveDialog);
    $('chDlgCancel').addEventListener('click', function () { closeDialog(); });
    $('chDlgDelete').addEventListener('click', deleteFromDialog);
    ['chDlgName', 'chDlgInput'].forEach(function (id) {
        $(id).addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); saveDialog(); } });
    });

    // Keep Tab inside the dialog while it is open.
    dlgBack.addEventListener('keydown', function (e) {
        if (e.key !== 'Tab') return;
        var items = Array.prototype.slice.call(dlgBack.querySelectorAll('input, button')).filter(function (el) {
            return !el.disabled && !el.hidden && el.offsetParent !== null;
        });
        if (!items.length) return;
        var first = items[0], last = items[items.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    });

    // Escape closes the dialog first, then the dropdown.
    document.addEventListener('keydown', function (e) {
        if (e.key !== 'Escape') return;
        if (dlgState) { if (!$('chDlgSave').disabled) closeDialog(); }
        else if (panelState) closePicker();
    });

    // ---- wiring ----
    var trigger = $('channelPickerBtn');
    if (trigger) {
        trigger.addEventListener('click', function () {
            openPicker(trigger, {
                selectedId: selectedId,
                onPick: function (id) { selectedId = id; updateTrigger(); }
            });
        });
    }

    window.getSelectedChannelId = function () { return selectedId; };

    // Per-row "Change Channel" button in the streams table.
    window.changeChannel = function (videoId, currentId, btn) {
        openPicker(btn, {
            selectedId: currentId,
            onPick: function (id) {
                if (id === currentId) return;
                fetch('/api/streams', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ video_id: videoId, channel_id: id })
                }).then(function (r) { return r.json(); }).then(function (data) {
                    alert(data.message);
                    if (window.__refreshView) window.__refreshView('live'); else location.reload();
                }).catch(function (e) { alert('Error: ' + e); });
            }
        });
    };

    setChannels(window.__SAVED_CHANNELS__ || []);
})();
"""

# --- Render Port Binding & HTTP Server ---
# ---------------------------------------------------------------------------
# v0.3.0.0 dashboard: one shell page with hash routing (/#/home, /#/logs, ...).
# The shell is a plain (non-f) string; __BOOT__ becomes JSON (admin flag, commands, counts).
# Live and Moderation are served as fragments (/_view/live, /_view/moderation) and run inside the shell.
# ---------------------------------------------------------------------------
BOT_VERSION = "0.3.1.1"
BOT_STARTED_AT = time.time()
DASHBOARD_PAGES = ("home", "commands", "leaderboard", "logs", "live", "moderation", "themes")


def _cached_user_count():
    """Number of NeilCoins users from the in-memory cache only (None until it is warm).
    Never calls load_coins(): that can fetch from GitHub, and a page load must not wait on the network."""
    with _coins_backup_lock:
        return len(_coins_cache) if _coins_cache is not None else None


DASHBOARD_COMMANDS = [('hello / hi / hey', 'Greets the user.'), ('!revertical', 'dawg WHO said "revertical" 😭✌️'), ('!random / !rand', 'Tells a random joke.'), ('!chatmbr <query> / !ai <query>', 'Asks ChatMBR a question.'), ('E', 'E'), ('!say <text>', 'Makes the bot repeat your text.'), ('!commands / !help', 'Displays this command list page.'), ('!coins', 'Shows your current NeilCoins balance.'), ('!gamble <number>', 'Bets that many NeilCoins — 50/50 chance to double it or lose it.'), ('!giftpoint @Username <points>', "Gifts NeilCoins to another user (can't gift yourself)."), ('!resetcoins', 'Grants +25 NeilCoins, but only if your balance is exactly 0. Max 5 uses ever.'), ('!angryrevertical', 'Lose 1,000 NeilCoins, but only if your balance is above 1,000.'), ('!leaderboard', 'Links to the /leaderboard page (top 50 users by NeilCoins).'), ('!quiz', 'Starts a 3 minute tech quiz with A, B, C, D choices (one quiz at a time per stream).'), ('!ans a', 'Answers the quiz (a, b, c or d). The first correct answer wins +67 NeilCoins. Wrong answers cost nothing and you can try again.')]

# The channel picker/dialog CSS was written for the old dark pages; map its hex palette onto the theme variables.
_THEME_HEX = {"#0d1117": "var(--bg)", "#161b22": "var(--surface)", "#30363d": "var(--line)", "#21262d": "var(--surface2)",
              "#1c2128": "var(--surface2)", "#c9d1d9": "var(--ink)", "#8b949e": "var(--muted)", "#58a6ff": "var(--accent)",
              "#1f6feb": "var(--accent)", "#f85149": "var(--bad)", "#6e2a2d": "var(--bad)"}


def _theme_css(css_text):
    return re.sub(r"#[0-9a-fA-F]{6}\b", lambda m: _THEME_HEX.get(m.group(0).lower(), m.group(0)), css_text)


DASHBOARD_SHELL_HTML = (r"""<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>ThemeMasterBot</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg?v=__VERSION__">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon-32.png?v=__VERSION__">
<link rel="apple-touch-icon" href="/apple-touch-icon.png?v=__VERSION__">
<meta name="theme-color" content="#f4f1ea">
<script>
/* Theme engine (v0.3.1.0). Modes: system (default) | light | dark | black.
   <html data-theme> always holds the RESOLVED theme (light | dark | black), so all CSS keeps working.
   Runs before first paint (no flash) and follows the phone's light/dark setting live while mode is "system". */
(function () {
  var KEY = 'tmb-theme-mode', BKEY = 'tmb-system-black', MODES = ['system', 'light', 'dark', 'black'];
  var BAR = { light: '#f4f1ea', dark: '#11141b', black: '#000000' };
  var mq = null; try { mq = window.matchMedia('(prefers-color-scheme: dark)'); } catch (e) {}
  function load(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }
  function save(k, v) { try { localStorage.setItem(k, v); return true; } catch (e) { return false; } }
  var st = { mode: 'system', sysBlack: false };
  var m = load(KEY); if (MODES.indexOf(m) > -1) st.mode = m;
  st.sysBlack = load(BKEY) === '1';
  function phoneDark() { return !!(mq && mq.matches); }
  function resolve() {
    if (st.mode !== 'system') return st.mode;
    if (!phoneDark()) return 'light';
    return st.sysBlack ? 'black' : 'dark';
  }
  function apply() {
    var t = resolve();
    document.documentElement.setAttribute('data-theme', t);
    var meta = document.querySelector('meta[name="theme-color"]'); if (meta) meta.setAttribute('content', BAR[t]);
    try { document.dispatchEvent(new Event('tmb-theme-change')); } catch (e) {}
    return t;
  }
  if (mq) {
    var onPhone = function () { if (st.mode === 'system') apply(); };
    if (mq.addEventListener) mq.addEventListener('change', onPhone); else if (mq.addListener) mq.addListener(onPhone);
  }
  window.__tmbTheme = {
    state: st, resolve: resolve, phoneDark: phoneDark, apply: apply,
    setMode: function (v) { if (MODES.indexOf(v) < 0) v = 'system'; st.mode = v; var ok = save(KEY, v); apply(); return ok; },
    setSysBlack: function (on) { st.sysBlack = !!on; var ok = save(BKEY, on ? '1' : '0'); apply(); return ok; }
  };
  apply();
})();
</script>
<style>
:root{--bg:#f4f1ea;--surface:#fffdf9;--surface2:#efeae0;--ink:#1f2430;--muted:#6a6f7b;--line:#ddd6c8;--accent:#b4440b;--accent-ink:#fff;--ok:#2d7a46;--warn:#a86a00;--bad:#b42318;--term:#faf8f3;--shadow:0 1px 2px rgba(31,36,48,.06),0 6px 18px rgba(31,36,48,.05);color-scheme:light}
:root[data-theme="black"]{--bg:#000000;--surface:#0b0b0b;--surface2:#161616;--ink:#ebe8e1;--muted:#8d94a3;--line:#262626;--accent:#f0913a;--accent-ink:#1a1206;--ok:#4cc27a;--warn:#e2a23b;--bad:#f2766b;--term:#050505;--shadow:0 1px 2px rgba(0,0,0,.6),0 6px 18px rgba(0,0,0,.5);color-scheme:dark}
:root[data-theme="dark"]{--bg:#11141b;--surface:#1a1f2a;--surface2:#232a38;--ink:#ebe8e1;--muted:#98a0b0;--line:#2c3445;--accent:#f0913a;--accent-ink:#1a1206;--ok:#4cc27a;--warn:#e2a23b;--bad:#f2766b;--term:#0d1016;--shadow:0 1px 2px rgba(0,0,0,.3),0 6px 18px rgba(0,0,0,.25);color-scheme:dark}
*{box-sizing:border-box}
html,body{margin:0;height:100%}
body{background:var(--bg);color:var(--ink);font:15px/1.5 "Segoe UI",system-ui,-apple-system,Roboto,Helvetica,Arial,sans-serif}
code,.mono{font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace}
code{background:var(--surface2);padding:2px 6px;border-radius:5px;font-size:.88em}
a{color:var(--accent)}
button,input,select,textarea{font:inherit;color:inherit}
button{cursor:pointer;background:var(--surface2);border:1px solid var(--line);border-radius:8px;padding:8px 14px;min-height:38px}
button:hover:not(:disabled){border-color:var(--accent)}
a.btn{display:inline-flex;align-items:center;justify-content:center;text-decoration:none;color:var(--ink);background:var(--surface2);border:1px solid var(--line);border-radius:8px;padding:8px 14px;min-height:38px}
a.btn:hover{border-color:var(--accent)}
a.btn.primary{background:var(--accent);color:var(--accent-ink);border-color:var(--accent);font-weight:600}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
button.primary{background:var(--accent);color:var(--accent-ink);border-color:var(--accent);font-weight:600}
button.danger{color:var(--bad)}
button:disabled,input:disabled,select:disabled,textarea:disabled{opacity:.5;cursor:not-allowed}
input[type=text],input[type=password],input[type=number],select,textarea{background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:8px 10px;min-height:38px;max-width:100%}
textarea{width:100%;min-height:60px}
.app{display:grid;grid-template-columns:230px 1fr;min-height:100%}
.side{background:var(--surface);border-right:1px solid var(--line);padding:18px 14px;position:sticky;top:0;align-self:start;height:100vh;display:flex;flex-direction:column;gap:14px}
.brand{display:flex;align-items:center;gap:10px;font-weight:700;font-size:17px;letter-spacing:-.01em}
.brand i{width:30px;height:30px;border-radius:9px;background:var(--accent);color:var(--accent-ink);display:grid;place-items:center;font-style:normal;font-weight:800}
.brand small{display:block;font-weight:500;font-size:11px;color:var(--muted)}
nav{display:flex;flex-direction:column;gap:2px}
nav a{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:9px;color:var(--ink);text-decoration:none;font-weight:500}
nav a:hover{background:var(--surface2)}
nav a.on{background:var(--accent);color:var(--accent-ink)}
.sudo{margin-top:auto;border:1px solid var(--line);border-radius:12px;padding:12px;background:var(--bg)}
.sudo .st{font-size:12px;color:var(--muted);margin-bottom:8px}
.sudo.on .st{color:var(--ok)}
.sudo button{width:100%}
main{padding:28px clamp(16px,4vw,40px) 60px;max-width:1100px;width:100%}
h1{font-size:26px;margin:0 0 4px;letter-spacing:-.02em}
.sub{color:var(--muted);margin:0 0 22px}
#view.refreshing{opacity:.45;transition:opacity .15s}
.rf-badge{position:fixed;top:14px;left:50%;transform:translateX(-50%);z-index:60;background:var(--surface);color:var(--ink);border:1px solid var(--line);border-radius:999px;padding:6px 16px;font-size:13px;box-shadow:var(--shadow)}
.card{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:18px;box-shadow:var(--shadow);margin-bottom:16px}
.card h3{margin:0 0 10px;font-size:15px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;margin-bottom:16px}
.stat{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:16px;box-shadow:var(--shadow)}
.stat b{display:block;font-size:28px;letter-spacing:-.02em}
.stat span{color:var(--muted);font-size:13px}
.hero{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:26px;margin-bottom:16px;box-shadow:var(--shadow);border-left:6px solid var(--accent)}
.hero h1{font-size:30px}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:var(--ok);margin-right:6px}
.tablewrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid var(--line);vertical-align:middle}
th{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted)}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
label.f{display:block;font-size:13px;color:var(--muted);margin:10px 0 4px}
.term{background:var(--term);border:1px solid var(--line);border-radius:12px;height:min(62vh,520px);overflow-y:auto;padding:10px 14px;font-size:13px;position:relative}
.term div{padding:3px 0;border-bottom:1px solid var(--line);word-break:break-word}
.term .t{color:var(--muted)}
.lockbar{background:var(--surface2);border:1px dashed var(--line);border-radius:10px;padding:9px 12px;font-size:13px;margin-bottom:14px}
.themes{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(160px,100%),1fr));gap:16px}
.tnote{color:var(--muted);margin:14px 0 0}
.tcheck{display:flex;align-items:flex-start;gap:10px;margin-top:14px;padding:12px 14px;border:1px solid var(--line);border-radius:12px;background:var(--surface);cursor:pointer}
.tcheck input{width:20px;height:20px;margin:2px 0 0;accent-color:var(--accent);flex:none}
.tcheck small{display:block;color:var(--muted)}
.tcard{border:2px solid var(--line);border-radius:14px;padding:14px;background:var(--surface);text-align:left;display:block;width:100%}
.tcard.sel{border-color:var(--accent)}
.swatch{border-radius:10px;height:92px;margin-bottom:10px;padding:10px;display:flex;flex-direction:column;gap:6px;border:1px solid #8884}
.swatch i{display:block;height:10px;border-radius:5px}
.nf{text-align:center;padding:60px 10px}
.nf .big{font-size:clamp(70px,18vw,140px);font-weight:800;letter-spacing:-.05em;color:var(--accent);line-height:1}
#toast{position:fixed;left:50%;bottom:22px;transform:translateX(-50%);background:var(--ink);color:var(--bg);padding:10px 16px;border-radius:10px;font-size:14px;z-index:60;max-width:90vw}
#toast[hidden],#modal[hidden]{display:none}
#modal{position:fixed;inset:0;background:rgba(5,8,14,.6);display:grid;z-index:50;padding:16px;overflow-y:auto;overscroll-behavior:contain}
#modal .box{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:20px;width:min(380px,100%);margin:auto;overflow-wrap:anywhere}
#modal h3{margin:0 0 6px}
.tag{font-size:11px;background:var(--warn);color:#fff;border-radius:6px;padding:2px 6px;margin-left:6px;vertical-align:middle}
@media (max-width:820px){
  .app{grid-template-columns:1fr}
  .side{position:sticky;top:0;z-index:20;height:auto;flex-direction:row;flex-wrap:wrap;align-items:center;padding:10px 12px;gap:8px;border-right:0;border-bottom:1px solid var(--line)}
  .brand small{display:none}
  nav{flex-direction:row;overflow-x:auto;order:3;width:100%;gap:4px;padding-bottom:2px}
  nav a{white-space:nowrap;padding:8px 12px}
  .sudo{margin:0 0 0 auto;padding:6px 8px;display:flex;align-items:center;gap:8px}
  .sudo .st{margin:0}
  .sudo button{width:auto;min-height:34px;padding:4px 10px}
  input[type=text],input[type=password],input[type=number],select,textarea{font-size:16px}
  main{padding-top:18px}
}

/* ===== responsive + accessibility layer ===== */
.app{overflow-x:clip}
.side,main{min-width:0}
.statsbox{container-type:inline-size}
body{padding-left:env(safe-area-inset-left,0px);padding-right:env(safe-area-inset-right,0px)}
#toast{bottom:calc(22px + env(safe-area-inset-bottom,0px))}
code,td,.mono,.term div{overflow-wrap:anywhere}
nav{scrollbar-width:thin;overscroll-behavior-x:contain}
.vh{position:absolute;width:1px;height:1px;margin:-1px;padding:0;border:0;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
.cell-label{display:none}
label.cell-label.ctl{display:block;position:absolute;width:1px;height:1px;margin:-1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
.ts{white-space:nowrap}
.act{padding:3px 0}
.grow{flex:1;min-width:150px}.w70{width:70px}.w80{width:80px}
.grid.stats{grid-template-columns:repeat(2,minmax(0,1fr))}
.stat b{overflow-wrap:anywhere;font-size:22px}
.stat span{overflow-wrap:anywhere}
.stat b.up{font-size:22px;font-variant-numeric:tabular-nums;overflow-wrap:normal}
.nw{white-space:nowrap}
.lb td.usr{overflow-wrap:break-word}
.lb td.bal{overflow-wrap:normal;word-break:keep-all}
@media (max-width:640px){ .lbcard{padding:10px} .lb th,.lb td{padding:8px 4px} .lb td{font-size:14px} .lb th{font-size:11px;letter-spacing:.03em} }
@container (min-width:640px){
  .grid.stats{grid-template-columns:repeat(4,minmax(0,1fr))}
  .stat b{font-size:28px;font-size:clamp(22px,3.6cqi,28px)}
}
@container (max-width:639px){ .stat b{font-size:clamp(20px,7cqi,24px)} .stat b.up{font-size:16px} }
@media (max-width:820px){
  nav{position:relative;scroll-padding-inline:12px;-webkit-overflow-scrolling:touch}
  nav a{flex:0 0 auto}
}
@media (max-width:640px){
  main{padding:14px 12px 40px}
  h1{font-size:22px}.hero{padding:18px}.hero h1{font-size:24px}.sub{margin-bottom:16px}
  .card{padding:14px;margin-bottom:12px;border-radius:12px}
  .stat{padding:12px}
  th,td{padding:8px 10px}
  .term{padding:8px 10px;height:min(65vh,560px);min-height:240px}
  .nf{padding:36px 6px}.tcard{padding:12px}
  .addform>*{flex:1 1 100%}
  .bulk>button{flex:1 1 140px}
  .fields>label{flex:1 1 100%;display:flex;align-items:center;justify-content:space-between;gap:10px}
  .fields>button{flex:1 1 100%}
  .fields input[type=number]{width:96px}
  /* stacked cards for the Live and Moderation per-stream tables */
  .stk,.stk tbody,.stk tr,.stk td{display:block}
  .stk thead{display:block;position:absolute;width:1px;height:1px;margin:-1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
  .stk tr{border:1px solid var(--line);border-radius:12px;background:var(--bg);padding:2px 12px;margin-bottom:12px}
  .stk td{display:flex;align-items:center;justify-content:space-between;gap:12px;border-bottom:1px dashed var(--line);padding:9px 0}
  .stk tr td:last-child{border-bottom:0}
  .stk .cell-label,.stk label.cell-label.ctl{display:block;position:static;width:auto;height:auto;margin:0;overflow:visible;clip-path:none;white-space:normal;flex:0 0 38%;font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted)}
  .stk .cv{flex:1 1 auto;min-width:0;text-align:right}
  .stk input[type=checkbox]{margin-left:auto}
  .stk input[type=number]{width:96px}
  .stk .btns{flex:1 1 auto;justify-content:flex-end}
}
@media (max-width:480px){
  .brand{font-size:15px;min-width:0}.brand i{width:26px;height:26px}
  .sudo .st{position:absolute;width:1px;height:1px;margin:-1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
}
@media (pointer:coarse){
  button,a.btn,nav a,.sudo button,input[type=text],input[type=password],input[type=number],select{min-height:44px}
  input[type=checkbox]{width:22px;height:22px}
}

/* ===== Live / Moderation fragments (existing markup, themed) ===== */
.frag .box,.frag .add-box,.frag .backup-box{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:18px;box-shadow:var(--shadow);margin-bottom:16px}
.frag .backup-box{border-color:var(--warn)}.frag .backup-box h3{color:var(--warn)}
.frag h2{font-size:18px;margin:20px 0 8px;letter-spacing:-.01em}
.frag h3{margin:0 0 8px;font-size:15px}
.frag .note{color:var(--muted);font-size:13px}
.frag .box label,.frag .add-box label{display:block;margin:10px 0 4px;color:var(--muted);font-size:13px}
.frag input[type=text]:not(.grow),.frag textarea{width:100%}
.frag textarea{min-height:70px;margin-bottom:8px;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px}
.frag .add-box input{margin:0 8px 8px 0}
.frag .add-btn,.frag .save-btn{background:var(--ok);color:var(--bg);border-color:var(--ok);font-weight:700}
.frag-mod .add-btn,.frag .restore-btn{background:var(--accent);color:var(--accent-ink);border-color:var(--accent);font-weight:700}
.frag .copy-btn{background:var(--warn);color:var(--bg);border-color:var(--warn);font-weight:700}
.frag .danger-btn,.frag .delete-btn{background:var(--bad);color:var(--bg);border-color:var(--bad);font-weight:700}
.frag .bulk-bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:10px 0}
.frag .bulk-bar button{margin:0}
.frag .stream-check,.frag #selectAllStreams{width:20px;height:20px;cursor:pointer}
.frag .box button,.frag .add-box button{margin:4px 8px 4px 0}
.frag .sa-text{display:none}
@media (max-width:640px){
  .frag .add-box input[type=text]{width:100% !important;margin-right:0}
  .frag .add-btn{width:100%}
  .stk-live thead{display:block;position:static;width:auto;height:auto;margin:0;overflow:visible;clip-path:none}
  .stk-live thead tr{display:block;border:0;background:none;padding:0 2px 6px;margin:0}
  .stk-live thead th{position:absolute;width:1px;height:1px;margin:-1px;padding:0;border:0;overflow:hidden;clip-path:inset(50%)}
  .stk-live thead th:first-child{position:static;width:auto;height:auto;margin:0;overflow:visible;clip-path:none;display:flex;align-items:center;gap:10px;padding:4px 0}
  .stk-live .sa-text{display:inline}
  .stk textarea{width:100%;margin:0}
  .stk .cv{text-align:right}
}
__CHANNEL_CSS__
</style>
</head>
<body>
<div class="app" id="app">
  <aside class="side">
    <div class="brand"><i>T</i><div>ThemeMasterBot<small>Dashboard v__VERSION__</small></div></div>
    <nav id="nav" aria-label="Main"></nav>
    <div class="sudo" id="sudo"><div class="st" id="sudoSt" role="status">🔒 Read-only</div><button id="sudoBtn" type="button">🔒 Sudo mode</button></div>
  </aside>
  <main id="view"></main>
</div>
<div id="modal" hidden><div class="box" role="dialog" aria-modal="true" aria-labelledby="mTitle" aria-describedby="mDesc">
  <h3 id="mTitle">🔒 Sudo mode</h3>
  <p id="mDesc" style="color:var(--muted);font-size:13px;margin:0 0 10px">Enter the password to edit. This device stays logged in for 30 days, or until you tap Log out.</p>
  <input type="password" id="pw" placeholder="Password" style="width:100%" autocomplete="current-password">
  <p id="pwErr" role="alert" style="color:var(--bad);min-height:18px;margin:6px 0;font-size:13px"></p>
  <div class="row"><button class="primary" id="pwOk" type="button" style="flex:1">Unlock</button><button id="pwNo" type="button" style="flex:1">Cancel</button></div>
</div></div>
<div id="toast" hidden></div>
<script type="application/json" id="boot">__BOOT__</script>
<script>
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var view = $('view'), boot = {};
  try { boot = JSON.parse($('boot').textContent); } catch (e) {}
  var admin = !!boot.admin;           // cosmetic only: the server enforces sudo on every write
  var cleanups = [], navTok = 0;
  var FRAG = { live: 1, moderation: 1 };

  function esc(s) { return String(s).replace(/[&<>"']/g, function (c) { return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function pad(n) { return (n < 10 ? '0' : '') + n; }
  function hhmmss(d) { return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()); }
  var toastT;
  function toast(m, ms) { var t = $('toast'); t.textContent = m; t.hidden = false; clearTimeout(toastT); toastT = setTimeout(function () { t.hidden = true; }, ms || 2600); }
  function getJson(url) { return fetch(url, { cache: 'no-store' }).then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); }); }
  function href(k) { return (boot.notFound ? '/' : '') + '#/' + k; }
  function head(t, s) { return '<h1>' + t + '</h1><p class="sub">' + s + '</p>'; }
  function fmtBig(v) {          /* thousands separators on the decimal string itself: no Number(), parseInt() or unary + */
    var m = /^(-?)(\d+)$/.exec(String(v));
    if (!m) return String(v);
    var d = m[2].replace(/^0+(?=\d)/, '');
    return (d === '0' ? '' : m[1]) + d.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }
  var loadedAt = Date.now();      /* uptime = server uptime when the page was built + time elapsed here, so the device's own clock offset or timezone does not matter */
  function tickUptime() {
    var a = $('upA'), b = $('upB'); if (!a || !b) return;
    if (boot.startedAt == null || boot.serverNow == null) { a.textContent = '–'; b.textContent = ''; return; }
    var s = Math.floor(Math.max(0, boot.serverNow - boot.startedAt + (Date.now() - loadedAt) / 1000));
    a.textContent = Math.floor(s / 86400) + 'd ' + Math.floor(s % 86400 / 3600) + 'h';
    b.textContent = Math.floor(s % 3600 / 60) + 'm ' + (s % 60) + 's';
  }
  /* text with invisible line-break opportunities after each delimiter (a capturing group); textContent stays identical */
  function softWrap(td, text, delim, longRun) {
    String(text).split(delim).forEach(function (p, i) {
      if (!p) return;
      if (i % 2 === 0 && longRun && Array.from(p).length > longRun) {      /* very long unbroken token: last-resort break opportunity every 6 characters */
        var cps = Array.from(p);
        for (var k = 0; k < cps.length; k += 6) { if (k) td.appendChild(document.createElement('wbr')); td.appendChild(document.createTextNode(cps.slice(k, k + 6).join(''))); }
      } else td.appendChild(document.createTextNode(p));
      if (i % 2 === 1) td.appendChild(document.createElement('wbr'));
    });
  }

  /* ---------- router ---------- */
  var ROUTES = {
    home: ['🏠', 'Home', renderHome], commands: ['📜', 'Commands', renderCommands], leaderboard: ['🏆', 'Leaderboard', renderBoard],
    logs: ['🧾', 'Logs', renderLogs], live: ['📡', 'Live', function () { loadFragment('live'); }],
    moderation: ['🛡️', 'Moderation', function () { loadFragment('moderation'); }], themes: ['🎨', 'Themes', renderThemes]
  };
  $('nav').innerHTML = Object.keys(ROUTES).map(function (k) {
    return '<a href="' + href(k) + '" data-r="' + k + '"><span>' + ROUTES[k][0] + '</span>' + ROUTES[k][1] + '</a>';
  }).join('');

  function current() { return location.hash.replace(/^#\/?/, '').split('?')[0].replace(/\/+$/, '').toLowerCase() || 'home'; }

  function render() {
    cleanups.forEach(function (fn) { try { fn(); } catch (e) {} }); cleanups = []; navTok++; endSoft();
    var k = boot.notFound ? '' : current();
    document.querySelectorAll('#nav a').forEach(function (a) {
      var on = a.dataset.r === k; a.classList.toggle('on', on);
      if (on) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
    });
    var act = document.querySelector('#nav a.on'), nv = $('nav');
    if (act && nv.scrollWidth > nv.clientWidth) {
      var off = act.getBoundingClientRect().left - nv.getBoundingClientRect().left + nv.scrollLeft;
      nv.scrollLeft = Math.max(0, off - (nv.clientWidth - act.offsetWidth) / 2);
    }
    if (ROUTES[k]) { document.title = ROUTES[k][1] + ' - ThemeMasterBot'; ROUTES[k][2](); }
    else { document.title = '404 - ThemeMasterBot'; render404(); }
    applyLock();
    window.scrollTo(0, 0);
  }
  if (!boot.notFound && (!location.hash || location.hash === '#')) {        /* "/" -> /#/home and legacy "/logs" -> /#/logs, no reload */
    var start = '/#/' + (boot.legacy || 'home');
    try { history.replaceState(null, '', start); } catch (e) { location.hash = '#/' + (boot.legacy || 'home'); }
  }
  window.addEventListener('hashchange', render);

  /* ---------- sudo (real: talks to the existing /api/auth endpoints) ---------- */
  function applyLock() {
    $('sudo').classList.toggle('on', admin);
    $('sudoSt').textContent = admin ? '🔓 Sudo mode active' : '🔒 Read-only';
    $('sudoBtn').textContent = admin ? '🔓 Log out' : '🔒 Sudo mode';
    $('sudoBtn').classList.toggle('on', admin);
    var lb = $('lockbar'); if (lb) lb.textContent = admin ? '🔓 Sudo mode active: editing is enabled.' : '🔒 Read-only: unlock sudo mode to edit. The server also enforces this.';
    if (!admin && FRAG[current()] && !boot.notFound) {    /* same rule as before: lock every control except the sudo UI and the harmless Copy button */
      view.querySelectorAll('button, input, textarea, select').forEach(function (el) { if (!el.classList.contains('copy-btn')) el.disabled = true; });
    }
  }
  function setInert(on) { [$('app')].forEach(function (el) { if (on) el.setAttribute('inert', ''); else el.removeAttribute('inert'); }); }
  function openModal() { $('pwErr').textContent = ''; $('pw').value = ''; $('modal').hidden = false; setInert(true); setTimeout(function () { $('pw').focus(); }, 50); }
  function closeModal() { $('modal').hidden = true; setInert(false); $('sudoBtn').focus(); }
  function doLogin() {
    var pw = $('pw'), err = $('pwErr'), ok = $('pwOk');
    if (!pw.value) { err.textContent = 'Enter the password to continue.'; return; }
    ok.disabled = true;
    fetch('/api/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password: pw.value }) })
      .then(function (r) { return r.json().then(function (d) { return { s: r.status, d: d }; }); })
      .then(function (x) {
        if (x.s === 200) { location.reload(); return; }
        err.textContent = (x.d && x.d.message) || 'Login failed.'; ok.disabled = false; pw.select();
      })
      .catch(function () { err.textContent = 'Network error. Try again.'; ok.disabled = false; });
  }
  function doLogout() {
    fetch('/api/auth/logout', { method: 'POST' }).catch(function () {}).then(function () {
      try { sessionStorage.setItem('sudoMsg', "Logged out. If you tap Sudo mode again, you'll need to enter the password again."); } catch (e) {}
      location.reload();
    });
  }
  $('sudoBtn').addEventListener('click', function () { if (admin) doLogout(); else openModal(); });
  $('pwOk').addEventListener('click', doLogin);
  $('pwNo').addEventListener('click', closeModal);
  $('modal').addEventListener('click', function (e) { if (e.target === $('modal')) closeModal(); });
  $('pw').addEventListener('keydown', function (e) { if (e.key === 'Enter') doLogin(); });
  $('modal').addEventListener('keydown', function (e) {
    if (e.key === 'Escape') { e.preventDefault(); closeModal(); return; }
    if (e.key !== 'Tab') return;
    var f = [$('pw'), $('pwOk'), $('pwNo')], i = f.indexOf(document.activeElement);
    if (e.shiftKey && i <= 0) { e.preventDefault(); f[f.length - 1].focus(); }
    else if (!e.shiftKey && (i === -1 || i === f.length - 1)) { e.preventDefault(); f[0].focus(); }
  });
  $('toast').addEventListener('click', function () { $('toast').hidden = true; });
  try { var pending = sessionStorage.getItem('sudoMsg'); if (pending) { sessionStorage.removeItem('sudoMsg'); toast(pending, 9000); } } catch (e) {}

  /* ---------- pages ---------- */
  function logHtml(t) { var m = /^(\[[^\]]*\]) ([\s\S]*)$/.exec(t); return m ? '<span class="ts">' + esc(m[1]) + '</span> ' + esc(m[2]) : esc(t); }
  function logNode(t) {
    var d = document.createElement('div'), m = /^(\[[^\]]*\]) ([\s\S]*)$/.exec(t);
    if (m) { var sp = document.createElement('span'); sp.className = 'ts'; sp.textContent = m[1]; d.appendChild(sp); d.appendChild(document.createTextNode(' ' + m[2])); }
    else d.textContent = t;
    return d;
  }

  function renderHome() {
    var n = boot.listening || 0;
    view.innerHTML = '<div class="hero"><h1>Welcome back, Theme Master</h1><p class="sub" style="margin:0"><span class="dot"></span>Bot online · listening to ' + n + ' stream' + (n === 1 ? '' : 's') + '</p></div>' +
      '<div class="statsbox"><div class="grid stats"><div class="stat"><b>' + n + '</b><span>Streams listening</span></div><div class="stat"><b>' + (boot.commands || []).length + '</b><span>Chat commands</span></div><div class="stat"><b>' + (boot.users == null ? '–' : Number(boot.users).toLocaleString('en-US')) + '</b><span>Users with NeilCoins</span></div><div class="stat"><b class="up" id="uptime" role="timer"><span class="nw" id="upA"></span> <span class="nw" id="upB"></span></b><span>Uptime</span></div></div></div>' +
      '<div class="card"><h3>Jump to</h3><div class="row">' + Object.keys(ROUTES).filter(function (k) { return k !== 'home'; }).map(function (k) { return '<a class="btn" href="' + href(k) + '">' + ROUTES[k][0] + ' ' + ROUTES[k][1] + '</a>'; }).join('') + '</div></div>' +
      '<div class="card"><h3>Latest activity</h3><div class="mono" id="act" style="font-size:13px">Loading…</div></div>';
    tickUptime();
    var upT = setInterval(tickUptime, 1000);
    cleanups.push(function () { clearInterval(upT); });
    getJson('/api/logs?since=0').then(function (d) {
      var a = $('act'); if (!a) return;
      a.innerHTML = d.entries.slice(-4).map(function (e) { return '<div class="act">' + logHtml(e.text) + '</div>'; }).join('') || 'No chat activity logged yet.';
    }).catch(function () { var a = $('act'); if (a) a.textContent = 'Could not load activity.'; });
  }

  function renderCommands() {
    view.innerHTML = head('📜 Available Bot Commands', 'New users start with <b>' + esc(boot.startCoins) + ' NeilCoins</b> automatically.') +
      '<div class="card tablewrap"><table><thead><tr><th>Command</th><th>Description</th></tr></thead><tbody>' +
      (boot.commands || []).map(function (c) { return '<tr><td><code>' + esc(c[0]) + '</code></td><td>' + esc(c[1]) + '</td></tr>'; }).join('') + '</tbody></table></div>';
  }

  function renderBoard() {
    view.innerHTML = head('🏆 NeilCoins Leaderboard', '<span id="cnt"></span> user(s) shown. New viewers start with ' + esc(boot.startCoins) + ' NeilCoins. Updates every 30 seconds <span id="chk"></span>') +
      '<div class="card tablewrap lbcard"><table class="lb"><thead><tr><th>Rank</th><th>User</th><th>NeilCoins</th><th>Resets used</th></tr></thead><tbody id="lbody"><tr><td colspan="4">Loading…</td></tr></tbody></table></div>';
    function load() {
      getJson('/api/leaderboard').then(function (d) {
        var tb = $('lbody'); if (!tb) return;
        tb.textContent = '';
        d.rows.forEach(function (r) {
          var tr = document.createElement('tr');
          [[r.rank], [r.name, /([_\-. ]+)/, 'usr', 10], [fmtBig(r.balance), /(,)/, 'bal'], [r.resets]].forEach(function (c) {
            var td = document.createElement('td');
            if (c[2]) td.className = c[2];
            if (c[1]) softWrap(td, c[0], c[1], c[3]); else td.textContent = c[0];
            tr.appendChild(td);
          });
          tb.appendChild(tr);
        });
        if (!d.rows.length) { tb.innerHTML = '<tr><td colspan="4">No NeilCoins data yet.</td></tr>'; }
        $('cnt').textContent = 'Top ' + d.rows.length;
        $('chk').textContent = '(last checked ' + hhmmss(new Date()) + ')';
      }).catch(function () { var c = $('chk'); if (c) c.textContent = '(reconnecting…)'; });
    }
    load();
    var t = setInterval(load, 30000);
    cleanups.push(function () { clearInterval(t); });
  }

  function renderLogs() {
    view.innerHTML = head('🤖 YouTube Bot Live Logs (UTC)', 'Oldest at the top, newest at the bottom. <span id="lst"></span>') +
      '<div class="row" style="margin-bottom:10px"><button id="pz" type="button">⏸ Pause</button><button id="jump" type="button" hidden>↓ Jump to latest</button></div><div class="term mono" id="term"><span class="sub">Loading…</span></div>';
    var term = $('term'), lst = $('lst'), jump = $('jump'), pz = $('pz');
    var lastId = 0, paused = false, seen = {}, timer = null, failures = 0, inFlight = false, resync = false, first = true;
    function setStatus(m, err) { lst.textContent = m; lst.style.color = err ? 'var(--warn)' : ''; }
    function near() { return term.scrollHeight - term.scrollTop - term.clientHeight < 40; }
    function add(entries) {
      var stick = first || near();
      if (first) { term.textContent = ''; first = false; }
      entries.forEach(function (e) {
        if (seen[e.id]) return;
        seen[e.id] = 1; lastId = Math.max(lastId, e.id);
        var d = logNode(e.text); d.dataset.id = e.id; term.appendChild(d);
      });
      while (term.children.length > 100) { delete seen[term.firstChild.dataset.id]; term.removeChild(term.firstChild); }
      var note = term.querySelector('.empty');
      if (!term.querySelector('[data-id]')) {
        if (!note) { note = document.createElement('div'); note.className = 'empty'; note.textContent = 'No chat activity logged yet.'; term.appendChild(note); }
      } else if (note) note.parentNode.removeChild(note);
      if (stick) term.scrollTop = term.scrollHeight; else jump.hidden = false;
    }
    function poll() {
      if (inFlight) return; inFlight = true;
      getJson('/api/logs?since=' + lastId).then(function (d) {
        failures = 0;
        if (d.latest < lastId) { term.textContent = ''; seen = {}; lastId = 0; first = true; resync = true; return; }   /* server restarted: resync without a reload */
        add(d.entries); setStatus('Updated ' + hhmmss(new Date()), false);
      }).catch(function () { failures++; if (failures >= 2) setStatus('Reconnecting…', true); })
        .then(function () { inFlight = false; if (resync) { resync = false; poll(); } });
    }
    poll(); timer = setInterval(poll, 5000);
    pz.addEventListener('click', function () {
      paused = !paused; pz.textContent = paused ? '▶ Resume' : '⏸ Pause';
      if (paused) { clearInterval(timer); setStatus('Paused', false); } else { poll(); timer = setInterval(poll, 5000); }
    });
    jump.addEventListener('click', function () { term.scrollTop = term.scrollHeight; jump.hidden = true; });
    term.addEventListener('scroll', function () { if (near()) jump.hidden = true; });
    cleanups.push(function () { clearInterval(timer); });
  }

  /* Live and Moderation: the server sends the page body plus its scripts as a fragment. The scripts run in
     a fresh scope each visit (indirect eval), and every listener or node they add outside the fragment is removed on leave. */
  function runScripts(codes) {
    var before = Array.prototype.slice.call(document.body.children), tracked = [];
    var dHad = Object.prototype.hasOwnProperty.call(document, 'addEventListener'), wHad = Object.prototype.hasOwnProperty.call(window, 'addEventListener');
    var dOrig = document.addEventListener, wOrig = window.addEventListener;
    document.addEventListener = function (t, f, o) { tracked.push([document, t, f, o]); return dOrig.call(document, t, f, o); };
    window.addEventListener = function (t, f, o) { tracked.push([window, t, f, o]); return wOrig.call(window, t, f, o); };
    try { codes.forEach(function (c) { try { (0, eval)(c); } catch (e) { console.error(e); } }); }
    finally {
      if (dHad) document.addEventListener = dOrig; else delete document.addEventListener;
      if (wHad) window.addEventListener = wOrig; else delete window.addEventListener;
    }
    var added = Array.prototype.slice.call(document.body.children).filter(function (n) { return before.indexOf(n) === -1; });
    cleanups.push(function () {
      tracked.forEach(function (t) { t[0].removeEventListener(t[1], t[2], t[3]); });
      added.forEach(function (n) { if (n.parentNode) n.parentNode.removeChild(n); });
    });
  }
  /* Soft refresh (v0.3.0.2): keeps the old content on screen (dimmed, not clickable) with a Loading badge,
     swaps in the fresh fragment, and restores the scroll position. Nothing reloads the page. */
  function endSoft() {
    view.classList.remove('refreshing'); view.removeAttribute('inert'); view.removeAttribute('aria-busy');
    var b = $('rfBadge'); if (b && b.parentNode) b.parentNode.removeChild(b);
  }
  function loadFragment(name, soft) {
    var tok = navTok, keepY = window.scrollY;
    if (soft) {
      view.classList.add('refreshing'); view.setAttribute('inert', ''); view.setAttribute('aria-busy', 'true');
      var badge = document.createElement('div'); badge.id = 'rfBadge'; badge.className = 'rf-badge'; badge.setAttribute('role', 'status'); badge.textContent = 'Loading…';
      document.body.appendChild(badge);
    } else {
      view.innerHTML = '<p class="sub">Loading…</p>';
    }
    fetch('/_view/' + name, { cache: 'no-store' }).then(function (r) { if (!r.ok) throw new Error(r.status); return r.text(); }).then(function (html) {
      if (tok !== navTok) return;
      var tpl = document.createElement('template'); tpl.innerHTML = html;
      var codes = Array.prototype.slice.call(tpl.content.querySelectorAll('script')).map(function (s) { var c = s.textContent; s.parentNode.removeChild(s); return c; });
      view.innerHTML = ''; view.appendChild(tpl.content);
      endSoft();
      runScripts(codes); applyLock();
      if (soft) window.scrollTo(0, keepY);
    }).catch(function () {
      if (tok !== navTok) return;
      if (soft) { endSoft(); toast('Could not refresh. Check your connection and reload the page.', 4000); return; }
      view.innerHTML = '<div class="card"><h3>Could not load this page</h3><p class="sub">Check your connection and try again.</p><button type="button" id="retry">Retry</button></div>';
      $('retry').addEventListener('click', render);
    });
  }
  /* Called by the Live / Moderation fragments after an add, edit or remove. Only refreshes if that tab is still open. */
  window.__refreshView = function (name) {
    if (boot.notFound || !FRAG[name] || current() !== name) return;
    loadFragment(name, true);
  };

  function renderThemes() {
    var T = window.__tmbTheme, st = T.state;
    var names = { light: 'Light', dark: 'Dark', black: 'Black' };
    function card(id, name, bg, ink, ac) {
      var sel = st.mode === id;
      return '<button type="button" class="tcard' + (sel ? ' sel' : '') + '" data-th="' + id + '"><div class="swatch" style="background:' + bg + '"><i style="background:' + ink + ';width:60%"></i><i style="background:' + ink + ';opacity:.5;width:80%"></i><i style="background:' + ac + ';width:30%"></i></div><b>' + name + '</b> ' + (sel ? '✓ Active' : '') + (id === 'system' ? ' <span style="color:var(--muted);font-size:12px">(default)</span>' : '') + '</button>';
    }
    var sysBg = 'linear-gradient(90deg,#f4f1ea 50%,' + (st.sysBlack ? '#000000' : '#11141b') + ' 50%)';
    var note = st.mode === 'system'
      ? 'Following your phone: ' + names[T.resolve()] + '.'
      : 'Fixed theme: ' + names[T.resolve()] + ' (not following your phone).';
    view.innerHTML = head('🎨 Themes', 'Your choice is saved in this browser and applies to every page.') +
      '<div class="themes">' + card('system', '🖥️ System', sysBg, '#888888', '#d9742a') + card('light', '☀️ Light', '#f4f1ea', '#1f2430', '#b4440b') + card('dark', '🌙 Dark', '#11141b', '#ebe8e1', '#f0913a') + card('black', '⚫ Black', '#000000', '#ebe8e1', '#f0913a') + '</div>' +
      '<p class="tnote" id="tnote">' + note + '</p>' +
      '<label class="tcheck"><input type="checkbox" id="sysBlack"' + (st.sysBlack ? ' checked' : '') + '><span>When System is dark, use Black instead of Dark<small>Only applies while System is selected. Black turns AMOLED pixels off.</small></span></label>';
    view.querySelectorAll('[data-th]').forEach(function (b) {
      b.addEventListener('click', function () {
        if (!T.setMode(b.dataset.th)) toast('Theme applied for this session only (browser storage unavailable).');
        renderThemes();
      });
    });
    $('sysBlack').addEventListener('change', function (e) {
      if (!T.setSysBlack(e.target.checked)) toast('Theme applied for this session only (browser storage unavailable).');
      renderThemes();
    });
    /* While this tab is open, refresh the note if the phone flips between light and dark. */
    var onChange = function () { var n = $('tnote'); if (n && st.mode === 'system') n.textContent = 'Following your phone: ' + names[T.resolve()] + '.'; };
    document.addEventListener('tmb-theme-change', onChange);
    cleanups.push(function () { document.removeEventListener('tmb-theme-change', onChange); });
  }

  function render404() {
    view.innerHTML = '<div class="nf"><div class="big">404</div><h1>404 - Page Not Found</h1><p class="sub">The page you are looking for does not exist or has moved.</p><a class="btn primary" href="' + href('home') + '">Back to the Homepage</a></div>';
  }

  render();
})();
</script>
</body>
</html>
""").replace("__CHANNEL_CSS__", _theme_css(CHANNEL_UI_CSS)).replace("__VERSION__", BOT_VERSION)


# ---------------------------------------------------------------------------
# v0.3.1.1: tab icon (favicon). Same orange rounded square + dark "T" as the dashboard header logo.
# The T is drawn as shapes (not text) so it looks identical on every device. PNGs are embedded as base64.
# ---------------------------------------------------------------------------
FAVICON_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">'
    '<rect width="64" height="64" rx="19" fill="#f0913a"/>'
    '<rect x="16" y="17" width="32" height="9" fill="#1a1206"/>'
    '<rect x="27" y="17" width="10" height="31" fill="#1a1206"/>'
    '</svg>'
).encode("utf-8")
FAVICON_PNG_32 = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAADLklEQVR42u1XTWhcVRT+vvPum8nMvJlkYlpLEYVG3LQKbgSzkeBCXQS7aRdddVUQCq5cWtu1CA0UirhxbVeSlaCIP9h96dZCsVBBmkwz/+/ee46LyaTJOGaSiTKbHrib93PPd77znXPuJXbMDCRhANBYX1kuOrkcTNegPKOwzAzEEYyECdiC2ANH2egH/Xrh499+H/XFvQ/ss3dc50S4LsRHBSeLeTD4qFDDVCYE0kRQcEQedFMNt8t/ueu88VMY+qQZCAJ/fv5GuTpX/baSuXebrYAYNQIU8miRj5oZDDBNEkmqmUO7FX5o9pofnvrkXgcG0L65kGDrgWx3C9/VsnR1qxk8CXdcx+OAmCHUqy7dbvkfa6X8PdTPqPDindjoFq/V5gurjZbPRZD+1853NEERpI2Wz2vzhdVGt3iNF+9Ebt56+5Uk8n4iKPsI/h/OR5lIE1hUdGJi5xwjrlTnkqzRDVFIGf0hqsGO4TCR/RGRYB5NF0oue9oLVxzBtX4wI8kxtGGxkh4r4mYvIo7UMEn2gxnBNQdg2Ucl8ewbMyARoJsrvvr1Ebp5hAgxDRWX3jqJE1kKHw3DEAmIjwoAy45EeVydC4meV3zx/SM8aXukCWFTAHj/bB2nawXkcT8LagCJsjtoUxJ4oZKCwNQAkmHLGy9IuEkbBDUEHdA3DYBJvwhmbGysr4wFSQDRDH9s5Yg7DEQ1LJQcvvz5MW7/8hj18oDAp52A828u4dMPXsZ2L0D2FNRL9SIK7t/T5w6iTki8drI0UK8BXg1ZlmIpS3dLywCoGeZLDq+eLqPdDnDyDEA/6oGpm6iBrtd9eijnOiipMVqJuaLrFckeADKhr04EsHcD4WCR41M2fC9HaOYzF+FzAM8BCDk75yQgZugIZxE5YIYOG+sr94pOzvWCGiekxHam22bb40nb7zacqIZayeHFanqoI7wBOueE/aD3ncE2io6vd70pJ+RjOB+WshSn5gu7LXY4J3w83Lg0Mys6Si/YxlSHUrN/jllifIecdCiVxat3H6rhZiVLBTB/aPGMrMOL2XwlS0UNNxev3n0484vJ7K9mM7+czvp6/jc2/egqvaJ1cAAAAABJRU5ErkJggg==")
APPLE_TOUCH_ICON_PNG_180 = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAYAAAA9zQYyAAATRklEQVR42u2dW2xd1ZnH/9+31t7n4uNLAhMlVYdOhxIgZpiReglOQg5RS8tLmUrluJ1pp0+j8hCRAGl4mLY6sdp5aaA0jVKJ0Tx1ytD6tGqLNBKF0tQksZMC0gyDuSOmvAQlEHw7F++91vrm4djBMUlIHPvYJ3w/xQLJUpyzz+/89399e+9lwgpCAMJgiTF6gmhgyJ3xPQG9e6DvqijgahD1ArhOBFcBWBeCrANRDOBKKAvhbYgkzHQcwHEivAngJYiMpozXV20feZMIcsb7US5a9K4R9FcC4czvLSe0IkQug4Eiz5VYAJp+qHitpP7mIGFjAD4lgo8zU1fWMoiAIIAPAh8EIoALomouAMsEIsAwwTCBCRABGi4gBJkgwhsMPMPExygyhzJ3Dr08V2IpFy0wFGgA4UMttAyWDEY3CA0MhNkDU1/jN4vI7T7IrSD0duYsQ4DUBUw7aUor4gGACBAQQUAgzPxRFnBmFAgAghBEZFZVImOZkLGEyDJAwGTdBQhGDdMTRPRo7oQ5MhtEUi4zel8g6q/4D5XQMlgyc09VjQNbrxf4rwTIHZa5N7aMJA2opwECmUlt4hmHVdpWy978T2gKQzYXMeKIkbgAF8Iog35JML/Ibn/qxbnVcTnEpuUUub5/863C2B5EbuvI2sx0ElBLghCJB4hn5FWBV5jjMiO4CJl8zJSJGdWGm2aixyjgQO6uI08sl9gtkUXKYPSWTp+Kqvs3fRlMuyxTX2QIk3WPAHEzErM601YJHgAJDLKdOYPUC1yQEQR5oOOu4V+9Vy0r0oqOveRCHywX7baZjjX54y2fYyN7cpY3ewGmpl2AQIiadUL1aPPkFgkgUCFj2RBQd+FI8LSnc8fh3893oe2EljIYeyBEkPEfbbwmjqLvM6OfiTDVcF4A4qbIymVGEAkESCFrTRBBCBhM0vQ73Xcfe1UEhD2gpUrrJRHqYLloaQCBCJjav/lbUWSfycbcX0t8mGy4QERGZb58YSImIjPZcKGW+JCNuT+K7DNT+zd/iwigAYSD5aJd8QktAP2xXDTbBobc+P1b1tus/Fs+a4qTNQ8Xgmcio2/3hzKxvWU2nXmDWsMPuQZ9s/tbh185WC7aWwaG/GJemFk0oedWjMl9m75mLR2ILXdP1J0DyBBpR/6Qj0YEEN+VszZxYdw52d65c/jhxa4giyKZDJYM9Vf8wXLRfvov0h/lY7O9Oh2Qek1l5f1pHRk2HRlGLfEHnj4Z3b1tYMjNOrTsQs/+Q47v7VvTleOf53Nm29hk2pwjayor507r0NMZmVrdH5yoh6+u2z1yYjGkvqSFmZSLlvor/uT9fdd153konzHbxiZdSqQVQzlPihKIiMzYpEvzGbOtO89DJ+/vu476K14ucbG4YOlmP01v/WDjjV0d9nex4bUTDeeYyOpbplxEBXFdWWsTH96aqLovrL3v2HOXktQLSujTybx34y3dhegQEa2daDivMisXLSCRnWg4T0RruwvRoZN7N95yKUl90Qk9++l5Z2/fDdk8jzCo0Ei9J138KZfUq8VnI2MCZKpRC31X7B55fiFJfVEJLaXmDzhx/5b1+Tw/zqBCXWVWFqVXk6mn3jOokM/z4yfu37Ke+iteSqWLcuuCE1rKYAxAjv/rliu7u+VQbPnayWbNUJmVxezUvjNrTeLCy+PjdPO6bx9+G+ULn1NfkNAiIFSaj0ZNrE6f7Mrbre9WU2eIrD4joixqUgPwIm5VR2Qnau6prlPRZ9G7RlCqhPmPgS28cuwpGuqv+LGedF9XIdo6VnUpq8zKUnTpmYXiWNWlXYVo61hPuo/6Kx57iuZCPxAXtgh8sO8fVndE/zleTx2g0wylJXq77lxkT1XTf7zinpFHLmSRSB/Um2kAYWzfpqsji/8WoXzihFgvmigt6dOQ2JIQSS11+LuencOvzzq5kMpB6C2RCEgEP81aU0hcEJVZaRVMoMQFyVpTEMFPRZpOni+I+TxVg6m/4k/t67u3pzPaNF53TicaSuulJjNed66nM9p0al/fvdRf8TJY4ouqHLO3go7tv/mvYhOekyC51ENvNlKWp0kLJDIIxFRPPN/Yc9eh/zvXLadnN723RESQ4N0D+dgUEi9QmZXlggiUeEE+NoXg3QNEkJnq8cEJPbuSPLXvpmIhY/9Ym/Ye0KqhrIis9vmMMVPT7pbVO48OnW3q8f6EHt0gIiAE3ksgiA6blZVTPZpbZAXeKwLC6AY5b0LPGj+xb9PfF3LmN+M1p/dpKCtMavHdeWum6v5LXTuHfzs/pXl+OgNAGuS7PkCzWVmR+ABJg3x3rrPvS+jZbbrGfti3rSNnnqwmPhB0qwFlBaY0JHTEhqt1/9mee0cOzt1u7D1hKwABAkP3WMMAln9rVEU5B8EaBgzdQ4CgMi+hT1/ifuCmT5iYR0NAJOeYgijKSghpAsCM1Ceht2fX0ddmHZ5J6CIDgFj6eiFn4wDxKrOygqEA8YWcjcXS1+c6TDIr7kOftO/W4+fzsVlfT0PQXUCVld2jEXIRcy3xr6zKJTfgzmdds0MPlpgAmZzO3NSRMevrqVeZlZUf0QDXUx86Mmb95HTmJgIEgyVmjJ6YqRbypThi0cWg0k6Lw6az8iUAwOgJYgwMeZEyO4/bkjQQdFSntE9Oc5IGch63iZQZzY0fgfEHN13LFi/4cHohqAtCpU2qNGAYEhw2dN8z/HJz4GywtZC1LDrdUNosogXiC1nLwWBrc1HYnIHcpMdGaWuzZxxmERBIPp26AO3PSjv26NQFgOTTIiB65/6b/5Kte8EaKszckKSVQ2mrHm0Y5LxMBWc3MLO7JrJccF5lVtozop2HRJYLzO4aK5DerGWkzoV2eTIliOiDBy3AcLvkm4SsNSZJfK8lpuupjXKZAHRm7Mzv+VaWknriEdrkIBMBxHS9heBjIbTPPzr1gqHXxuF8gD63u7Tc+NEOdMQGXmTFH+kQAAg+ZoloXRABaGXntAhgmTA27fDP//EK3qmmiIw+87iUPLnzRmxYl0ctFaxoO4goiICI1lkQ1rgggLRH4BGAfMxopKxCt+CM2B4VGuSCAIQ1FkCm3e5GCvLelwqtAKfvqMswgCu9biSjtPeZhLwXALhSrwwqlxUqtKJCK4oKrSgqtKKo0IoKrSgqtKKo0IqiQiuKCq2o0IqiQiuKCq0oKrSiqNCKCq0oKrSiqNCKsrjYtvwU0ntfrX5GttUbr7A+6Xl5Cy0AaklANQmt3cZAmo/1Z6PWntRqSWi+xmUQux2fqG8boYkAL4KOjMG//9P6lu6cFATIWsKfT03jX377Rktf9w/vuBofW51Bw0nL03pdT4zEC9ppq7i2SmgRIGJC8RPdLd3bLggQR4xXjldb/pr7/roT69d1IElDy4Vu7m0nbbW/RVtWjslp19LTYRAg7xhT077lr3dq2qNWd6gtg9CmDQt8my4KqaWdkqT55vIynHuZCIZp5ufrou8Dj5ceAkWFVhQVWlFUaEVRoRUVWlFUaEVRoRVFhVYUFVpRoRVFhVYUFVpRVGhFUaEVFVpRVGhFUaEVRYVWFBVaUaEVRYVWFBVaUVRoRVGhFRVaUVRoRVGhFUWFVhQVWlGhFUWFVhQVWlFUaEVRoRUVWlFUaEVRoRVFhVZUaEVRoRVFhVYUFVpRVGhFhVYUFVpRVGhFUaEVRYVWVGhFUaEVRYVWlCUR+m1jCCIQPRxKOyICMYYA4G0GMK0xrVwmVWOaIThhmQDShFbaFIJYJkBwgkXkOBMBIiq00q6dQ5gIInKcQfgza+dQ2r1yMADCn1mCvKjZrFwGC0NIkBctgUYbLgAgzelzffoJmPYBH12VwX9tv6GlP/ujqzKY9gFM+j6cp0RzwwUQaNSGYF9NnZuyhgo+QADooTtHAsSWcP3afEt/7rQP0DPo+d8aa0CpC1Mh2FdJBDT+477/ycfmb6pJCKQXW85LaLFcmswfYDMQOmLmWuL/t3vHyN8yEQRCT0eWAUjQQ/TBgrXyS/lApUNkGRB6mgjCACCMo3pglLbWesZhBgD2eGqq4QKBTDPFFaU9PCaQmWq4wB5PAQALQF133/pq6uSlXMwkKrTSPv1ZcjFT6uSlrrtvfVUAYpSLhmggWIPH4ohFe7TSTv05jliswWNEAwHlomH0rplJZPpNkgaCTjmUNlqjN52l3wAAetcIyezc+aFP2nfr8fP52Kyvpzq+U1Z83Qi5iLmW+FdW5ZIbcOezDmhOhgTloqE7n02J8Egm1vGd0h51IxMziPAI3flsinLREGbGdsBQAABy8rOpuktYpx3KCreZQWaq7hJy8rO5DjMA0ACClEqmZ9fR11zA4505SwLxetyUlVk3xHfmLLmAx3t2HX1NSiVDA3hPaABACRCA4OVB5+d9T1FW2GLQ+QB4eVAAQmnON2b/h/orHuUyrbp35A+TDf9sIWNJRFNaWWHpLOILGUuTDf/sqntH/oBymai/4t8ndHPs8QIBQMT0PcN6152yMjEMipi+N9fZswpN/RUv5TJ37hh+dKzqny5krdGUVlZUOmetGav6pzt3DD8q5TLPTeez9+TeF4gIAg67BQLSnFZWCESAQAAOu4kg89P5rEJTf8XLYMms3nl0aLLuf92dj0zQlFaWmSDiu/ORmaz7X6/eeXRIBktmfjqfe5IxWhEREBu7q5b4qVg3olGWtWpAYkOoJX6Kjd0lAsJo5aw+nlVoGkBApcSrdhx6o5GGPR35yOhcWlk2oSG+Ix+ZRhr2rNpx6A1USjw7d36fu+erLDJYYpQqYWzfpsNdObNpvO48Exk9xEpLq0bOmom6H+7ZObwFlRJTfyXgHFey+bwfjNGKEEGI8I2G81OxZQpaPZSWyQyJLVPD+SkifIOo6STOc1vGea8G0gCCDJZMz87h1xuJfLMjNsyk1UNpDUziO2LDjUS+2bNz+HUZfO8S94KEPj31KBftFfeMPDJWdT/pLsRWBKkebmWJF4JpdyG2Y1X3kyvuGXlEykV7tqnGxXTouX85oVJijJ6gidXpk115u/XdauoMkdX+oSwmBMCLuFUdkZ2ouae6TkWfRe8aQakS6AI2FL2gG5BOd5eBIV+d4jtq0/7lzqy1XufTyiLjRXxn1tratH+5OsV3YGDIz67lLqimXPAnZwABpRJ/5NuHT1YbdLvz4XguMnrRRVnUiUYuMsb5cLzaoNs/8u3DJ1E694huwZXjjPoxc4Xmnb19N2TzPMKgQiP1nnScp1xSZxafjYwJkKlGLfRdsXvk+XNdDVyUhH7fInH3yPO1mvsiCBPZWJNaubRkzsbGgDBRq7kvXrF75PkLXQReckLPT+q3frDxxq4O+7vY8NqJhnNMZPUtUi5CZteVtTbx4a2JqvvC2vuOPbeQZF5wQs9P6rX3HXuuWg/bEh9eWtUR6UhPuYiagXRVR2QTH16q1sO2tfcde26hyXzJCT0/qY/v7VvTleOf53Nm29hk6gFiIn1IQDmryAJI6OmMTK3uD07Uw1fX7R45cSnJfMkJfUZSD5bMut0jJ/500n6+WncHuvPWWEOkvVo5W1+2hqg7b0217g786aT9/GLJvCgJffpTVwZjD4QIMrlv09espQOx5e6JunMAGU1rTWVAfFfO2sSFcedke+fO4YdFQNgDupjR3JIm9OlPxgACCDhYLtrOncMPJ3X6TCMNQz2FyBrWtP6wp7Jhop5CZBtpGErq9JnOncMPHywXLWjGncXycClewMFy0W4bGHIAaGr/5l3M+G4uNl1jtTSIAEz6+1w+JCIHIqAnH3E98RMh4HuFu448AEDmOLKoLFkNmFtBxn+08Zo4ir7PjH4mwlTDeWmuGlXsy1VkQApZa4IIQsBgkqbf6b772KuLXTFaJvRZ0hqTP97yOTayJ2d5sxdgatoFCISaYmvHbveaLBJAoELGsiGg7sKR4GlP547Dv5/vwlLREomkDEZv6fSGINX9m74Mpl2WqS8yhMm6R4A4gFh3PW0zi4EASGCQ7cwZpF7ggowgyAMddw3/CmiOdjFakaVK5ZYLffrFD5YM+iuBZp44qO/ffKswtgeR2zqyNjOdBNSSIETiZ+QmTe6VOLBozpFFyORjpkzMqDbcNBM9RgEHcncdeWJGdsJgiRdjHLcihT6X2I0DW68X+K8EyB2WuTe2jCQNqKcBApk5RTVrCangrU7gmUeemlssE8jmIkYcMRIX4EIYZdAvCeYX2e1PvbhcIi+r0GeIPbpBaGAgNKtJ0dbX+M0icrsPcisIvZ05yxAgdQHTTuCCADMjwObGI0QQEJqmq+wLlbb5K1eFIHL6F30SGcuEjCVElgECJusuQDBqmJ4gokdzJ8wRmunFUi4zel+g5RB5RQh9RsdGkWnOgkEAmn6oeK2k/uYgYWMAPiWCjzNTV9YyiJq/BNMHgQ8CETRlVy4aywQiwDDBMDV3wReg4QJCkAkivMHAM0x8jCJzKHPn0Ms050FVKRctMBRa0ZHbQui5EmOw+agXzVsNi4DePdB3VRRwNYh6AVwngqsArAtB1oEoBnCl6rkg3oZIwkzHARwnwpsAXoLIaMp4fdX2kTfnPzEi5aJF7xqZWx1XAv8PM1dM8Gu7iXEAAAAASUVORK5CYII=")
ICON_ROUTES = {
    "/favicon.svg": (FAVICON_SVG, "image/svg+xml"),
    "/favicon.ico": (FAVICON_PNG_32, "image/png"),
    "/favicon-32.png": (FAVICON_PNG_32, "image/png"),
    "/apple-touch-icon.png": (APPLE_TOUCH_ICON_PNG_180, "image/png"),
}


class HealthCheckHandler(BaseHTTPRequestHandler):
    def _client_ip(self):
        # Rightmost X-Forwarded-For entry is the one Render's proxy added (the leftmost can be spoofed).
        forwarded = self.headers.get("X-Forwarded-For", "")
        if forwarded:
            return forwarded.split(",")[-1].strip()
        return self.client_address[0]

    def _is_admin(self):
        raw_cookie = self.headers.get("Cookie", "")
        if not raw_cookie:
            return False
        try:
            morsel = SimpleCookie(raw_cookie).get(SUDO_COOKIE_NAME)
        except Exception:
            return False
        return bool(morsel) and verify_sudo_token(morsel.value)

    def _cookie_header(self, value, max_age):
        parts = [f"{SUDO_COOKIE_NAME}={value}", f"Max-Age={max_age}", "Path=/", "HttpOnly", "SameSite=Strict"]
        if self.headers.get("X-Forwarded-Proto", "").lower() == "https":
            parts.append("Secure")
        return "; ".join(parts)

    def _send_json(self, status, payload, extra_headers=None):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (extra_headers or []):
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _handle_login(self):
        if not ADMIN_PASSWORD:
            self._send_json(503, {"message": "Sudo mode isn't set up. Add the ADMIN_PASSWORD environment variable on Render."})
            return
        ip = self._client_ip()
        wait = sudo_lockout_remaining(ip)
        if wait:
            self._send_json(429, {"message": f"Too many wrong attempts. Try again in {math.ceil(wait / 60)} min."})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(min(length, 4096)).decode("utf-8"))
            supplied = str(data.get("password", "")).strip()
        except Exception:
            supplied = ""
        if hmac.compare_digest(supplied.encode("utf-8"), ADMIN_PASSWORD.encode("utf-8")):
            sudo_clear_failures(ip)
            self._send_json(200, {"message": "Sudo mode on."},
                            [("Set-Cookie", self._cookie_header(make_sudo_token(), SUDO_SESSION_SECONDS))])
        else:
            sudo_register_failure(ip)
            print("🔒 Failed sudo login attempt", flush=True)   # stdout only: /logs is public, so no IPs there
            self._send_json(401, {"message": "Wrong password."})

    def _handle_logout(self):
        self._send_json(200, {"message": "Logged out."}, [("Set-Cookie", self._cookie_header("", 0))])

    def _handle_channels_api(self, route):
        """GET/POST/DELETE for the saved-channel list (POST/DELETE are sudo-guarded in do_GET)."""
        if self.command == "GET":
            self._send_json(200, {"channels": list_saved_channels()})
            return

        if self.command == "DELETE":
            channel_id = urllib.parse.unquote(route.rsplit("/", 1)[-1])
            existing = get_saved_channel(channel_id)
            if existing is None:
                self._send_json(404, {"message": "That channel isn't in the saved list.", "channels": list_saved_channels()})
                return
            delete_saved_channel(channel_id)
            label = channel_display_name(channel_id, existing)
            add_log(f"Saved channel '{label}' ({channel_id}) deleted.")
            self._send_json(200, {
                "message": f"Deleted '{label}'. Streams already using it keep their channel ID.",
                "channels": list_saved_channels(),
            })
            return

        if self.command != "POST":
            self._send_json(405, {"message": "Method not allowed."})
            return

        try:
            length = min(int(self.headers.get('Content-Length', 0)), 8192)
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(body, dict):
                raise ValueError("body must be an object")
        except Exception:
            self._send_json(400, {"message": "Bad request."})
            return

        editing_id = str(body.get("id") or "").strip()
        name = str(body.get("name") or "").strip()[:60]
        input_text = str(body.get("input") or "").strip()[:300]

        if not input_text:
            self._send_json(400, {"message": "Enter a handle or a channel ID."})
            return

        old = None
        if editing_id:
            old = get_saved_channel(editing_id)
            if old is None:
                self._send_json(404, {"message": "That channel no longer exists. Close this and try again.", "channels": list_saved_channels()})
                return

        # Edit with the ID/handle field left as it was: just rename, no new lookup.
        unchanged = False
        if old is not None:
            unchanged = input_text == editing_id or (
                bool(old["handle"]) and input_text.lstrip("@").lower() == old["handle"].lstrip("@").lower()
            )

        if unchanged:
            channel_id, handle, fetched_name = editing_id, old["handle"], ""
        else:
            resolved, error = resolve_channel_input(input_text)
            if error:
                self._send_json(400, {"message": error})
                return
            channel_id, handle, fetched_name = resolved["channel_id"], resolved["handle"], resolved.get("name", "")

        if channel_id != editing_id:
            duplicate = get_saved_channel(channel_id)
            if duplicate is not None:
                self._send_json(409, {
                    "message": f"That channel is already saved as '{channel_display_name(channel_id, duplicate)}'.",
                    "channels": list_saved_channels(),
                })
                return

        if not editing_id and not name:
            name = fetched_name[:60]

        upsert_saved_channel(channel_id, name, handle, replace_id=editing_id or None)
        saved = {"id": channel_id, "name": name, "handle": handle}
        label = channel_display_name(channel_id, saved)
        add_log(f"Saved channel '{label}' ({channel_id}) {'updated' if editing_id else 'added'}.")
        self._send_json(200, {
            "message": f"Saved '{label}'.",
            "channel": saved,
            "channels": list_saved_channels(),
        })

    def _handle_bulk_delete(self, route):
        """POST /api/streams/delete-selected and /api/streams/delete-stopped (sudo-guarded in do_GET)."""
        try:
            length = int(self.headers.get('Content-Length', 0))
            raw = self.rfile.read(length).decode('utf-8') if length else ''
            body = json.loads(raw) if raw.strip() else {}
        except (ValueError, UnicodeDecodeError):
            self._send_json(400, {"message": "Invalid request."})
            return

        if route == "/api/streams/delete-stopped":
            result = delete_streams_bulk(only_stopped=True)
            if result["deleted"] == 0:
                message = "No stopped streams to delete."
            else:
                message = f"Deleted {result['deleted']} stopped stream(s)."
        else:
            ids = body.get("video_ids") if isinstance(body, dict) else None
            if (not isinstance(ids, list) or not ids or len(ids) > BULK_DELETE_MAX
                    or any(not isinstance(v, str) or not v for v in ids)):
                self._send_json(400, {"message": "Select at least one stream first."})
                return
            result = delete_streams_bulk(video_ids=list(dict.fromkeys(ids)))
            message = f"Deleted {result['deleted']} stream(s)."
            if result["stopped_running"]:
                message += f" {result['stopped_running']} were listening and have been stopped."
            if result["not_found"]:
                message += f" {result['not_found']} were not found."
        self._send_json(200, dict(result, message=message))

    # ---- v0.3.0.0 dashboard: shell, fragments, unmatched-route handling ----
    def _send_html(self, status, text):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_shell(self, status=200, legacy=None):
        with active_streams_lock:
            listening = len(active_stream_ids)
        users = _cached_user_count()
        boot = {
            "version": BOT_VERSION, "admin": self._is_admin(), "startedAt": BOT_STARTED_AT, "serverNow": time.time(),
            "commands": DASHBOARD_COMMANDS, "startCoins": DEFAULT_STARTING_COINS,
            "listening": listening, "users": users, "legacy": legacy, "notFound": status == 404,
        }
        data = json.dumps(boot).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
        self._send_html(status, DASHBOARD_SHELL_HTML.replace("__BOOT__", data))

    def _handle_unmatched(self, route):
        """Replaces the old catch-all 'Bot is active!'. GET / and the legacy page paths serve the dashboard shell (HTTP 200);
        unknown /api/ and /_view/ paths get a JSON 404; unknown pages get the shell with HTTP 404 so it can show the 404 view."""
        norm = route.rstrip("/") or "/"
        last = norm.rsplit("/", 1)[-1]
        if self.command == "GET" and norm == "/":
            self._send_shell(200)
        elif self.command == "GET" and norm.startswith("/") and norm[1:] in DASHBOARD_PAGES:
            self._send_shell(200, legacy=norm[1:])
        elif self.command != "GET" or norm == "/api" or norm.startswith("/api/") or norm.startswith("/_view/"):
            self._send_json(404, {"message": "Not found"})
        elif "." in last:
            body = b"Not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._send_shell(404)

    def _live_fragment(self):
        esc = lambda v: html_lib.escape(str(v), quote=True)

        def js_arg(v):   # value for a '...' JS string inside an onclick="..." attribute (JS-escape, then HTML-escape)
            s = (str(v).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
                 .replace("\u2028", "\\u2028").replace("\u2029", "\\u2029"))
            return html_lib.escape(s, quote=True)

        streams = load_streams()
        with active_streams_lock:
            running_ids = set(active_stream_ids)

        saved_by_id = {c["id"]: c for c in list_saved_channels()}
        channels_json = (
            json.dumps(list(saved_by_id.values()))
            .replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
        )

        stream_rows = ""
        for i, (video_id, data) in enumerate(streams.items()):
            raw_label = data.get('label', '') or ''
            label = raw_label or 'N/A'
            status = "🟢 Listening" if video_id in running_ids else "⚪ Stopped"
            label_btn_text = "✏️ Edit/Remove Label" if raw_label else "➕ Add Label"
            raw_channel_id = (data.get('channel_id', '') or '') if isinstance(data, dict) else ''
            if raw_channel_id:
                saved_channel = saved_by_id.get(raw_channel_id)
                channel_cell = html_lib.escape(channel_display_name(raw_channel_id, saved_channel))
                if saved_channel and (saved_channel["name"] or saved_channel["handle"]):
                    channel_cell += f'<br><code style="font-size: 11px; color: var(--muted);">{html_lib.escape(_short_channel_id(raw_channel_id))}</code>'
            else:
                channel_cell = '<span style="color: var(--muted);">Automatic</span>'
            vid = esc(video_id)
            stream_rows += f"""
                <tr role="row">
                    <td role="cell"><label class="cell-label ctl" for="sel-{i}">Select<span class="vh"> {vid}</span></label><input type="checkbox" id="sel-{i}" class="stream-check" autocomplete="off" data-id="{vid}" data-running="{1 if video_id in running_ids else 0}"></td>
                    <td role="cell"><span class="cell-label" aria-hidden="true">Video ID</span><div class="cv"><code>{vid}</code></div></td>
                    <td role="cell"><span class="cell-label" aria-hidden="true">Label</span><div class="cv">{esc(label)}</div></td>
                    <td role="cell"><span class="cell-label" aria-hidden="true">Channel</span><div class="cv">{channel_cell}</div></td>
                    <td role="cell"><span class="cell-label" aria-hidden="true">Status</span><div class="cv">{status}</div></td>
                    <td role="cell"><span class="cell-label" aria-hidden="true">Actions</span><div class="row btns">
                        <button onclick="editLabel('{js_arg(video_id)}', '{js_arg(raw_label)}')">{label_btn_text}</button>
                        <button onclick="changeChannel('{js_arg(video_id)}', '{js_arg(raw_channel_id)}', this)" aria-haspopup="dialog" aria-expanded="false">📺 Change Channel</button>
                        <button onclick="removeStream('{js_arg(video_id)}')">Remove</button>
                    </div></td>
                </tr>
                """

        stopped_count = sum(1 for vid in streams if vid not in running_ids)
        stopped_disabled = "" if stopped_count else " disabled"

        return f"""
<div class="frag frag-live"><h1>📡 Live Streams</h1><p class="sub">Manage the streams the bot listens to.</p><div class="lockbar" id="lockbar"></div>
                <div class="container">
                    <div class="backup-box">
                        <h3 style="color: var(--warn); margin-top: 0;">💾 Backup / Restore</h3>
                        <p style="font-size: 13px; color: var(--muted); margin-top: 0;">
                            Streams don't survive a redeploy on Render's free tier (fresh container each time).
                            Copy your current list before redeploying, then paste it back in after — no retyping.
                        </p>
                        <button class="copy-btn" onclick="copyBackup()">📋 Copy Current Streams</button>
                        <div style="margin-top: 15px;">
                            <textarea id="restoreInput" placeholder='Paste backup JSON here, e.g. {{"VIDEO_ID": {{"label": "Owner"}}}}'></textarea>
                            <button class="restore-btn" onclick="restoreBackup()">🔄 Restore These Streams</button>
                        </div>
                    </div>

                    <div class="add-box">
                        <h3 style="color: var(--accent); margin-top: 0;">➕ Add Live Stream</h3>
                        <p style="font-size: 13px; color: var(--muted); margin-top: 0;">
                            Paste a YouTube video ID (livestream or premiere) to start listening immediately.
                            Supports multiple streams at once.
                        </p>
                        <input type="text" id="videoIdInput" placeholder="Video ID (e.g. BblhFWoDyWk)" style="width: 220px;">
                        <input type="text" id="labelInput" placeholder="Label (optional)" style="width: 180px;">
                        <button type="button" id="channelPickerBtn" class="picker-btn" aria-haspopup="dialog" aria-expanded="false"><span class="picker-label">Select Channel (optional)</span><span class="caret" aria-hidden="true">▾</span></button>
                        <button class="add-btn" onclick="addStream()">▶ Start Listening</button>
                        <p style="font-size: 12px; color: var(--muted); margin: 6px 0 0 0;">
                            Selecting a channel is optional and a last resort: only pick one if pytchat can't find the
                            channel on its own (for example, the creator turned off embedding and the lookups fail from
                            the cloud server). Pick the stream creator's channel, not your bot's. New channel? Open the
                            dropdown and tap Add Channel ID.
                        </p>
                    </div>

                    <h2>📺 Active Streams</h2>
                    <div class="bulk-bar">
                        <button type="button" id="deleteSelectedBtn" class="danger-btn" onclick="deleteSelectedStreams()" disabled>🗑️ Delete Selected (0)</button>
                        <button type="button" id="deleteStoppedBtn" class="danger-btn" onclick="deleteStoppedStreams()"{stopped_disabled}>🧹 Delete Stopped Streams ({stopped_count})</button>
                    </div>
                    <div class="tablewrap"><table class="stk stk-live" role="table" aria-label="Streams">
                        <thead role="rowgroup"><tr role="row">
                            <th role="columnheader" scope="col"><input type="checkbox" id="selectAllStreams" autocomplete="off" onchange="toggleAllStreams(this)" aria-label="Select all streams"><label class="sa-text" for="selectAllStreams">Select all</label></th>
                            <th role="columnheader" scope="col">Video ID</th>
                            <th role="columnheader" scope="col">Label</th>
                            <th role="columnheader" scope="col">Channel</th>
                            <th role="columnheader" scope="col">Status</th>
                            <th role="columnheader" scope="col"><span class="vh">Actions</span></th>
                        </tr></thead>
                        <tbody role="rowgroup">{stream_rows or "<tr role='row'><td role='cell' colspan='6'>No streams added yet.</td></tr>"}</tbody>
                    </table></div>
                </div>

                <script>
                    function addStream() {{
                        const videoId = document.getElementById('videoIdInput').value.trim();
                        const label = document.getElementById('labelInput').value.trim();
                        const channelId = window.getSelectedChannelId ? window.getSelectedChannelId() : '';

                        if (!videoId) {{
                            alert('Please enter a Video ID!');
                            return;
                        }}

                        if (channelId && !isValidChannelId(channelId)) {{
                            alert(CHANNEL_ID_HELP);
                            return;
                        }}

                        const body = {{video_id: videoId, label: label}};
                        if (channelId) body.channel_id = channelId;  // omitted = leave any saved one untouched

                        fetch('/api/streams', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify(body)
                        }}).then(r => r.json()).then(data => {{
                            alert(data.message);
                            refreshLive();
                        }}).catch(e => alert('Error: ' + e));
                    }}

                    const CHANNEL_ID_HELP = 'That is not a valid channel ID. It should start with UC and be 24 characters long.';
                    function isValidChannelId(value) {{
                        return /^UC[0-9A-Za-z_-]{{22}}$/.test(value);
                    }}

                    function refreshLive() {{
                        if (window.__refreshView) window.__refreshView('live'); else location.reload();
                    }}

                    function editLabel(videoId, currentLabel) {{
                        const result = prompt('Label for this stream (leave blank to remove):', currentLabel);
                        if (result === null) return;  // Cancelled

                        const newLabel = result.trim();
                        fetch('/api/streams', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{video_id: videoId, label: newLabel}})
                        }}).then(r => r.json()).then(data => {{
                            refreshLive();
                        }}).catch(e => alert('Error: ' + e));
                    }}

                    const BULK_FOOTER = "\\n\\nThis can't be undone. Use Copy Current Streams first if unsure.";

                    function streamChecks() {{
                        return Array.from(document.querySelectorAll('.stream-check'));
                    }}

                    function updateBulkUI() {{
                        const checks = streamChecks();
                        const n = checks.filter(c => c.checked).length;
                        const btn = document.getElementById('deleteSelectedBtn');
                        btn.disabled = (n === 0);
                        btn.textContent = '🗑️ Delete Selected (' + n + ')';
                        const all = document.getElementById('selectAllStreams');
                        all.checked = checks.length > 0 && n === checks.length;
                        all.indeterminate = n > 0 && n < checks.length;
                    }}

                    function toggleAllStreams(src) {{
                        streamChecks().forEach(c => {{ c.checked = src.checked; }});
                        updateBulkUI();
                    }}

                    document.addEventListener('change', function (e) {{
                        if (e.target.classList && e.target.classList.contains('stream-check')) updateBulkUI();
                    }});

                    function bulkDeleteRequest(url, payload) {{
                        fetch(url, {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify(payload)
                        }}).then(r => r.json())
                          .then(data => {{
                              alert(data.message);
                              refreshLive();
                          }})
                          .catch(e => alert('Error: ' + e));
                    }}

                    function deleteSelectedStreams() {{
                        const checked = streamChecks().filter(c => c.checked);
                        if (checked.length === 0) return;
                        const running = checked.filter(c => c.dataset.running === '1').length;
                        let msg = `Delete ${{checked.length}} stream(s)?`;
                        if (running > 0) msg += `\\n${{running}} currently listening and will be stopped.`;
                        if (!confirm(msg + BULK_FOOTER)) return;
                        bulkDeleteRequest('/api/streams/delete-selected', {{video_ids: checked.map(c => c.dataset.id)}});
                    }}

                    function deleteStoppedStreams() {{
                        if (!confirm(`Delete all stopped stream(s)? (${{{stopped_count}}} shown as stopped)` + BULK_FOOTER)) return;
                        bulkDeleteRequest('/api/streams/delete-stopped', {{}});
                    }}

                    function removeStream(videoId) {{
                        if (!confirm('Remove and stop listening to ' + videoId + '?')) return;
                        fetch('/api/streams/' + videoId, {{method: 'DELETE'}})
                            .then(r => r.json())
                            .then(data => {{
                                alert(data.message);
                                refreshLive();
                            }})
                            .catch(e => alert('Error: ' + e));
                    }}

                    function copyBackup() {{
                        fetch('/api/streams')
                            .then(r => r.json())
                            .then(data => {{
                                const text = JSON.stringify(data, null, 2);
                                navigator.clipboard.writeText(text).then(() => {{
                                    alert('Copied! Paste it somewhere safe before you redeploy.');
                                }}).catch(() => {{
                                    // Clipboard API can fail on some mobile browsers — fall back to showing it
                                    document.getElementById('restoreInput').value = text;
                                    alert('Could not auto-copy — the JSON is now in the textarea below, copy it manually.');
                                }});
                            }})
                            .catch(e => alert('Error: ' + e));
                    }}

                    function restoreBackup() {{
                        const raw = document.getElementById('restoreInput').value.trim();
                        if (!raw) {{
                            alert('Paste your backup JSON first!');
                            return;
                        }}

                        let parsed;
                        try {{
                            parsed = JSON.parse(raw);
                        }} catch (e) {{
                            alert('That doesn\\'t look like valid JSON. Paste exactly what Copy Current Streams gave you.');
                            return;
                        }}

                        const videoIds = Object.keys(parsed);
                        if (videoIds.length === 0) {{
                            alert('No streams found in that JSON.');
                            return;
                        }}

                        let completed = 0;
                        videoIds.forEach(videoId => {{
                            const label = (parsed[videoId] && parsed[videoId].label) || '';
                            const channelId = (parsed[videoId] && parsed[videoId].channel_id) || '';
                            const restoreBody = {{video_id: videoId, label: label}};
                            if (channelId) restoreBody.channel_id = channelId;
                            fetch('/api/streams', {{
                                method: 'POST',
                                headers: {{'Content-Type': 'application/json'}},
                                body: JSON.stringify(restoreBody)
                            }}).finally(() => {{
                                completed++;
                                if (completed === videoIds.length) {{
                                    alert(`Restored ${{videoIds.length}} stream(s)!`);
                                    refreshLive();
                                }}
                            }});
                        }});
                    }}
                </script>
                <script>window.__SAVED_CHANNELS__ = {channels_json};</script>
{CHANNEL_UI_HTML}
                <script>{CHANNEL_UI_JS}</script>
</div>
"""

    def _moderation_fragment(self):
        esc = lambda v: html_lib.escape(str(v), quote=True)

        def js_arg(v):
            s = (str(v).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
                 .replace("\u2028", "\\u2028").replace("\u2029", "\\u2029"))
            return html_lib.escape(s, quote=True)

        config = load_moderation()
        streams = load_streams()
        with active_streams_lock:
            running_ids = set(active_stream_ids)

        all_streams_settings = config.get("all_streams", _default_stream_moderation())

        stream_rows = ""
        i = 0
        for video_id, data in streams.items():
            if video_id not in running_ids:
                continue  # Only currently-listening streams can be moderated
            label = data.get('label') or video_id
            settings = config.get("streams", {}).get(video_id, _default_stream_moderation())
            checked = "checked" if settings.get("enabled") else ""
            vid = esc(video_id)
            stream_rows += f"""
                <tr role="row">
                    <td role="cell"><span class="cell-label" aria-hidden="true">Stream</span><div class="cv">{esc(label)}<br><code style="font-size:11px;color:var(--muted);">{vid}</code></div></td>
                    <td role="cell"><label class="cell-label ctl" for="md-en-{i}">Enabled<span class="vh"> {vid}</span></label><input type="checkbox" id="md-en-{i}" class="mod-enabled" data-video-id="{vid}" {checked}></td>
                    <td role="cell"><label class="cell-label ctl" for="md-th-{i}">Spam threshold (repeats in a row)<span class="vh"> {vid}</span></label><input type="number" id="md-th-{i}" class="mod-threshold" data-video-id="{vid}" value="{esc(settings.get('spam_threshold', DEFAULT_SPAM_THRESHOLD))}" min="1" style="width:60px;"></td>
                    <td role="cell"><label class="cell-label ctl" for="md-du-{i}">Timeout duration<span class="vh"> {vid}</span></label><input type="number" id="md-du-{i}" class="mod-duration" data-video-id="{vid}" value="{esc(settings.get('timeout_seconds', DEFAULT_TIMEOUT_SECONDS))}" min="1" style="width:80px;"> sec</td>
                    <td role="cell"><label class="cell-label ctl" for="md-tm-{i}">Timeout message<span class="vh"> {vid}</span></label><textarea id="md-tm-{i}" class="mod-timeout-msg" data-video-id="{vid}" style="min-height:40px;">{esc(settings.get('timeout_message', DEFAULT_TIMEOUT_MESSAGE))}</textarea></td>
                    <td role="cell"><label class="cell-label ctl" for="md-rm-{i}">Reminder message<span class="vh"> {vid}</span></label><textarea id="md-rm-{i}" class="mod-reminder-msg" data-video-id="{vid}" style="min-height:40px;">{esc(settings.get('reminder_message', DEFAULT_REMINDER_MESSAGE))}</textarea></td>
                    <td role="cell"><span class="cell-label" aria-hidden="true">Actions</span><div class="row btns"><button class="save-btn" onclick="saveStreamSettings('{js_arg(video_id)}')">Save</button></div></td>
                </tr>
                """
            i += 1

        blocklist_category_boxes = ""
        for cat_id, category in config.get("blocklist_categories", {}).items():
            words_text = ", ".join(category.get("words", []))
            blocklist_category_boxes += f"""
                <div class="box" data-category-id="{esc(cat_id)}">
                    <label>Category name</label>
                    <input type="text" class="cat-name" value="{esc(category.get('name', ''))}">
                    <label>Blocked words/phrases (comma-separated)</label>
                    <textarea class="cat-words">{esc(words_text)}</textarea>
                    <label>Timeout duration (seconds)</label>
                    <input type="number" class="cat-duration" value="{esc(category.get('timeout_seconds', DEFAULT_TIMEOUT_SECONDS))}" min="1">
                    <label>Timeout message (use {{userName}} and {{duration}})</label>
                    <textarea class="cat-timeout-msg">{esc(category.get('timeout_message', DEFAULT_BLOCKLIST_TIMEOUT_MESSAGE))}</textarea>
                    <label>Reminder message (use {{userName}})</label>
                    <textarea class="cat-reminder-msg">{esc(category.get('reminder_message', DEFAULT_BLOCKLIST_REMINDER_MESSAGE))}</textarea>
                    <button class="save-btn" onclick="saveBlocklistCategory('{js_arg(cat_id)}')">Save Category</button>
                    <button class="delete-btn" onclick="deleteBlocklistCategory('{js_arg(cat_id)}')">Delete Category</button>
                </div>
                """

        return f"""
<div class="frag frag-mod">
                <div class="container">
                    <h1>🛡️ Moderation Settings</h1><p class="sub">Spam and blocklist settings for the streams the bot is listening to.</p><div class="lockbar" id="lockbar"></div>

                    <div class="box">
                        <h3>🚫 Blocklist Categories (global — applies to all streams)</h3>
                        <p class="note">Each category has its own words, timeout duration, and messages. A message matching any word in any category triggers that category's reminder → timeout escalation.</p>
                        <div id="blocklistCategories">
                            {blocklist_category_boxes}
                        </div>
                        <button class="add-btn" onclick="addBlocklistCategory()">+ Add Blocklist</button>
                    </div>

                    <div class="box">
                        <h3>🌐 All Streams (default spam settings)</h3>
                        <p class="note">When enabled, applies to every listening stream that doesn't have its own per-stream override below.</p>
                        <label>Enabled</label>
                        <input type="checkbox" id="allStreamsEnabled" {"checked" if all_streams_settings.get("enabled") else ""}>
                        <label>Spam threshold (repeats in a row)</label>
                        <input type="number" id="allStreamsThreshold" value="{esc(all_streams_settings.get('spam_threshold', DEFAULT_SPAM_THRESHOLD))}" min="1">
                        <label>Timeout duration (seconds)</label>
                        <input type="number" id="allStreamsDuration" value="{esc(all_streams_settings.get('timeout_seconds', DEFAULT_TIMEOUT_SECONDS))}" min="1">
                        <label>Timeout message (use {{userName}} and {{duration}})</label>
                        <textarea id="allStreamsTimeoutMsg">{esc(all_streams_settings.get('timeout_message', DEFAULT_TIMEOUT_MESSAGE))}</textarea>
                        <label>Reminder message (use {{userName}})</label>
                        <textarea id="allStreamsReminderMsg">{esc(all_streams_settings.get('reminder_message', DEFAULT_REMINDER_MESSAGE))}</textarea>
                        <button class="save-btn" onclick="saveAllStreamsSettings()">Save All-Streams Settings</button>
                    </div>

                    <div class="box">
                        <h3>📺 Per-Stream Spam Overrides</h3>
                        <p class="note">Only currently-listening (🟢) streams are shown. A per-stream override, when enabled, takes priority over "All Streams" for that stream's spam detection.</p>
                        <div class="tablewrap"><table class="stk" role="table" aria-label="Per-stream spam settings">
                            <thead role="rowgroup"><tr role="row">
                                <th role="columnheader" scope="col">Stream</th>
                                <th role="columnheader" scope="col">Enabled</th>
                                <th role="columnheader" scope="col">Spam threshold</th>
                                <th role="columnheader" scope="col">Timeout duration</th>
                                <th role="columnheader" scope="col">Timeout message</th>
                                <th role="columnheader" scope="col">Reminder message</th>
                                <th role="columnheader" scope="col"><span class="vh">Actions</span></th>
                            </tr></thead>
                            <tbody role="rowgroup">{stream_rows or "<tr role='row'><td role='cell' colspan='7'>No currently-listening streams.</td></tr>"}</tbody>
                        </table></div>
                    </div>
                </div>

                <script>
                    function addBlocklistCategory() {{
                        fetch('/api/moderation/blocklist-category', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{action: 'create'}})
                        }}).then(r => r.json()).then(data => {{
                            alert(data.message);
                            refreshModeration();
                        }});
                    }}

                    function refreshModeration() {{
                        if (window.__refreshView) window.__refreshView('moderation'); else location.reload();
                    }}

                    function saveBlocklistCategory(catId) {{
                        const box = document.querySelector(`[data-category-id="${{catId}}"]`);
                        const wordsRaw = box.querySelector('.cat-words').value;
                        const words = wordsRaw.split(',').map(w => w.trim()).filter(w => w.length > 0);
                        const body = {{
                            action: 'update',
                            category_id: catId,
                            name: box.querySelector('.cat-name').value,
                            words: words,
                            timeout_seconds: parseInt(box.querySelector('.cat-duration').value) || {DEFAULT_TIMEOUT_SECONDS},
                            timeout_message: box.querySelector('.cat-timeout-msg').value,
                            reminder_message: box.querySelector('.cat-reminder-msg').value
                        }};
                        fetch('/api/moderation/blocklist-category', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify(body)
                        }}).then(r => r.json()).then(data => alert(data.message));
                    }}

                    function deleteBlocklistCategory(catId) {{
                        if (!confirm('Delete this blocklist category?')) return;
                        fetch('/api/moderation/blocklist-category', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{action: 'delete', category_id: catId}})
                        }}).then(r => r.json()).then(data => {{
                            alert(data.message);
                            refreshModeration();
                        }});
                    }}

                    function saveAllStreamsSettings() {{
                        const body = {{
                            enabled: document.getElementById('allStreamsEnabled').checked,
                            spam_threshold: parseInt(document.getElementById('allStreamsThreshold').value) || {DEFAULT_SPAM_THRESHOLD},
                            timeout_seconds: parseInt(document.getElementById('allStreamsDuration').value) || {DEFAULT_TIMEOUT_SECONDS},
                            timeout_message: document.getElementById('allStreamsTimeoutMsg').value,
                            reminder_message: document.getElementById('allStreamsReminderMsg').value
                        }};
                        fetch('/api/moderation/all-streams', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify(body)
                        }}).then(r => r.json()).then(data => alert(data.message));
                    }}

                    function saveStreamSettings(videoId) {{
                        const row = document.querySelector(`[data-video-id="${{videoId}}"].mod-enabled`).closest('tr');
                        const body = {{
                            video_id: videoId,
                            enabled: row.querySelector('.mod-enabled').checked,
                            spam_threshold: parseInt(row.querySelector('.mod-threshold').value) || {DEFAULT_SPAM_THRESHOLD},
                            timeout_seconds: parseInt(row.querySelector('.mod-duration').value) || {DEFAULT_TIMEOUT_SECONDS},
                            timeout_message: row.querySelector('.mod-timeout-msg').value,
                            reminder_message: row.querySelector('.mod-reminder-msg').value
                        }};
                        fetch('/api/moderation/stream', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify(body)
                        }}).then(r => r.json()).then(data => alert(data.message));
                    }}
                </script>
</div>
"""

    def do_GET(self):
        route = self.path.split("?")[0]

        # Default-deny: every POST/DELETE (except login/logout) needs a valid sudo cookie.
        if self.command in ("POST", "DELETE") and route not in SUDO_AUTH_ROUTES and not self._is_admin():
            self._send_json(401, {"message": "🔒 Sudo mode required. Tap 'Sudo mode' at the top right and enter the password."})
            return

        if route == "/api/auth/login" and self.command == "POST":
            self._handle_login()

        elif route == "/api/auth/logout" and self.command == "POST":
            self._handle_logout()

        elif route == "/api/logs":
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:
                since_id = int(query.get("since", ["0"])[0])
            except ValueError:
                since_id = 0
            latest_id, entries = get_logs_since(since_id)
            body = json.dumps({"latest": latest_id, "entries": entries}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif route in ICON_ROUTES and self.command == "GET":
            body, ctype = ICON_ROUTES[route]
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif route == "/_view/live" and self.command == "GET":
            self._send_html(200, self._live_fragment())

        elif route == "/_view/moderation" and self.command == "GET":
            self._send_html(200, self._moderation_fragment())

        elif self.path.split("?")[0] == "/api/leaderboard":
            body = json.dumps({"rows": get_leaderboard_rows()}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif route == "/api/channels" or route.startswith("/api/channels/"):
            self._handle_channels_api(route)

        elif route in ("/api/streams/delete-selected", "/api/streams/delete-stopped") and self.command == "POST":
            self._handle_bulk_delete(route)

        elif self.path.startswith("/api/streams"):
            if self.command == "GET":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(load_streams()).encode("utf-8"))

            elif self.command == "POST":
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length)
                data = json.loads(body.decode('utf-8'))

                video_id = data.get('video_id', '').strip()
                # label / channel_id are each optional in the request. A field that
                # is ABSENT is left unchanged; a field that is present but blank
                # clears it. (Editing only the label must not wipe the channel ID,
                # and vice versa.)
                has_label_field = 'label' in data
                has_channel_field = 'channel_id' in data
                channel_id = (data.get('channel_id') or '').strip()

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()

                if not video_id:
                    self.wfile.write(json.dumps({"message": "Video ID is required!"}).encode("utf-8"))
                    return

                if channel_id and not pytchat_manual_channel_patch.is_valid_channel_id(channel_id):
                    self.wfile.write(json.dumps({
                        "message": "That is not a valid channel ID. It should start with UC and be 24 characters long. Nothing was saved."
                    }).encode("utf-8"))
                    return

                streams = load_streams()
                existing = streams.get(video_id)
                if not isinstance(existing, dict):
                    existing = {}
                old_channel_id = existing.get('channel_id', '')

                label = data.get('label', '').strip() if has_label_field else existing.get('label', '')
                record = dict(existing)  # keep any other saved fields
                record['label'] = label
                if has_channel_field:
                    if channel_id:
                        record['channel_id'] = channel_id
                    else:
                        record.pop('channel_id', None)
                streams[video_id] = record
                save_streams(streams)
                if has_channel_field and channel_id:
                    ensure_channel_saved(channel_id)  # so it shows up in the Select Channel dropdown

                # Keep the pytchat patch's last-resort lookup in sync with what was saved.
                pytchat_manual_channel_patch.set_manual_channel_id(video_id, record.get('channel_id'))

                channel_changed = has_channel_field and channel_id != old_channel_id
                old_label = existing.get('label', '')
                label_changed = has_label_field and label != old_label
                if is_stream_running(video_id):
                    # Already listening: never go through start_stream_listener (its "duplicate start" log
                    # is misleading for an edit). Just push the new label to the running thread.
                    started = False
                    if label_changed:
                        set_stream_label(video_id, label or video_id)
                        add_log(f"Renamed '{old_label or video_id}' → '{label or video_id}' ({video_id})")
                else:
                    started = start_stream_listener(video_id, label or video_id)
                if started:
                    self.wfile.write(json.dumps({
                        "message": f"Started listening to '{label or video_id}'! Check /logs to confirm connection."
                    }).encode("utf-8"))
                elif channel_changed and channel_id:
                    # Already running (likely in API-fallback mode) — ask it to retry pytchat now
                    # instead of waiting for the next scheduled retry.
                    request_pytchat_retry(video_id)
                    self.wfile.write(json.dumps({
                        "message": f"Channel ID saved for '{label or video_id}'. Retrying pytchat now — check /logs."
                    }).encode("utf-8"))
                elif channel_changed:
                    self.wfile.write(json.dumps({
                        "message": f"Channel ID removed for '{label or video_id}'."
                    }).encode("utf-8"))
                elif label_changed:
                    self.wfile.write(json.dumps({
                        "message": f"Label updated to '{label or video_id}'."
                    }).encode("utf-8"))
                else:
                    self.wfile.write(json.dumps({
                        "message": "Already listening to this video ID — saved, no duplicate listener started."
                    }).encode("utf-8"))

            elif self.command == "DELETE":
                video_id = self.path.split('/')[-1]

                streams = load_streams()
                if video_id in streams:
                    del streams[video_id]
                    save_streams(streams)

                stop_stream_listener(video_id)
                pytchat_manual_channel_patch.set_manual_channel_id(video_id, None)
                _clear_pytchat_retry_request(video_id)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "message": f"Stopped and removed {video_id}."
                }).encode("utf-8"))

        elif self.path == "/api/moderation/blocklist-category" and self.command == "POST":
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length).decode('utf-8'))
            action = body.get("action", "")

            config = load_moderation()
            config.setdefault("blocklist_categories", {})

            if action == "create":
                import uuid
                new_id = uuid.uuid4().hex[:8]
                config["blocklist_categories"][new_id] = {
                    "name": "New blocklist",
                    "words": [],
                    "timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
                    "timeout_message": DEFAULT_BLOCKLIST_TIMEOUT_MESSAGE,
                    "reminder_message": DEFAULT_BLOCKLIST_REMINDER_MESSAGE,
                }
                save_moderation(config)
                message = "New blocklist category created."

            elif action == "update":
                category_id = body.get("category_id", "")
                if category_id not in config["blocklist_categories"]:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"message": "Category not found."}).encode("utf-8"))
                    return
                config["blocklist_categories"][category_id] = {
                    "name": body.get("name", "Untitled"),
                    "words": body.get("words", []),
                    "timeout_seconds": int(body.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)),
                    "timeout_message": body.get("timeout_message", DEFAULT_BLOCKLIST_TIMEOUT_MESSAGE),
                    "reminder_message": body.get("reminder_message", DEFAULT_BLOCKLIST_REMINDER_MESSAGE),
                }
                save_moderation(config)
                message = f"Blocklist category '{config['blocklist_categories'][category_id]['name']}' saved."

            elif action == "delete":
                category_id = body.get("category_id", "")
                config["blocklist_categories"].pop(category_id, None)
                save_moderation(config)
                message = "Blocklist category deleted."

            else:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"message": "Unknown action."}).encode("utf-8"))
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"message": message}).encode("utf-8"))

        elif self.path == "/api/moderation/all-streams" and self.command == "POST":
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length).decode('utf-8'))

            config = load_moderation()
            existing = config.get("all_streams", _default_stream_moderation())
            config["all_streams"] = {
                "enabled": bool(body.get("enabled", False)),
                "spam_threshold": int(body.get("spam_threshold", DEFAULT_SPAM_THRESHOLD)),
                "timeout_seconds": int(body.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)),
                "timeout_message": body.get("timeout_message", DEFAULT_TIMEOUT_MESSAGE),
                "reminder_message": body.get("reminder_message", DEFAULT_REMINDER_MESSAGE),
            }
            save_moderation(config)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "message": "All-streams moderation settings saved."
            }).encode("utf-8"))

        elif self.path == "/api/moderation/stream" and self.command == "POST":
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length).decode('utf-8'))

            video_id = body.get("video_id", "").strip()
            if not video_id:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"message": "video_id is required."}).encode("utf-8"))
                return

            config = load_moderation()
            existing = config.get("streams", {}).get(video_id, _default_stream_moderation())
            config.setdefault("streams", {})[video_id] = {
                "enabled": bool(body.get("enabled", False)),
                "spam_threshold": int(body.get("spam_threshold", DEFAULT_SPAM_THRESHOLD)),
                "timeout_seconds": int(body.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)),
                "timeout_message": body.get("timeout_message", existing.get("timeout_message", DEFAULT_TIMEOUT_MESSAGE)),
                "reminder_message": body.get("reminder_message", existing.get("reminder_message", DEFAULT_REMINDER_MESSAGE)),
            }
            save_moderation(config)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "message": f"Moderation settings saved for {video_id}."
            }).encode("utf-8"))

        else:
            self._handle_unmatched(route)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        self.do_GET()

    def do_DELETE(self):
        self.do_GET()


def start_health_check_server():
    port = int(os.getenv("SERVER_PORT", 10000))
    server = ThreadingHTTPServer(('0.0.0.0', port), HealthCheckHandler)
    server.daemon_threads = True
    server.serve_forever()

threading.Thread(target=start_health_check_server, daemon=True).start()
# -----------------------------------------------

# 1. Main Project YouTube Client (Used for sending replies) — required
credentials_main = Authorize('client_secret.json', 'token.json', token_env='TOKEN')
youtube_main = build('youtube', 'v3', credentials=credentials_main)

# 2. Backup 1 Project YouTube Client (First fallback reader) — optional.
# If backup_client_secret.json / backup_token.json / TOKEN_BACKUP are all
# missing, credentials_backup1 is None and youtube_backup1 stays None too —
# the bot runs fine without it, just without this fallback layer.
credentials_backup1 = Authorize('backup_client_secret.json', 'backup_token.json', token_env='TOKEN_BACKUP', optional=True)
youtube_backup1 = build('youtube', 'v3', credentials=credentials_backup1) if credentials_backup1 else None

# 3. Backup 2 Project YouTube Client (Second fallback reader) — optional, same as above.
credentials_backup2 = Authorize('backup2_client_secret.json', 'backup2_token.json', token_env='TOKEN_BACKUP2', optional=True)
youtube_backup2 = build('youtube', 'v3', credentials=credentials_backup2) if credentials_backup2 else None

if not youtube_backup1 and not youtube_backup2:
    add_log("No backup credentials configured. API fallback will use the Main project (shares quota with replies).")
elif not youtube_backup2:
    add_log("Backup 2 not configured. API fallback order: Backup 1 -> Main (last resort).")
elif not youtube_backup1:
    add_log("Backup 1 not configured. API fallback order: Backup 2 -> Main (last resort).")
else:
    add_log("API fallback order: Backup 1 -> Backup 2 -> Main (last resort).")


# --- Quota failover: Main -> Backup 1 -> Backup 2 -> wait for 00:00 PT ---
# YouTube quota is per Google Cloud project (not per stream), so the
# exhausted-state is shared across every stream thread. Once a client hits
# quotaExceeded it is skipped by everyone until the next 00:00 PT reset.
# NOTE: Backup tokens must be authorized with the same bot account
# (@ThemeMasterBot) so replies/timeouts come from the bot's own channel.
class AllQuotasExhausted(Exception):
    """Raised when Main, Backup 1 and Backup 2 have all hit their daily quota."""
    def __init__(self, wait_seconds, utc_offset_label):
        self.wait_seconds = wait_seconds
        self.utc_offset_label = utc_offset_label
        super().__init__(
            f"All API quotas exhausted. Next reset at 00:00 PT ({utc_offset_label} UTC)."
        )


_api_clients = [
    (name, client) for name, client in (
        ("Main", youtube_main),
        ("Backup 1", youtube_backup1),
        ("Backup 2", youtube_backup2),
    ) if client is not None
]
_quota_exhausted_until = {}  # client name -> datetime (PT) when quota resets
_quota_lock = threading.Lock()
_PT_ZONE = ZoneInfo("America/Los_Angeles")


def _next_midnight_pt():
    now_pt = datetime.now(_PT_ZONE)
    return (now_pt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def is_quota_exceeded_error(e):
    return (
        isinstance(e, HttpError)
        and getattr(getattr(e, 'resp', None), 'status', None) == 403
        and "quotaExceeded" in str(e)
    )


def call_with_quota_failover(make_request, purpose="API call"):
    """
    Runs make_request(youtube_client) against Main, then Backup 1, then
    Backup 2, moving on only when a client reports quotaExceeded. Any other
    error is re-raised immediately. If every client is exhausted, raises
    AllQuotasExhausted (carrying the seconds until 00:00 PT).
    """
    for name, client in _api_clients:
        with _quota_lock:
            reset_at = _quota_exhausted_until.get(name)
            if reset_at is not None and datetime.now(_PT_ZONE) >= reset_at:
                del _quota_exhausted_until[name]
                reset_at = None
                add_log(f"Quota reset reached — {name} API is available again.")
            if reset_at is not None:
                continue  # Still exhausted, skip to the next client

        try:
            return make_request(client)
        except HttpError as e:
            if not is_quota_exceeded_error(e):
                raise
            with _quota_lock:
                newly_marked = name not in _quota_exhausted_until
                _quota_exhausted_until[name] = _next_midnight_pt()
            if newly_marked:
                add_log(f"{name} quota exceeded during {purpose}. Switching to next available API client...")

    wait_seconds, utc_offset_label = seconds_until_next_midnight_pt()
    raise AllQuotasExhausted(wait_seconds, utc_offset_label)


# --- API fallback reader chain: Backup 1 -> Backup 2 -> Main (last resort) ---
# Uses the same shared _quota_exhausted_until table as call_with_quota_failover,
# so a client one listener finds exhausted is skipped by every other listener
# AND by the reply-sending path. Main is always available (it is required at
# startup), so API fallback works even when no backup project is configured.
def reader_chain():
    return [c for c in (youtube_backup1, youtube_backup2, youtube_main) if c is not None]


def _client_name(client):
    for name, c in _api_clients:
        if c is client:
            return name
    return "API"


def _quota_is_exhausted(name):
    with _quota_lock:
        reset_at = _quota_exhausted_until.get(name)
        if reset_at is not None and datetime.now(_PT_ZONE) >= reset_at:
            del _quota_exhausted_until[name]
            add_log(f"Quota reset reached — {name} API is available again.")
            return False
        return reset_at is not None


def _mark_quota_exhausted(name):
    """Mark a client exhausted until 00:00 PT. Returns True if newly marked."""
    with _quota_lock:
        newly_marked = name not in _quota_exhausted_until
        _quota_exhausted_until[name] = _next_midnight_pt()
    return newly_marked


def pick_reader():
    """First client in the reader chain that still has quota, or None if all are exhausted."""
    for client in reader_chain():
        if not _quota_is_exhausted(_client_name(client)):
            return client
    return None


# v0.2.8.2: takes stream_name so the log line says which stream connected.
def getLiveChatId(LIVE_STREAM_ID, stream_name="?"):
    response = call_with_quota_failover(
        lambda yt: yt.videos().list(
            part="liveStreamingDetails",
            id=LIVE_STREAM_ID,
        ).execute(),
        purpose="getLiveChatId",
    )

    items = response.get('items', [])
    if not items:
        raise Exception(f"Live stream with ID '{LIVE_STREAM_ID}' not found.")

    live_details = items[0].get('liveStreamingDetails', {})
    liveChatId = live_details.get('activeLiveChatId')

    if not liveChatId:
        raise Exception(f"No active live chat found for stream '{LIVE_STREAM_ID}'. Make sure the stream is live!")

    add_log(f"Connected to Live Chat ID in '{stream_name}': {liveChatId}")
    return liveChatId


def sendReplyToLiveChat(liveChatId, message, stream_name="?"):
    """
    Sends messages using youtube_main (Main project quota). YouTube's
    response to this insert() call includes authorDetails for the bot's
    own post — including isChatModerator/isChatOwner — so every successful
    send doubles as a free, always-current mod-status check. This avoids
    liveChatModerators().list(), which requires the calling account to BE
    the channel owner and always 403s for a separate bot account, even
    when that bot genuinely has moderator status.
    """
    try:
        reply = call_with_quota_failover(
            lambda yt: yt.liveChatMessages().insert(
                part="snippet,authorDetails",
                body={
                    "snippet": {
                        "liveChatId": liveChatId,
                        "type": "textMessageEvent",
                        "textMessageDetails": {
                            "messageText": message,
                        }
                    }
                }
            ).execute(),
            purpose="sendReply",
        )
        add_log(f"Bot replied in '{stream_name}': {message}")

        author_details = reply.get("authorDetails", {})
        is_mod = bool(author_details.get("isChatModerator") or author_details.get("isChatOwner"))
        with _mod_status_cache_lock:
            _mod_status_cache[liveChatId] = is_mod
        return True
    except AllQuotasExhausted as e:
        add_log(f"All API quotas exhausted — dropped reply in '{stream_name}' (resets 00:00 PT, {e.utc_offset_label} UTC): {message}")
        return False
    except Exception as e:
        add_log(f"Failed to send message in '{stream_name}': {e}")
        return False


# --- Moderation: bot identity + moderator-status cache ---
BOT_CHANNEL_ID = "UCFLh_LNE-OSPRl1rwxB48rQ"  # @ThemeMasterBot

# liveChatId -> bool. Populated as a side effect of sendReplyToLiveChat()
# reading its own message's authorDetails — NOT via a separate API call,
# since liveChatModerators().list() requires the caller to be the channel
# owner and will 403 for a bot account regardless of its real mod status.
_mod_status_cache = {}
_mod_status_cache_lock = threading.Lock()


def is_bot_moderator(liveChatId):
    """
    Returns True/False if we already know (from a prior sendReplyToLiveChat
    call on this liveChatId), or None if unknown yet — e.g. the bot hasn't
    sent anything here yet this session. Callers should treat None as
    "attempt the action and let its own success/failure decide", not as
    "assume not a mod".
    """
    with _mod_status_cache_lock:
        return _mod_status_cache.get(liveChatId)


def timeoutUser(liveChatId, userChannelId, duration_seconds, stream_name="?"):
    """
    Times out a user from live chat for duration_seconds using
    liveChatBans().insert(). Returns (success: bool, permission_denied: bool).
    permission_denied is True specifically when the failure looks like the
    bot lacking moderator status (HTTP 403 insufficientPermissions/forbidden)
    — as opposed to some other failure (network error, invalid channel ID,
    etc.) — so callers can tell "not a mod" apart from "something else broke".
    """
    try:
        call_with_quota_failover(
            lambda yt: yt.liveChatBans().insert(
                part="snippet",
                body={
                    "snippet": {
                        "liveChatId": liveChatId,
                        "type": "temporary",
                        "banDurationSeconds": duration_seconds,
                        "bannedUserDetails": {
                            "channelId": userChannelId
                        }
                    }
                }
            ).execute(),
            purpose="timeoutUser",
        )
        add_log(f"Timed out user (channel {userChannelId}) in '{stream_name}' for {duration_seconds}s.")
        with _mod_status_cache_lock:
            _mod_status_cache[liveChatId] = True  # A successful ban proves mod status
        return True, False
    except HttpError as e:
        status = getattr(getattr(e, 'resp', None), 'status', None)
        try:
            reason = e.error_details if hasattr(e, 'error_details') else e.content.decode('utf-8', errors='replace')
        except Exception:
            reason = str(e) or repr(e)
        add_log(f"Failed to time out user in '{stream_name}': HTTP {status} — {reason}")

        permission_denied = status == 403
        if permission_denied:
            with _mod_status_cache_lock:
                _mod_status_cache[liveChatId] = False
        return False, permission_denied
    except AllQuotasExhausted as e:
        add_log(f"All API quotas exhausted — could not time out user in '{stream_name}' (resets 00:00 PT, {e.utc_offset_label} UTC).")
        return False, False
    except Exception as e:
        add_log(f"Failed to time out user in '{stream_name}': {e}")
        return False, False


def format_duration(seconds):
    """Human-readable duration for use in {duration} message placeholders."""
    if seconds < 60:
        return f"{seconds} second(s)"
    minutes = seconds // 60
    remaining = seconds % 60
    if remaining == 0:
        return f"{minutes} minute(s)"
    return f"{minutes} minute(s) {remaining} second(s)"


# --- Moderation: per-stream, per-user tracking state ---
# _recent_messages[video_id][userChannelId] -> (last_message_text, repeat_count)
# _offense_counts[video_id][userChannelId]  -> number of prior offenses (for escalation)
# _last_mod_request_time[video_id]  -> timestamp of the last "add me as mod"
#     message sent on this stream (per-stream cooldown, not once-ever).
# _last_mod_owner_reminder_time[video_id] -> timestamp of the last reminder
#     sent to a moderator/owner on this stream (per-stream cooldown).
# _recently_timed_out[video_id][userChannelId] -> timestamp of the last
#     successful timeout for this user on this stream. Messages that were
#     already queued up in the same sync_items()/API poll batch as the one
#     that triggered a timeout still get handed to run_moderation_check()
#     afterward — YouTube's ban hasn't silenced them yet from the bot's
#     point of view, just from the perspective of NEW messages arriving.
#     This buffer makes the bot itself skip anything from that user for a
#     short window right after a timeout, so a burst of messages sent all
#     at once can't cycle through reminder->timeout->reset several times
#     in a single batch and fire multiple real liveChatBans() calls for
#     what was really only ever one spam burst.
_recent_messages = {}
_offense_counts = {}
_last_mod_request_time = {}
_last_mod_owner_reminder_time = {}
_recently_timed_out = {}
_moderation_state_lock = threading.Lock()

MOD_MESSAGE_COOLDOWN_SECONDS = 10
POST_TIMEOUT_SKIP_SECONDS = 20  # Short buffer, not the real ban duration —
                                 # see _recently_timed_out note above for why.


def _send_mod_request(video_id, liveChatId, stream_name):
    """
    Sends MOD_REQUEST_MESSAGE, but at most once per MOD_MESSAGE_COOLDOWN_SECONDS
    per stream — so repeated violations while the bot still isn't a mod
    don't flood the chat, while still re-reminding periodically rather
    than staying silent forever after the first send.
    """
    now = time.time()
    with _moderation_state_lock:
        last_sent = _last_mod_request_time.get(video_id, 0)
        on_cooldown = (now - last_sent) < MOD_MESSAGE_COOLDOWN_SECONDS
        if not on_cooldown:
            _last_mod_request_time[video_id] = now

    if not on_cooldown:
        sendReplyToLiveChat(liveChatId, MOD_REQUEST_MESSAGE, stream_name=stream_name)


def _send_mod_owner_reminder(video_id, liveChatId, stream_name, reminder_msg):
    """
    Sends a reminder to a moderator/owner who triggered a violation, at
    most once per MOD_MESSAGE_COOLDOWN_SECONDS per stream — same cooldown
    treatment as the mod-request message, so repeated mod/owner violations
    (or several different mods in quick succession) don't flood the chat.
    """
    now = time.time()
    with _moderation_state_lock:
        last_sent = _last_mod_owner_reminder_time.get(video_id, 0)
        on_cooldown = (now - last_sent) < MOD_MESSAGE_COOLDOWN_SECONDS
        if not on_cooldown:
            _last_mod_owner_reminder_time[video_id] = now

    if not on_cooldown:
        sendReplyToLiveChat(liveChatId, reminder_msg, stream_name=stream_name)


def run_moderation_check(video_id, stream_name, liveChatId, userName, userChannelId, message_text, is_moderator, is_owner, recent_messages_unused=None):
    """
    Checks a single chat message against active moderation settings for
    this stream: spam-repeat (per-stream settings) and blocklist categories
    (global, independent of the per-stream enabled toggle). Applies the
    reminder/timeout escalation using whichever violation type's own
    duration and message templates apply. Called on every chat message,
    not just bot commands.
    """
    # A batch of sync_items()/API-poll results can contain several messages
    # a user sent within the same second or two — all handed to this
    # function one after another, with no gap for YouTube's actual ban to
    # have taken effect yet. Without this check, that whole backlog could
    # cycle through reminder -> timeout -> reset multiple times per batch,
    # firing several real liveChatBans() calls for one spam burst. Skip
    # anyone timed out in roughly the last POST_TIMEOUT_SKIP_SECONDS —
    # YouTube's own ban already silences them for the real duration; this
    # buffer only needs to cover the batch-processing gap, not the full ban.
    with _moderation_state_lock:
        last_timeout_at = _recently_timed_out.get(video_id, {}).get(userChannelId)
    if last_timeout_at is not None and (time.time() - last_timeout_at) < POST_TIMEOUT_SKIP_SECONDS:
        return

    config = load_moderation()
    lower_msg = message_text.strip().lower()

    # --- Check blocklist categories first (global, always active) ---
    matched_category = check_blocklist_categories(config, lower_msg)

    # --- Check spam-repeat (only if this stream has spam detection enabled) ---
    spam_settings = get_stream_moderation_settings(config, video_id)
    is_spam = False
    if spam_settings is not None:
        # Messages starting with "!" are bot commands (!gamble, !coins, ...)
        # and are never treated as spam — people legitimately repeat them,
        # and the bot's own reply cooldown already rate-limits them. They
        # still go through the blocklist check above. Sending one also
        # breaks the user's repeat streak, since it's a different message
        # from whatever they sent before it.
        is_command_message = lower_msg.startswith("!")

        with _moderation_state_lock:
            stream_recent = _recent_messages.setdefault(video_id, {})
            last_text, repeat_count = stream_recent.get(userChannelId, (None, 0))

            if is_command_message:
                repeat_count = 0
                stream_recent[userChannelId] = (None, 0)
            else:
                if lower_msg == last_text:
                    repeat_count += 1
                else:
                    repeat_count = 1
                stream_recent[userChannelId] = (lower_msg, repeat_count)

        if not is_command_message:
            spam_threshold = spam_settings.get("spam_threshold", DEFAULT_SPAM_THRESHOLD)
            if repeat_count >= spam_threshold:
                is_spam = True

    if matched_category is None and not is_spam:
        return  # No violation of any kind

    # A blocklist hit takes priority over spam if both somehow matched at
    # once (e.g. someone spams a banned word) — its message/duration apply.
    if matched_category is not None:
        duration_seconds = matched_category.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        timeout_template = matched_category.get("timeout_message", DEFAULT_BLOCKLIST_TIMEOUT_MESSAGE)
        reminder_template = matched_category.get("reminder_message", DEFAULT_BLOCKLIST_REMINDER_MESSAGE)
    else:
        duration_seconds = spam_settings.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        timeout_template = spam_settings.get("timeout_message", DEFAULT_TIMEOUT_MESSAGE)
        reminder_template = spam_settings.get("reminder_message", DEFAULT_REMINDER_MESSAGE)

    # --- Moderator/owner: reminder only (cooldown-limited), never escalates to a real timeout ---
    if is_moderator or is_owner:
        reminder_msg = reminder_template.format(userName=userName)
        _send_mod_owner_reminder(video_id, liveChatId, stream_name, reminder_msg)
        return

    # --- Regular viewer: 1st offense = reminder, 2nd offense = real timeout ---
    with _moderation_state_lock:
        stream_offenses = _offense_counts.setdefault(video_id, {})
        offense_count = stream_offenses.get(userChannelId, 0) + 1
        stream_offenses[userChannelId] = offense_count

    if offense_count == 1:
        reminder_msg = reminder_template.format(userName=userName)
        sendReplyToLiveChat(liveChatId, reminder_msg, stream_name=stream_name)
        return

    # 2nd (or later) offense — attempt an actual timeout.
    # is_bot_moderator() returns True/False only if we already know from a
    # prior send/ban on this liveChatId; None means unknown, in which case
    # we still attempt the timeout and let its own result tell us — this
    # avoids the owner-only liveChatModerators().list() endpoint entirely.
    known_mod_status = is_bot_moderator(liveChatId)

    if known_mod_status is False:
        _send_mod_request(video_id, liveChatId, stream_name)
        return

    success, permission_denied = timeoutUser(liveChatId, userChannelId, duration_seconds, stream_name=stream_name)

    if not success and permission_denied:
        _send_mod_request(video_id, liveChatId, stream_name)
        return

    if success:
        timeout_msg = timeout_template.format(
            userName=userName, duration=format_duration(duration_seconds)
        )
        sendReplyToLiveChat(liveChatId, timeout_msg, stream_name=stream_name)
        # Reset offense count after a successful timeout so it doesn't
        # immediately re-trigger the moment they're allowed back, and
        # record when this happened so any remaining messages from this
        # user still left in the current batch get skipped instead of
        # re-triggering another full reminder->timeout cycle.
        with _moderation_state_lock:
            _offense_counts.setdefault(video_id, {})[userChannelId] = 0
            _recently_timed_out.setdefault(video_id, {})[userChannelId] = time.time()


# --- Quiz: !quiz / !ans ---
#
# !quiz posts a random tech question with A-D choices and starts a
# QUIZ_DURATION_SECONDS timer for that stream. !ans <letter> answers it:
# the FIRST correct answer wins QUIZ_REWARD NeilCoins and ends the quiz;
# wrong answers cost nothing and can be retried. If nobody gets it before
# the timer runs out, a timeout message (with the answer) is posted.
#
# Quiz state is per stream and in memory only (a redeploy simply drops any
# running quiz). Everything touching that state happens under _quiz_lock so
# two correct answers at the same instant, or an answer landing exactly as
# the timer fires, can never both win or double-pay.
#
# Cost notes: every bot message is 50 quota units. There are no periodic
# countdown posts for that reason. To stop a spammer from burning quota via
# wrong answers, each user is limited to one !ans per
# QUIZ_ANSWER_THROTTLE_SECONDS and the stream posts at most one ❌ reply per
# QUIZ_WRONG_REPLY_MIN_GAP_SECONDS (the answer still counts either way).
QUIZ_DURATION_SECONDS = 180
QUIZ_REWARD = 67
QUIZ_ANSWER_THROTTLE_SECONDS = 2
QUIZ_WRONG_REPLY_MIN_GAP_SECONDS = 5

# (question, [four options], index of the correct option). Options are
# shuffled each time a quiz starts. The whole chat message (question + all
# four options + prefix/suffix) must stay within YouTube's 200-character
# limit — a test checks every entry.
QUIZ_QUESTIONS = [
    ("What does CPU stand for?", ["Central Processing Unit", "Computer Personal Unit", "Core Power Unit", "Central Program Utility"], 0),
    ("Which one is a markup language?", ["Python", "Java", "HTML", "C"], 2),
    ("What does HTTP stand for?", ["HyperText Transfer Protocol", "High Transfer Text Process", "HyperText Transmission Program", "Host Transfer Type Protocol"], 0),
    ("Which port does HTTPS use by default?", ["80", "21", "443", "25"], 2),
    ("What does SQL stand for?", ["Structured Query Language", "Simple Question Logic", "Sequential Query List", "System Quick Language"], 0),
    ("Which data structure uses FIFO order?", ["Stack", "Queue", "Tree", "Heap"], 1),
    ("Which data structure uses LIFO order?", ["Queue", "Array", "Stack", "Graph"], 2),
    ("What does RAM stand for?", ["Random Access Memory", "Read Access Memory", "Rapid Active Module", "Run All Memory"], 0),
    ("Who created the Linux kernel?", ["Bill Gates", "Linus Torvalds", "Steve Jobs", "Dennis Ritchie"], 1),
    ("What does GPU stand for?", ["General Power Unit", "Graphics Processing Unit", "Game Program Utility", "Graphical Pixel Unit"], 1),
    ("Which one is a version control system?", ["Git", "Docker", "Nginx", "Redis"], 0),
    ("What is decimal 5 in binary?", ["101", "110", "011", "111"], 0),
    ("How many bits are in a byte?", ["4", "8", "16", "32"], 1),
    ("Which symbol starts a comment in Python?", ["//", "#", "--", "/*"], 1),
    ("What does DNS do?", ["Encrypts files", "Turns domain names into IPs", "Blocks viruses", "Compresses images"], 1),
    ("Which one is a relational database?", ["MongoDB", "PostgreSQL", "Redis", "Neo4j"], 1),
    ("What does URL stand for?", ["Uniform Resource Locator", "Universal Reference Link", "Unified Resource Loader", "User Request Line"], 0),
    ("Which keyword defines a function in Python?", ["func", "define", "def", "function"], 2),
    ("What does USB stand for?", ["Universal Serial Bus", "United System Board", "Unified Storage Block", "Universal Signal Base"], 0),
    ("Which one is an open source OS?", ["Windows", "macOS", "Linux", "iOS"], 2),
    ("What does CSS control on a web page?", ["Database queries", "Styling and layout", "Server routing", "Encryption"], 1),
    ("Which protocol is used to send email?", ["FTP", "SMTP", "SSH", "DHCP"], 1),
    ("Which one runs JavaScript on servers?", ["Node.js", "Django", "Flask", "Laravel"], 0),
    ("What does IP stand for in IP address?", ["Internet Protocol", "Internal Program", "Interface Port", "Input Process"], 0),
    ("What does git commit do?", ["Deletes the repo", "Saves changes to history", "Downloads code", "Merges branches"], 1),
    ("Which logic gate outputs 1 only if both inputs are 1?", ["OR", "XOR", "AND", "NOT"], 2),
    ("What does SSD stand for?", ["Solid State Drive", "Super Speed Disk", "Static System Data", "Secure Storage Device"], 0),
    ("Which part executes program instructions?", ["Monitor", "CPU", "Keyboard", "Router"], 1),
    ("What does API stand for?", ["Application Programming Interface", "Applied Program Integration", "Automated Process Input", "Application Protocol Index"], 0),
    ("Which HTTP status code means Not Found?", ["200", "301", "404", "500"], 2),
    ("What is a bug in software?", ["A feature", "An error or flaw", "A virus scan", "A backup"], 1),
    ("What is the main job of a firewall?", ["Cool the CPU", "Filter network traffic", "Store passwords", "Speed up Wi-Fi"], 1),
    ("Which language is named after a comedy group?", ["Python", "Ruby", "Swift", "Perl"], 0),
    ("What does HTML stand for?", ["HyperText Markup Language", "High Tech Modern Language", "HyperTransfer Markup Logic", "Home Tool Markup Language"], 0),
    ("In Python, what does len('hi') return?", ["1", "2", "3", "Error"], 1),
    ("What is the smallest unit of data?", ["Byte", "Bit", "Kilobyte", "Word"], 1),
    ("Which company develops the Chrome browser?", ["Microsoft", "Mozilla", "Google", "Apple"], 2),
    ("Which tool is used to containerize apps?", ["Docker", "Excel", "Photoshop", "Notepad"], 0),
    ("What does WWW stand for?", ["World Wide Web", "Wide Web Window", "World Web Wire", "Wireless Wide Web"], 0),
    ("What does OS stand for?", ["Open Software", "Operating System", "Online Service", "Output Stream"], 1),
    ("Which number system uses only 0 and 1?", ["Decimal", "Octal", "Binary", "Hex"], 2),
    ("Which Linux command lists files?", ["ls", "cd", "rm", "mv"], 0),
    ("What does RGB stand for?", ["Red Green Blue", "Run Graphics Boot", "Raw Gray Black", "Real Green Beam"], 0),
    ("Which port does SSH use by default?", ["21", "22", "23", "80"], 1),
    ("What does LAN stand for?", ["Local Area Network", "Long Access Node", "Linked Array Net", "Large Area Network"], 0),
]

_active_quizzes = {}           # stream_key -> quiz dict (see start_quiz)
_quiz_last_answer = {}         # stream_key -> {user_key: timestamp of their last counted !ans}
_quiz_last_wrong_reply = {}    # stream_key -> timestamp of the last ❌ reply posted
_quiz_question_bag = []        # shuffled indices; refilled once every question has been used
_quiz_next_id = 0
_quiz_lock = threading.Lock()


def _quiz_stream_key(video_id, liveChatId):
    return video_id if video_id is not None else liveChatId


def _is_quiz_answer_command(lower_msg):
    parts = lower_msg.split()
    return bool(parts) and parts[0] == "!ans"


def _next_quiz_question():
    """Random question, no repeats until all have been used. Caller holds _quiz_lock."""
    global _quiz_question_bag
    if not _quiz_question_bag:
        _quiz_question_bag = list(range(len(QUIZ_QUESTIONS)))
        random.shuffle(_quiz_question_bag)
    return QUIZ_QUESTIONS[_quiz_question_bag.pop()]


def format_quiz_message(question_text, options):
    choices = "  ".join(f"{letter}) {text}" for letter, text in zip("ABCD", options))
    return f"🧠 QUIZ ({QUIZ_DURATION_SECONDS // 60} min): {question_text} {choices} — reply !ans a/b/c/d"


def _cancel_quiz(stream_key, quiz_id):
    with _quiz_lock:
        quiz = _active_quizzes.get(stream_key)
        if quiz is not None and quiz["id"] == quiz_id:
            quiz["timer"].cancel()
            del _active_quizzes[stream_key]
            _quiz_last_answer.pop(stream_key, None)


def _quiz_expire(stream_key, quiz_id):
    """Timer callback: the time limit was reached with no correct answer."""
    with _quiz_lock:
        quiz = _active_quizzes.get(stream_key)
        if quiz is None or quiz["id"] != quiz_id:
            return  # already won or cancelled
        del _active_quizzes[stream_key]
        _quiz_last_answer.pop(stream_key, None)

    minutes = QUIZ_DURATION_SECONDS // 60
    sendReplyToLiveChat(
        quiz["liveChatId"],
        f"⏰ {minutes} minute timeout has been reached! Nobody answered correctly — "
        f"the answer was {quiz['correct_letter']}) {quiz['correct_text']}.",
        stream_name=quiz["stream_name"],
    )


def start_quiz(stream_key, liveChatId, stream_name):
    """
    Starts a quiz on this stream. Returns "started", "already_running", or
    "failed" (the question message couldn't be posted, so the quiz was cancelled).
    """
    global _quiz_next_id
    with _quiz_lock:
        if stream_key in _active_quizzes:
            return "already_running"

        question_text, options, correct_index = _next_quiz_question()
        correct_text = options[correct_index]
        shuffled = list(options)
        random.shuffle(shuffled)
        correct_letter = "ABCD"[shuffled.index(correct_text)]

        _quiz_next_id += 1
        quiz_id = _quiz_next_id
        timer = threading.Timer(QUIZ_DURATION_SECONDS, _quiz_expire, args=(stream_key, quiz_id))
        timer.daemon = True
        _active_quizzes[stream_key] = {
            "id": quiz_id,
            "timer": timer,
            "correct_letter": correct_letter,
            "correct_text": correct_text,
            "liveChatId": liveChatId,
            "stream_name": stream_name,
        }
        _quiz_last_answer.pop(stream_key, None)
        timer.start()

    posted = sendReplyToLiveChat(liveChatId, format_quiz_message(question_text, shuffled), stream_name=stream_name)
    if not posted:
        _cancel_quiz(stream_key, quiz_id)
        return "failed"
    add_log(f"Quiz started in '{stream_name}' ({QUIZ_DURATION_SECONDS}s, answer {correct_letter}).")
    return "started"


def _award_quiz_coins(userName, userChannelId):
    coins = load_coins()
    user_key = make_user_key(userName, userChannelId)
    record = get_user_record(coins, user_key, display_name=userName)
    record['balance'] += QUIZ_REWARD
    save_coins(coins)
    return record['balance']


def handle_quiz_answer(userName, userChannelId, lower_msg, liveChatId, last_reply_time, COOLDOWN_SECONDS, stream_name, video_id):
    """
    Handles "!ans <letter>". Deliberately called BEFORE the shared reply
    cooldown check in process_command: that check drops a command outright,
    which would silently lose answers from two people answering close
    together. Evaluation always happens here; only the *replies* that aren't
    a win (no-quiz / usage hints) still respect the normal cooldown, and
    ❌ replies have their own spacing — see the cost notes above.
    Returns the new last_reply_time.
    """
    now = time.time()
    stream_key = _quiz_stream_key(video_id, liveChatId)
    user_key = make_user_key(userName, userChannelId)
    cooldown_active = (now - last_reply_time) < COOLDOWN_SECONDS

    parts = lower_msg.split()
    letter = parts[1].strip("().,:!") if len(parts) > 1 else ""
    valid_letter = len(letter) == 1 and letter in "abcd"

    outcome = None
    winning_quiz = None
    send_wrong_reply = False
    with _quiz_lock:
        quiz = _active_quizzes.get(stream_key)
        if quiz is None:
            outcome = "no_quiz"
        elif not valid_letter:
            outcome = "bad_usage"
        else:
            per_user = _quiz_last_answer.setdefault(stream_key, {})
            if now - per_user.get(user_key, 0) < QUIZ_ANSWER_THROTTLE_SECONDS:
                outcome = "throttled"
            else:
                per_user[user_key] = now
                if letter.upper() == quiz["correct_letter"]:
                    quiz["timer"].cancel()
                    del _active_quizzes[stream_key]
                    _quiz_last_answer.pop(stream_key, None)
                    winning_quiz = quiz
                    outcome = "correct"
                else:
                    outcome = "wrong"
                    if now - _quiz_last_wrong_reply.get(stream_key, 0) >= QUIZ_WRONG_REPLY_MIN_GAP_SECONDS:
                        _quiz_last_wrong_reply[stream_key] = now
                        send_wrong_reply = True

    if outcome == "correct":
        new_balance = _award_quiz_coins(userName, userChannelId)
        add_log(f"Quiz won in '{stream_name}' by {userName} (+{QUIZ_REWARD} NeilCoins).")
        sendReplyToLiveChat(
            liveChatId,
            f"🎉 {userName} got it right! The answer was {winning_quiz['correct_letter']}) "
            f"{winning_quiz['correct_text']}. +{QUIZ_REWARD} NeilCoins — new balance: {new_balance}.",
            stream_name=stream_name,
        )
        return time.time()

    if outcome == "wrong":
        if send_wrong_reply:
            sendReplyToLiveChat(liveChatId, f"❌ {userName} — not quite, try again!", stream_name=stream_name)
        return last_reply_time  # don't extend the shared cooldown for wrong answers

    if outcome == "no_quiz" and not cooldown_active:
        sendReplyToLiveChat(liveChatId, f"{userName} No quiz is running — start one with !quiz", stream_name=stream_name)
        return time.time()

    if outcome == "bad_usage" and not cooldown_active:
        sendReplyToLiveChat(liveChatId, f"{userName} Answer with !ans a, !ans b, !ans c or !ans d", stream_name=stream_name)
        return time.time()

    return last_reply_time  # throttled, or a hint suppressed by the cooldown


def process_command(userName, userChannelId, message_text, liveChatId, last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS, stream_name="?", video_id=None, is_moderator=False, is_owner=False, recent_messages=None):
    """Processes commands coming from either pytchat or backup API."""
    clean_name = userName.lower().replace('@', '')
    clean_handle = str(userChannelId).lower().replace('@', '')

    if any(bot in clean_name or bot in clean_handle for bot in BLOCKED_BOTS):
        add_log(f"Ignored message from blocked bot: {userName}")
        return last_reply_time

    message_text = message_text.strip()
    add_log(f"New chat message in '{stream_name}' from {userName}: {message_text}")

    if video_id is not None:
        run_moderation_check(
            video_id, stream_name, liveChatId, userName, userChannelId,
            message_text, is_moderator, is_owner, recent_messages
        )

    lower_msg = message_text.lower()
    is_command = (
        lower_msg in ["hello", "hi", "hey", "!revertical", "!random", "!rand", "!commands", "!help", "e",
                      "!coins", "!resetcoins", "!leaderboard", "!quiz", "!angryrevertical"]
        or lower_msg.startswith(("!chatmbr", "!ai", "!gamble", "!giftpoint"))
        or lower_msg == "!say" or lower_msg.startswith("!say ")
        or _is_quiz_answer_command(lower_msg)
    )

    if not is_command:
        return last_reply_time

    # !ans is handled BEFORE the shared reply cooldown on purpose: that check
    # drops a command entirely, which would silently lose an answer whenever
    # two people answered within a few seconds of each other (or right after
    # any other bot reply). handle_quiz_answer applies its own throttling.
    if _is_quiz_answer_command(lower_msg):
        return handle_quiz_answer(
            userName, userChannelId, lower_msg, liveChatId, last_reply_time,
            COOLDOWN_SECONDS, stream_name, video_id
        )

    current_time = time.time()
    if current_time - last_reply_time < COOLDOWN_SECONDS:
        # Round UP (ceiling), not truncate — truncating a value like 0.87s
        # down to 0 makes the log claim "0s remaining" while the cooldown
        # is, in fact, still active. Ceiling guarantees the displayed
        # number is only ever 0 once there's truly nothing left to wait.
        time_left = math.ceil(COOLDOWN_SECONDS - (current_time - last_reply_time))
        add_log(f"Skipped reply to {userName}: Cooldown active ({time_left}s remaining)")
        return last_reply_time

    if lower_msg in ["hello", "hi", "hey"]:
        sendReplyToLiveChat(liveChatId, f"Hey {userName}! Welcome to the stream!", stream_name=stream_name)
        return time.time()

    elif lower_msg in ["!revertical"]:
        sendReplyToLiveChat(liveChatId, f"dawg WHO said “revertical” 😭✌️", stream_name=stream_name)
        return time.time()

    elif lower_msg in ["!random", "!rand"]:
        jokes = [
            # Classic Dad Jokes
            "Why do fathers take an extra pair of socks when they go golfing? In case they get a hole in one!",
            "Dear Math, grow up and solve your own problems.",
            "What has more letters than the alphabet? The post office!",
            "Why are elevator jokes so classic and good? They work on so many levels!",
            "What do you call a fake noodle? An impasta!",
            "What do you call a belt made out of watches? A waist of time!",
            "Why did the scarecrow win an award? Because he was outstanding in his field!",
            "Why don't skeletons ever go trick or treating? Because they have no body to go with!",
            "What's brown and sticky? A stick!",
            # Tech & Developer Jokes
            "There are 10 types of people in the world: those who understand binary, and those who don't.",
            "Why do programmers prefer dark mode? Because light attracts bugs!",
            "A SQL query walks into a bar, walks up to two tables and asks: 'Can I join you?'",
            "Why do Java programmers have to wear glasses? Because they don't C#!",
            "How many programmers does it take to change a lightbulb? None, that's a hardware problem!",
            "Real programmers count from 0.",
            "Software developers: turning caffeine into code since forever.",
            "Why was the computer cold? It left its Windows open!",
            "There's no place like 127.0.0.1"
        ]
        joke = random.choice(jokes)
        sendReplyToLiveChat(liveChatId, joke, stream_name=stream_name)
        return time.time()

    elif lower_msg in ["!commands", "!help"]:
        cmd_url = "https://youtube-livestream-chatbot.onrender.com/commands"
        sendReplyToLiveChat(liveChatId, f"{userName} -> The bot commands are available at {cmd_url}", stream_name=stream_name)
        return time.time()

    elif lower_msg in ["e"]:
        sendReplyToLiveChat(liveChatId, f"E", stream_name=stream_name)
        return time.time()

    # --- !say command (v0.2.8.1) ---
    elif lower_msg == "!say" or lower_msg.startswith("!say "):
        parts = message_text.split(maxsplit=1)   # keep the original casing
        say_text = parts[1].strip() if len(parts) > 1 else ""

        if not say_text:
            sendReplyToLiveChat(liveChatId, f"{userName} Usage: !say <text>", stream_name=stream_name)
            return time.time()

        # Don't let the bot repeat anything on the global blocklist.
        if check_blocklist_categories(load_moderation(), say_text.lower()) is not None:
            add_log(f"!say from {userName} skipped in '{stream_name}': text matches the blocklist.")
            return time.time()

        if len(say_text) > 197:
            say_text = say_text[:197] + "..."

        sendReplyToLiveChat(liveChatId, say_text, stream_name=stream_name)
        return time.time()

    elif lower_msg.startswith(("!chatmbr", "!ai")):
        query = message_text[8:].strip() if lower_msg.startswith("!chatmbr") else message_text[3:].strip()

        if not query:
            sendReplyToLiveChat(liveChatId, f"{userName} Please provide a query! Usage: !chatmbr <question> or !ai <question>", stream_name=stream_name)
            return time.time()
        else:
            try:
                encoded_query = urllib.parse.quote(query)
                encoded_user = urllib.parse.quote(userName)
                api_url = f"https://chatmbr-bot.vercel.app/api/chat?platform=YouTube&user={encoded_user}&limit=200&query={encoded_query}"
                req = urllib.request.Request(api_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=8) as api_response:
                    api_reply = api_response.read().decode("utf-8").strip()

                if len(api_reply) > 200:
                    api_reply = api_reply[:197] + "..."

                sendReplyToLiveChat(liveChatId, api_reply, stream_name=stream_name)
                return time.time()
            except Exception as e:
                add_log(f"Error fetching from ChatMBR API: {e}")
                sendReplyToLiveChat(liveChatId, f"{userName} Unable to reach ChatMBR right now.", stream_name=stream_name)
                return time.time()

    elif lower_msg == "!coins":
        coins = load_coins()
        user_key = make_user_key(userName, userChannelId)
        record = get_user_record(coins, user_key, display_name=userName)
        save_coins(coins)  # Persist in case this created a brand-new user record
        sendReplyToLiveChat(liveChatId, f"{userName} has {record['balance']} NeilCoins.", stream_name=stream_name)
        return time.time()

    elif lower_msg.startswith("!gamble"):
        parts = message_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip().lstrip('-').isdigit():
            sendReplyToLiveChat(liveChatId, f"{userName} Usage: !gamble <number> (e.g. !gamble 50)", stream_name=stream_name)
            return time.time()

        bet = int(parts[1].strip())
        if bet <= 0:
            sendReplyToLiveChat(liveChatId, f"{userName} Bet must be a positive number!", stream_name=stream_name)
            return time.time()

        coins = load_coins()
        user_key = make_user_key(userName, userChannelId)
        record = get_user_record(coins, user_key, display_name=userName)

        if record['balance'] == 0:
            if record.get('resetcoins_uses', 0) < RESETCOINS_MAX_USES:
                sendReplyToLiveChat(liveChatId, f"{userName}, you have 0 NeilCoins — use !resetcoins to get some back, or try !quiz to earn more.", stream_name=stream_name)
            else:
                sendReplyToLiveChat(liveChatId, f"{userName}, you have 0 NeilCoins and no !resetcoins left — wait for someone to giftpoint you, or try !quiz to earn some.", stream_name=stream_name)
            save_coins(coins)
            return time.time()

        if bet > record['balance']:
            sendReplyToLiveChat(liveChatId, f"{userName} You only have {record['balance']} NeilCoins — can't bet {bet}!", stream_name=stream_name)
            save_coins(coins)
            return time.time()

        won = random.choice([True, False])
        if won:
            record['balance'] += bet
            sendReplyToLiveChat(liveChatId, f"🎉 {userName} gambled {bet} and WON! New balance: {record['balance']} NeilCoins.", stream_name=stream_name)
        else:
            record['balance'] -= bet
            sendReplyToLiveChat(liveChatId, f"💀 {userName} gambled {bet} and LOST! New balance: {record['balance']} NeilCoins.", stream_name=stream_name)

        save_coins(coins)
        return time.time()

    elif lower_msg.startswith("!giftpoint"):
        parts = message_text.split(maxsplit=2)
        if len(parts) < 3 or not parts[2].strip().isdigit():
            sendReplyToLiveChat(liveChatId, f"{userName} Usage: !giftpoint @Username <points>", stream_name=stream_name)
            return time.time()

        target_raw = parts[1].strip()
        gift_amount = int(parts[2].strip())

        if gift_amount <= 0:
            sendReplyToLiveChat(liveChatId, f"{userName} Gift amount must be a positive number!", stream_name=stream_name)
            return time.time()

        # Clean `@` from both target and sender for a clean string check
        clean_target = target_raw.lstrip('@').strip().lower()
        clean_sender = userName.lstrip('@').strip().lower()

        if clean_target == clean_sender:
            sendReplyToLiveChat(liveChatId, f"{userName} You can't giftpoint yourself!", stream_name=stream_name)
            return time.time()

        coins = load_coins()
        sender_key = make_user_key(userName, userChannelId)
        sender_record = get_user_record(coins, sender_key, display_name=userName)

        if sender_record['balance'] == 0:
            if sender_record.get('resetcoins_uses', 0) < RESETCOINS_MAX_USES:
                sendReplyToLiveChat(liveChatId, f"{userName}, you have 0 NeilCoins — use !resetcoins to get some back, or try !quiz to earn more.", stream_name=stream_name)
            else:
                sendReplyToLiveChat(liveChatId, f"{userName}, you have 0 NeilCoins and no !resetcoins left — wait for someone to giftpoint you, or try !quiz to earn some.", stream_name=stream_name)
            save_coins(coins)
            return time.time()

        if gift_amount > sender_record['balance']:
            sendReplyToLiveChat(liveChatId, f"{userName} You only have {sender_record['balance']} NeilCoins — can't gift {gift_amount}!", stream_name=stream_name)
            save_coins(coins)
            return time.time()

        # Only gift to users who already have a real account (i.e. they've
        # chatted at least once, tied to an actual channel ID). This closes
        # an exploit where !giftpoint could conjure a brand-new coin account
        # for any typed name — including names nobody could ever claim —
        # effectively creating coins out of thin air.
        target_key = None
        for key, rec in coins.items():
            stored_name_clean = rec.get('name', '').lstrip('@').strip().lower()
            if stored_name_clean == clean_target:
                target_key = key
                break

        if target_key is None:
            sendReplyToLiveChat(liveChatId, f"{userName} Couldn't find a user named '{target_raw}'. They must send !gamble or !coins first!", stream_name=stream_name)
            save_coins(coins)
            return time.time()

        target_record = coins[target_key]

        sender_record['balance'] -= gift_amount
        target_record['balance'] += gift_amount
        save_coins(coins)

        sendReplyToLiveChat(liveChatId, f"🎁 {userName} gifted {gift_amount} NeilCoins to {target_record['name']}!", stream_name=stream_name)
        return time.time()

    elif lower_msg == "!resetcoins":
        coins = load_coins()
        user_key = make_user_key(userName, userChannelId)
        record = get_user_record(coins, user_key)

        if record['balance'] != 0:
            sendReplyToLiveChat(liveChatId, f"{userName} !resetcoins only works when your balance is exactly 0 (you have {record['balance']}).", stream_name=stream_name)
            save_coins(coins)
            return time.time()

        if record.get('resetcoins_uses', 0) >= RESETCOINS_MAX_USES:
            sendReplyToLiveChat(liveChatId, f"{userName} You've already used !resetcoins {RESETCOINS_MAX_USES} times — no more resets for you!", stream_name=stream_name)
            save_coins(coins)
            return time.time()

        record['balance'] += RESETCOINS_GRANT
        record['resetcoins_uses'] = record.get('resetcoins_uses', 0) + 1
        save_coins(coins)

        uses_left = RESETCOINS_MAX_USES - record['resetcoins_uses']
        sendReplyToLiveChat(liveChatId, f"{userName} got +{RESETCOINS_GRANT} NeilCoins! Balance: {record['balance']} ({uses_left} reset(s) left).", stream_name=stream_name)
        return time.time()

    elif lower_msg == "!angryrevertical":
        coins = load_coins()
        user_key = make_user_key(userName, userChannelId)
        record = get_user_record(coins, user_key, display_name=userName)

        if record['balance'] > ANGRYREVERTICAL_PENALTY:
            record['balance'] -= ANGRYREVERTICAL_PENALTY
            save_coins(coins)
            sendReplyToLiveChat(
                liveChatId,
                f"dawg WHO said “angryrevertical” 😭✌️🥀 was it {userName}!? "
                f"That's it, imma lose {ANGRYREVERTICAL_PENALTY:,} NeilCoins. "
                f"New balance: {record['balance']}",
                stream_name=stream_name
            )
        else:
            save_coins(coins)  # Persist in case this created a brand-new user record
            sendReplyToLiveChat(
                liveChatId,
                f"dawg WHO said “angryrevertical” 😭✌️🥀 was it {userName}!? "
                f"Wait, u have {record['balance']} NeilCoins???? "
                f"Ohhh, I won't lose {ANGRYREVERTICAL_PENALTY:,} NeilCoins for now",
                stream_name=stream_name
            )
        return time.time()

    elif lower_msg == "!leaderboard":
        theme_master = "https://youtube-livestream-chatbot.onrender.com/leaderboard"
        sendReplyToLiveChat(liveChatId, f"{userName} -> The leaderboard is available at {theme_master}", stream_name=stream_name)
        return time.time()

    elif lower_msg == "!quiz":
        result = start_quiz(_quiz_stream_key(video_id, liveChatId), liveChatId, stream_name)
        if result == "already_running":
            sendReplyToLiveChat(liveChatId, f"{userName} A quiz is already running — answer with !ans a, b, c or d", stream_name=stream_name)
            return time.time()
        if result == "started":
            return time.time()
        return last_reply_time  # "failed": the question never posted (already logged)

    return last_reply_time


def listen_to_stream(stream_id, stream_name, stop_flag):
    """
    Listen to a single stream in its own thread. Retries getLiveChatId in a
    loop (not recursion) since chat may not be active yet (e.g. a premiere
    that hasn't started playing). Checks stop_flag regularly so the /live
    dashboard's Remove button can cleanly shut this thread down.

    Each stream gets its own independent pytchat -> Backup 1 API -> Backup 2
    API fallback chain, same as the original single-stream design.
    """
    _clear_pytchat_retry_request(stream_id)  # a brand-new listener tries pytchat first anyway

    # API fallback reader: Backup 1 -> Backup 2 -> Main (last resort). Never None,
    # because Main is always configured. default_youtube_reader is where we
    # return to after the 00:00 PT quota reset.
    default_youtube_reader = reader_chain()[0]
    active_youtube_backup = pick_reader() or default_youtube_reader

    if stop_flag.is_set():
        add_log(f"'{stream_name}' ({stream_id}) stopped before chat became available.")
        return

    # Only these two messages (raised by getLiveChatId itself) genuinely mean
    # "this stream doesn't exist / has no active chat" — safe to stop
    # immediately on. Any other exception (network blips, malformed API
    # responses, etc.) is treated as transient and gets up to 2 retries
    # (3 attempts total) with growing delays before giving up, so a
    # startup hiccup doesn't falsely kill a real stream.
    KNOWN_UNAVAILABLE_MARKERS = ("not found.", "No active live chat found")
    RETRY_DELAYS_SECONDS = [15, 30, 60, 60]  # Delay before attempt 2, then before attempt 3

    liveChatId = None
    attempt_number = 1
    while liveChatId is None:
        if stop_flag.is_set():
            add_log(f"'{stream_name}' ({stream_id}) stopped before chat became available.")
            return
        try:
            with CONNECTION_GATE.slot(stream_id):
                liveChatId = getLiveChatId(stream_id, stream_name)
            add_log(f"Started listening to '{stream_name}' ({stream_id})")
        except ConnectionQueueCancelled:
            add_log(f"'{stream_name}' ({stream_id}) stopped while waiting in the connection queue.")
            return
        except AllQuotasExhausted as qe:
            add_log(f"All API quotas exhausted while connecting '{stream_name}' ({stream_id}). Waiting until 00:00 PT ({qe.utc_offset_label} UTC) to retry...")
            deadline = time.time() + qe.wait_seconds + 5  # small buffer past the reset
            while time.time() < deadline:
                if stop_flag.is_set():
                    add_log(f"'{stream_name}' ({stream_id}) stopped while waiting for quota reset.")
                    return
                time.sleep(1)
            add_log(f"Quota reset time reached for '{stream_name}'. Retrying live chat ID fetch...")
            continue  # Doesn't count against the retry attempts
        except Exception as e:
            error_text = str(e)
            is_known_unavailable = any(marker in error_text for marker in KNOWN_UNAVAILABLE_MARKERS)

            add_log(f"Error connecting to '{stream_name}' ({stream_id}): {e}")

            if is_known_unavailable:
                add_log(f"🔴 '{stream_name}' ({stream_id}) — no active live chat. Stopping listener.")
                mark_stream_stopped(stream_id)
                add_log(f"'{stream_name}' ({stream_id}) listener stopped (never went live / already ended). /live now shows it as ⚪ Stopped.")
                return

            if attempt_number > len(RETRY_DELAYS_SECONDS):
                add_log(f"🔴 '{stream_name}' ({stream_id}) — unexpected error persisted after {attempt_number} attempts. Stopping listener.")
                mark_stream_stopped(stream_id)
                add_log(f"'{stream_name}' ({stream_id}) listener stopped (connection error). /live now shows it as ⚪ Stopped.")
                return

            delay = RETRY_DELAYS_SECONDS[attempt_number - 1]
            attempt_number += 1
            add_log(f"Unexpected error for '{stream_name}' — retrying (attempt {attempt_number}/{len(RETRY_DELAYS_SECONDS) + 1}) in {delay} seconds...")
            for _ in range(delay):
                if stop_flag.is_set():
                    return
                time.sleep(1)

    BLOCKED_BOTS = {"nightbot", "streamelements", "moobot", "streamlabs", "thememasterbot", "nabatchatbot", "windows243remastered"}
    COOLDOWN_SECONDS = 10
    last_reply_time = 0

    use_api_fallback = False
    next_page_token = None
    chat = None

    last_pytchat_retry = 0
    chat_started_at = time.time()   # when the current pytchat object was created
    rapid_pytchat_deaths = 0        # consecutive deaths shortly after a restore
    PYTCHAT_RETRY_INTERVAL = 120

    # Stream-end detection: count consecutive API failures
    # If we get 3+ in a row, the stream has likely ended
    consecutive_api_failures = 0
    MAX_CONSECUTIVE_FAILURES = 3

    add_log(f"Connecting pytchat listener for '{stream_name}' (0 quota cost)...")
    try:
        chat = safe_pytchat_create(stream_id)
        chat_started_at = time.time()
        if not chat.is_alive():
            raise Exception(f"Pytchat stream initialization failed ({describe_pytchat_stop(chat)}).")
        add_log(f"Listening to '{stream_name}' via pytchat...")
    except ConnectionQueueCancelled:
        add_log(f"'{stream_name}' ({stream_id}) stopped while waiting in the connection queue.")
        return
    except Exception as e:
        if active_youtube_backup is None:
            add_log(f"Pytchat failed for '{stream_name}' ({e}). No backup API configured — stopping listener.")
            with active_streams_lock:
                active_stream_ids.discard(stream_id)
            return
        add_log(f"Pytchat failed for '{stream_name}' ({e}). Switching to Backup API!")
        use_api_fallback = True

    while not stop_flag.is_set():
        stream_name = get_stream_label(stream_id, stream_name)  # v0.3.0.3: pick up renames from /live
        try:
            current_time = time.time()

            # Always consume a pending "retry now" request (set when a channel ID is
            # saved on /live), even if we're not in fallback mode — otherwise a request
            # made while pytchat was healthy would linger and trigger a pointless
            # retry the next time the stream falls back.
            retry_requested = _consume_pytchat_retry_request(stream_id)

            if use_api_fallback and (
                current_time - last_pytchat_retry > PYTCHAT_RETRY_INTERVAL
                or retry_requested
            ):
                add_log(f"Attempting to restore pytchat for '{stream_name}'...")
                last_pytchat_retry = current_time
                try:
                    test_chat = safe_pytchat_create(stream_id)
                    if test_chat.is_alive():
                        chat = test_chat
                        chat_started_at = time.time()
                        use_api_fallback = False
                        next_page_token = None
                        add_log(f"Pytchat restored for '{stream_name}'!")
                        continue
                    add_log(f"Pytchat restore failed for '{stream_name}': replacement chat not alive ({describe_pytchat_stop(test_chat)})")
                except Exception as e:
                    add_log(f"Pytchat restore failed for '{stream_name}': {e}")

            if not use_api_fallback:
                if not chat or not chat.is_alive():
                    # 1) Log WHY the old chat stopped before it gets replaced
                    stop_reason = describe_pytchat_stop(chat)

                    # Rapid-death counter: did this chat die right after being (re)created?
                    if time.time() - chat_started_at < PYTCHAT_RAPID_DEATH_WINDOW:
                        rapid_pytchat_deaths += 1
                    else:
                        rapid_pytchat_deaths = 0  # it lived a while, so this is an isolated death

                    if active_youtube_backup is not None and pytchat_stop_means_ended(chat):
                        rapid_pytchat_deaths = 0
                        # Pytchat itself says the chat data finished. Skip the (likely
                        # futile) restore and let the YouTube API give the final verdict:
                        # liveChatEnded/Disabled/NotFound stops the listener, while a
                        # normal API response means pytchat was wrong and the periodic
                        # pytchat retry will bring it back later.
                        add_log(f"Pytchat stopped for '{stream_name}': {stop_reason}. Verifying with the YouTube API before deciding...")
                        use_api_fallback = True
                        next_page_token = None
                        last_pytchat_retry = time.time()
                        continue

                    # Restore-loop guard: stop restoring a chat that keeps dying instantly
                    if rapid_pytchat_deaths >= PYTCHAT_MAX_RAPID_DEATHS:
                        add_log(f"Pytchat keeps dying right after restore ({rapid_pytchat_deaths}x in a row) for '{stream_name}': {stop_reason}.")
                        rapid_pytchat_deaths = 0
                        if active_youtube_backup is None:
                            add_log(f"🔴 Stream '{stream_name}' ({stream_id}) — no backup API configured to verify. Stopping listener.")
                            break  # Exit the while loop (cleanup below marks it stopped)
                        add_log(f"Verifying '{stream_name}' with the YouTube API before deciding...")
                        use_api_fallback = True
                        next_page_token = None
                        last_pytchat_retry = time.time()
                        continue

                    add_log(f"Pytchat stopped for '{stream_name}': {stop_reason}. Attempting to restore pytchat...")
                    restore_error = None
                    # 2) One restore attempt
                    try:
                        test_chat = safe_pytchat_create(stream_id)
                        if test_chat.is_alive():
                            chat = test_chat
                            chat_started_at = time.time()
                            use_api_fallback = False
                            next_page_token = None
                            add_log(f"Pytchat restored for '{stream_name}'!")
                            continue
                        restore_error = f"replacement chat not alive ({describe_pytchat_stop(test_chat)})"
                    except Exception as theme_master:
                        restore_error = str(theme_master)

                    # 3) Restore failed (exception OR not alive) -> fallback mode
                    if active_youtube_backup is None:
                        add_log(f"Pytchat failed for '{stream_name}': {restore_error}. No backup API configured — stopping listener.")
                        with active_streams_lock:
                            active_stream_ids.discard(stream_id)
                        return
                    add_log(f"Pytchat restore failed for '{stream_name}': {restore_error}. Switching to API fallback...")
                    use_api_fallback = True
                    next_page_token = None
                    last_pytchat_retry = time.time()
                    continue  # Route to the API-fallback branch next pass, not the dead pytchat object below

                for msg_item in chat.get().sync_items():
                    userName = msg_item.author.name
                    userChannelId = getattr(msg_item.author, 'channelId', '')
                    message_text = msg_item.message
                    is_moderator = getattr(msg_item.author, 'isChatModerator', False)
                    is_owner = getattr(msg_item.author, 'isChatOwner', False)

                    last_reply_time = process_command(
                        userName, userChannelId, message_text, liveChatId,
                        last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS, stream_name=stream_name,
                        video_id=stream_id, is_moderator=is_moderator, is_owner=is_owner
                    )
                time.sleep(1)

            else:
                if not next_page_token:
                    response = active_youtube_backup.liveChatMessages().list(
                        liveChatId=liveChatId,
                        part="snippet"
                    ).execute()
                    next_page_token = response.get('nextPageToken')
                    add_log(f"Anchored '{stream_name}' to API live edge.")
                    time.sleep(5)
                    continue

                request_args = {
                    "liveChatId": liveChatId,
                    "part": "snippet,authorDetails",
                    "pageToken": next_page_token
                }

                response = active_youtube_backup.liveChatMessages().list(**request_args).execute()
                next_page_token = response.get('nextPageToken')
                polling_millis = response.get('pollingIntervalMillis', 5000)
                consecutive_api_failures = 0  # Reset on successful poll

                for item in response.get('items', []):
                    userName = item['authorDetails']['displayName']
                    userChannelId = item['authorDetails']['channelId']
                    message_text = item['snippet']['displayMessage']
                    is_moderator = item['authorDetails'].get('isChatModerator', False)
                    is_owner = item['authorDetails'].get('isChatOwner', False)

                    last_reply_time = process_command(
                        userName, userChannelId, message_text, liveChatId,
                        last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS, stream_name=stream_name,
                        video_id=stream_id, is_moderator=is_moderator, is_owner=is_owner
                    )

                time.sleep(max(polling_millis / 1000.0, 10.0))

        except HttpError as e:
            gone_reason = chat_gone_description(e)
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                exhausted_name = _client_name(active_youtube_backup)
                if _mark_quota_exhausted(exhausted_name):
                    add_log(f"{exhausted_name} quota exceeded for '{stream_name}'.")
                next_reader = pick_reader()
                if next_reader is not None:
                    add_log(f"Switching '{stream_name}' to {_client_name(next_reader)} for API fallback...")
                    active_youtube_backup = next_reader
                    next_page_token = None
                else:
                    add_log(f"All API clients are out of quota for '{stream_name}'. Pausing 15 seconds before restoring pytchat one last time...")
                    time.sleep(15)

                    try:
                        test_chat = safe_pytchat_create(stream_id)
                        if test_chat.is_alive():
                            chat = test_chat
                            chat_started_at = time.time()
                            use_api_fallback = False
                            next_page_token = None
                            add_log(f"Pytchat restored for '{stream_name}'! Resuming without waiting for quota reset.")
                            consecutive_api_failures = 0
                            continue
                        else:
                            raise Exception(f"Pytchat stream initialization failed ({describe_pytchat_stop(test_chat)}).")
                    except ConnectionQueueCancelled:
                        break  # removed while queued; the code after the loop logs the stop
                    except Exception as pytchat_error:
                        wait_seconds, utc_offset_label = seconds_until_next_midnight_pt()
                        add_log(f"Pytchat restore failed for '{stream_name}': {pytchat_error}. Pausing until 00:00 PT ({utc_offset_label} UTC).")
                        time.sleep(wait_seconds)
                        active_youtube_backup = pick_reader() or default_youtube_reader
                        next_page_token = None
                        use_api_fallback = False  # Try pytchat first on resume
                        last_pytchat_retry = time.time()
                        add_log(f"Quota reset time reached for '{stream_name}'. Retrying pytchat first...")
                consecutive_api_failures = 0  # Quota errors don't mean the stream ended
            elif gone_reason:
                # YouTube explicitly says the chat is gone -> no need for 3 strikes
                add_log(f"🔴 Stream '{stream_name}' ({stream_id}) — {gone_reason}. Stopping listener.")
                break  # Exit the while loop
            elif e.resp.status == 404:
                # Any other 404 → stream may have ended (3-strike check)
                consecutive_api_failures += 1
                add_log(f"YouTube API 404 for '{stream_name}' (attempt {consecutive_api_failures}/{MAX_CONSECUTIVE_FAILURES}) — stream may have ended.")
                if consecutive_api_failures >= MAX_CONSECUTIVE_FAILURES:
                    add_log(f"🔴 Stream '{stream_name}' ({stream_id}) has ended (3 consecutive 404s). Stopping listener.")
                    break  # Exit the while loop
                time.sleep(10)
            else:
                # Other API errors (5xx, rate limit, etc.) — don't immediately assume stream ended
                consecutive_api_failures += 1
                add_log(f"YouTube API Error for '{stream_name}': {e} (failure {consecutive_api_failures}/{MAX_CONSECUTIVE_FAILURES})")
                if consecutive_api_failures >= MAX_CONSECUTIVE_FAILURES:
                    add_log(f"🔴 Stream '{stream_name}' ({stream_id}) — too many API errors, stopping listener.")
                    break  # Exit the while loop
                time.sleep(10)
        except Exception as e:
            add_log(f"Error in '{stream_name}' chat loop: {e}")
            time.sleep(3)  # Small pause first, in case it's a transient blip

            try:
                test_chat = safe_pytchat_create(stream_id)
                if test_chat.is_alive():
                    chat = test_chat
                    chat_started_at = time.time()
                    use_api_fallback = False
                    next_page_token = None
                    add_log(f"Pytchat restored for '{stream_name}' after chat loop error!")
                    continue
                else:
                    raise Exception(f"Pytchat stream initialization failed ({describe_pytchat_stop(test_chat)}).")
            except Exception as restore_error:
                add_log(f"Pytchat restore failed after chat loop error for '{stream_name}': {restore_error}. Switching to API Fallback...")
                use_api_fallback = True
                next_page_token = None
                last_pytchat_retry = time.time()

    if stop_flag.is_set():
        # Loop exited because /live's Remove button was clicked —
        # stop_stream_listener() already cleaned up active_stream_ids.
        add_log(f"'{stream_name}' ({stream_id}) listener stopped (removed via /live dashboard).")
    else:
        # Loop exited on its own (break — stream ended / too many API
        # errors). Nothing has cleaned up active_stream_ids yet, so /live
        # would otherwise keep showing this as 🟢 Listening forever.
        mark_stream_stopped(stream_id)
        add_log(f"'{stream_name}' ({stream_id}) listener stopped (stream ended). /live now shows it as ⚪ Stopped.")


def main():
    """Load saved streams on startup and start a listener for each."""
    add_log("=" * 60)
    add_log("🤖 YouTube Bot Starting...")
    add_log("=" * 60)

    # Log whenever pytchat has to fall back to a channel ID entered on /live
    # (i.e. ThemeMasterChat's automatic lookups failed for that video).
    pytchat_manual_channel_patch.set_manual_used_callback(
        lambda vid, cid: add_log(f"Automatic channel ID lookup failed for '{vid}' — using the manual channel ID instead.")
    )

    streams = load_streams()

    imported = import_stream_channels_once(streams)
    if imported:
        add_log(f"Imported {imported} channel ID(s) from saved streams into the Select Channel list.")

    if not streams:
        add_log("⚠️  No streams configured yet.")
        add_log("👉 Go to: localhost:10000/live")
        add_log("📝 Add a video ID there to start listening!")
    else:
        add_log(f"✅ Found {len(streams)} saved stream(s). Starting listeners...")

        # Register saved manual channel IDs BEFORE any listener starts, so pytchat's
        # last-resort lookup can use them on the very first connection attempt.
        manual_count = 0
        for video_id, data in streams.items():
            manual_id = data.get('channel_id') if isinstance(data, dict) else None
            if manual_id and pytchat_manual_channel_patch.is_valid_channel_id(manual_id):
                pytchat_manual_channel_patch.set_manual_channel_id(video_id, manual_id)
                manual_count += 1
        if manual_count:
            add_log(f"Registered {manual_count} manual channel ID(s) from saved streams.")

        for video_id, data in streams.items():
            label = data.get('label') or video_id
            start_stream_listener(video_id, label)

    # Keep the main thread alive; the HTTP server and stream listener
    # threads are all daemon threads doing the real work in the background.
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
