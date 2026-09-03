import os
import json
import signal
import time
import random
import threading
import urllib.parse
import urllib.request
import urllib.error
import base64
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from auth import Authorize

# --- Apply pytchat oEmbed patch BEFORE importing/using pytchat ---
import pytchat_oembed_patch
pytchat_oembed_patch.apply_patch()
import pytchat
# ------------------------------------------------------------------


def safe_pytchat_create(video_id):
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

# --- Global Log Storage (Holds last 100 entries) ---
chat_logs = deque(maxlen=100)

# --- Live Streams Storage ---
STREAMS_FILE = "streams_config.json"

# --- NeilCoins Storage ---
COINS_FILE = "coins_config.json"
DEFAULT_STARTING_COINS = 100
RESETCOINS_GRANT = 25
RESETCOINS_MAX_USES = 3

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


# --- NeilCoins helpers ---

def load_coins():
    return load_json_with_fallback(COINS_FILE, "coin balance(s)")


def save_coins(coins):
    save_json_with_backup(COINS_FILE, coins, f"{len(coins)} user(s)")


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


def make_user_key(userName, userChannelId):
    """Prefer the stable channel ID; fall back to lowercased name if absent."""
    if userChannelId:
        return str(userChannelId)
    return userName.strip().lower()




def add_log(message):
    """Formats logs with a 24-hour UTC timestamp and pushes to memory and stdout."""
    utc_timestamp = time.strftime("[%H:%M:%S UTC]", time.gmtime())
    entry = f"{utc_timestamp} {message}"
    chat_logs.append(entry)
    print(entry, flush=True)


# --- Global thread-safe tracking of which video IDs currently have an
# active listener thread running, plus a "stop" flag per stream so the
# Remove button can cleanly signal a listener thread to shut down. ---
active_streams_lock = threading.Lock()
active_stream_ids = set()      # video_ids with a running listener thread
stop_flags = {}                # video_id -> threading.Event(), set() to stop that listener


def start_stream_listener(video_id, label):
    """Start a listener thread for a video ID if one isn't already running."""
    with active_streams_lock:
        if video_id in active_stream_ids:
            add_log(f"'{label}' ({video_id}) already has an active listener — skipping duplicate start.")
            return False
        active_stream_ids.add(video_id)
        stop_flags[video_id] = threading.Event()

    thread = threading.Thread(
        target=listen_to_stream,
        args=(video_id, label, stop_flags[video_id]),
        daemon=True
    )
    thread.start()
    return True


def stop_stream_listener(video_id):
    """Signal a running listener thread for this video_id to stop."""
    with active_streams_lock:
        flag = stop_flags.get(video_id)
        if flag:
            flag.set()
        active_stream_ids.discard(video_id)
        stop_flags.pop(video_id, None)


# --- Render Port Binding & HTTP Server ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/logs":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()

            log_entries = "".join([f"<li>{item}</li>" for item in reversed(chat_logs)])

            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Live Chat Logs</title>
                <meta http-equiv="refresh" content="5">
                <style>
                    body {{ background-color: #0d1117; color: #58a6ff; font-family: monospace; padding: 20px; }}
                    h2 {{ color: #ffffff; border-bottom: 1px solid #30363d; padding-bottom: 10px; }}
                    ul {{ list-style: none; padding: 0; }}
                    li {{ padding: 6px 0; border-bottom: 1px solid #21262d; }}
                </style>
            </head>
            <body>
                <h2>🤖 YouTube Bot Live Logs (UTC)</h2>
                <ul>{log_entries or "<li>No chat activity logged yet.</li>"}</ul>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode("utf-8"))

        elif self.path == "/commands":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()

            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Bot Commands</title>
                <style>
                    body { background-color: #0d1117; color: #c9d1d9; font-family: monospace; padding: 20px; }
                    h2 { color: #58a6ff; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
                    table { width: 100%; border-collapse: collapse; margin-top: 15px; }
                    th, td { text-align: left; padding: 10px; border-bottom: 1px solid #21262d; }
                    th { color: #58a6ff; }
                    code { background: #161b22; padding: 3px 6px; border-radius: 4px; color: #79c0ff; }
                </style>
            </head>
            <body>
                <h2>📜 Available Bot Commands</h2>
                <p style="color: #8b949e; font-size: 14px;">New users start with <b>100 NeilCoins</b> automatically.</p>
                <table>
                    <tr><th>Command</th><th>Description</th></tr>
                    <tr><td><code>hello / hi / hey</code></td><td>Greets the user.</td></tr>
                    <tr><td><code>!revertical</code></td><td>dawg WHO said "revertical" 😭✌️</td></tr>
                    <tr><td><code>!random / !rand</code></td><td>Tells a random joke.</td></tr>
                    <tr><td><code>!chatmbr &lt;query&gt; / !ai &lt;query&gt;</code></td><td>Asks ChatMBR a question.</td></tr>
                    <tr><td><code>E</code></td><td>E</td></tr>
                    <tr><td><code>!commands / !help</code></td><td>Displays this command list page.</td></tr>
                    <tr><td><code>!coins</code></td><td>Shows your current NeilCoins balance.</td></tr>
                    <tr><td><code>!gamble &lt;number&gt;</code></td><td>Bets that many NeilCoins — 50/50 chance to double it or lose it.</td></tr>
                    <tr><td><code>!giftpoint @Username &lt;points&gt;</code></td><td>Gifts NeilCoins to another user (can't gift yourself).</td></tr>
                    <tr><td><code>!resetcoins</code></td><td>Grants +25 NeilCoins, but only if your balance is exactly 0. Max 3 uses ever.</td></tr>
                    <tr><td><code>!leaderboard</code></td><td>Shows the top 5 users by NeilCoins.</td></tr>
                </table>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode("utf-8"))

        elif self.path == "/live":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()

            streams = load_streams()
            with active_streams_lock:
                running_ids = set(active_stream_ids)

            stream_rows = ""
            for video_id, data in streams.items():
                label = data.get('label', '') or 'N/A'
                status = "🟢 Listening" if video_id in running_ids else "⚪ Stopped"
                stream_rows += f"""
                <tr>
                    <td><code>{video_id}</code></td>
                    <td>{label}</td>
                    <td>{status}</td>
                    <td>
                        <button onclick="removeStream('{video_id}')">Remove</button>
                    </td>
                </tr>
                """

            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Live Streams</title>
                <style>
                    body {{ background-color: #0d1117; color: #c9d1d9; font-family: sans-serif; padding: 20px; }}
                    h2 {{ color: #58a6ff; border-bottom: 1px solid #30363d; padding-bottom: 10px; }}
                    .container {{ max-width: 900px; margin: 0 auto; }}
                    table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                    th, td {{ text-align: left; padding: 12px; border-bottom: 1px solid #21262d; }}
                    th {{ color: #58a6ff; background: #161b22; }}
                    button {{ padding: 8px 16px; margin: 4px 4px 4px 0; cursor: pointer; border: 1px solid #30363d; border-radius: 6px; background: #161b22; color: #58a6ff; }}
                    button:hover {{ background: #0d1117; border-color: #58a6ff; }}
                    .add-box {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 30px; }}
                    .add-box input {{ padding: 8px; background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; border-radius: 4px; margin-right: 8px; margin-bottom: 8px; box-sizing: border-box; }}
                    .add-btn {{ background: #28a745; color: white; font-weight: bold; }}
                    .add-btn:hover {{ background: #218838; }}
                    .backup-box {{ background: #161b22; border: 1px solid #d29922; border-radius: 8px; padding: 20px; margin-bottom: 30px; }}
                    .backup-box textarea {{ width: 100%; min-height: 70px; padding: 8px; background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; border-radius: 4px; box-sizing: border-box; font-family: monospace; font-size: 12px; margin-bottom: 8px; }}
                    .copy-btn {{ background: #d29922; color: #0d1117; font-weight: bold; }}
                    .copy-btn:hover {{ background: #bb8009; }}
                    .restore-btn {{ background: #1f6feb; color: white; font-weight: bold; }}
                    .restore-btn:hover {{ background: #1a5cc4; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="backup-box">
                        <h3 style="color: #d29922; margin-top: 0;">💾 Backup / Restore</h3>
                        <p style="font-size: 13px; color: #8b949e; margin-top: 0;">
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
                        <h3 style="color: #58a6ff; margin-top: 0;">➕ Add Live Stream</h3>
                        <p style="font-size: 13px; color: #8b949e; margin-top: 0;">
                            Paste a YouTube video ID (livestream or premiere) to start listening immediately.
                            Supports multiple streams at once.
                        </p>
                        <input type="text" id="videoIdInput" placeholder="Video ID (e.g. BblhFWoDyWk)" style="width: 220px;">
                        <input type="text" id="labelInput" placeholder="Label (optional)" style="width: 180px;">
                        <button class="add-btn" onclick="addStream()">▶ Start Listening</button>
                    </div>

                    <h2>📺 Active Streams</h2>
                    <table>
                        <tr>
                            <th>Video ID</th>
                            <th>Label</th>
                            <th>Status</th>
                            <th>Actions</th>
                        </tr>
                        {stream_rows or "<tr><td colspan='4'>No streams added yet.</td></tr>"}
                    </table>
                </div>

                <script>
                    function addStream() {{
                        const videoId = document.getElementById('videoIdInput').value.trim();
                        const label = document.getElementById('labelInput').value.trim();

                        if (!videoId) {{
                            alert('Please enter a Video ID!');
                            return;
                        }}

                        fetch('/api/streams', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{video_id: videoId, label: label}})
                        }}).then(r => r.json()).then(data => {{
                            alert(data.message);
                            location.reload();
                        }}).catch(e => alert('Error: ' + e));
                    }}

                    function removeStream(videoId) {{
                        if (!confirm('Remove and stop listening to ' + videoId + '?')) return;
                        fetch('/api/streams/' + videoId, {{method: 'DELETE'}})
                            .then(r => r.json())
                            .then(data => {{
                                alert(data.message);
                                location.reload();
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
                            fetch('/api/streams', {{
                                method: 'POST',
                                headers: {{'Content-Type': 'application/json'}},
                                body: JSON.stringify({{video_id: videoId, label: label}})
                            }}).finally(() => {{
                                completed++;
                                if (completed === videoIds.length) {{
                                    alert(`Restored ${{videoIds.length}} stream(s)!`);
                                    location.reload();
                                }}
                            }});
                        }});
                    }}
                </script>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode("utf-8"))

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
                label = data.get('label', '').strip()

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()

                if not video_id:
                    self.wfile.write(json.dumps({"message": "Video ID is required!"}).encode("utf-8"))
                    return

                streams = load_streams()
                streams[video_id] = {"label": label}
                save_streams(streams)

                started = start_stream_listener(video_id, label or video_id)
                if started:
                    self.wfile.write(json.dumps({
                        "message": f"Started listening to '{label or video_id}'! Check /logs to confirm connection."
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

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "message": f"Stopped and removed {video_id}."
                }).encode("utf-8"))

        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot is active!")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        self.do_GET()

    def do_DELETE(self):
        self.do_GET()


def start_health_check_server():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    server.serve_forever()

threading.Thread(target=start_health_check_server, daemon=True).start()
# -----------------------------------------------

# 1. Main Project YouTube Client (Used for sending replies)
credentials_main = Authorize('client_secret.json', 'token.json')
youtube_main = build('youtube', 'v3', credentials=credentials_main)

# 2. Backup 1 Project YouTube Client (First fallback reader)
credentials_backup1 = Authorize('backup_client_secret.json', 'backup_token.json')
youtube_backup1 = build('youtube', 'v3', credentials=credentials_backup1)

# 3. Backup 2 Project YouTube Client (Second fallback reader)
credentials_backup2 = Authorize('backup2_client_secret.json', 'backup2_token.json')
youtube_backup2 = build('youtube', 'v3', credentials=credentials_backup2)


def getLiveChatId(LIVE_STREAM_ID):
    stream = youtube_main.videos().list(
        part="liveStreamingDetails",
        id=LIVE_STREAM_ID,
    )
    response = stream.execute()

    items = response.get('items', [])
    if not items:
        raise Exception(f"Live stream with ID '{LIVE_STREAM_ID}' not found.")

    live_details = items[0].get('liveStreamingDetails', {})
    liveChatId = live_details.get('activeLiveChatId')

    if not liveChatId:
        raise Exception(f"No active live chat found for stream '{LIVE_STREAM_ID}'. Make sure the stream is live!")

    add_log(f"Connected to Live Chat ID: {liveChatId}")
    return liveChatId


def sendReplyToLiveChat(liveChatId, message):
    """Sends messages using youtube_main (Main project quota)."""
    try:
        reply = youtube_main.liveChatMessages().insert(
            part="snippet",
            body={
                "snippet": {
                    "liveChatId": liveChatId,
                    "type": "textMessageEvent",
                    "textMessageDetails": {
                        "messageText": message,
                    }
                }
            }
        )
        reply.execute()
        add_log(f"Bot replied: {message}")
    except Exception as e:
        add_log(f"Failed to send message: {e}")


def process_command(userName, userChannelId, message_text, liveChatId, last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS):
    """Processes commands coming from either pytchat or backup API."""
    clean_name = userName.lower().replace('@', '')
    clean_handle = str(userChannelId).lower().replace('@', '')

    if any(bot in clean_name or bot in clean_handle for bot in BLOCKED_BOTS):
        add_log(f"Ignored message from blocked bot: {userName}")
        return last_reply_time

    message_text = message_text.strip()
    add_log(f"New chat message from {userName}: {message_text}")

    lower_msg = message_text.lower()
    is_command = (
        lower_msg in ["hello", "hi", "hey", "!revertical", "!random", "!rand", "!commands", "!help", "e",
                      "!coins", "!resetcoins", "!leaderboard"]
        or lower_msg.startswith(("!chatmbr", "!ai", "!gamble", "!giftpoint"))
    )

    if not is_command:
        return last_reply_time

    current_time = time.time()
    if current_time - last_reply_time < COOLDOWN_SECONDS:
        time_left = int(COOLDOWN_SECONDS - (current_time - last_reply_time))
        add_log(f"Skipped reply to {userName}: Cooldown active ({time_left}s remaining)")
        return last_reply_time

    if lower_msg in ["hello", "hi", "hey"]:
        sendReplyToLiveChat(liveChatId, f"Hey {userName}! Welcome to the stream!")
        return time.time()

    elif lower_msg in ["!revertical"]:
        sendReplyToLiveChat(liveChatId, f"dawg WHO said “revertical” 😭✌️")
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
        sendReplyToLiveChat(liveChatId, joke)
        return time.time()

    elif lower_msg in ["!commands", "!help"]:
        cmd_url = "https://thememasteros.pythonanywhere.com/commands"
        sendReplyToLiveChat(liveChatId, f"{userName} -> The bot commands are available at {cmd_url}")
        return time.time()

    elif lower_msg in ["e"]:
        sendReplyToLiveChat(liveChatId, f"E")
        return time.time()

    elif lower_msg.startswith(("!chatmbr", "!ai")):
        query = message_text[8:].strip() if lower_msg.startswith("!chatmbr") else message_text[3:].strip()

        if not query:
            sendReplyToLiveChat(liveChatId, f"{userName} Please provide a query! Usage: !chatmbr <question> or !ai <question>")
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

                sendReplyToLiveChat(liveChatId, api_reply)
                return time.time()
            except Exception as e:
                add_log(f"Error fetching from ChatMBR API: {e}")
                sendReplyToLiveChat(liveChatId, f"{userName} Unable to reach ChatMBR right now.")
                return time.time()

    elif lower_msg == "!coins":
        coins = load_coins()
        user_key = make_user_key(userName, userChannelId)
        record = get_user_record(coins, user_key, display_name=userName)
        save_coins(coins)  # Persist in case this created a brand-new user record
        sendReplyToLiveChat(liveChatId, f"{userName} has {record['balance']} NeilCoins.")
        return time.time()

    elif lower_msg.startswith("!gamble"):
        parts = message_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip().lstrip('-').isdigit():
            sendReplyToLiveChat(liveChatId, f"{userName} Usage: !gamble <number> (e.g. !gamble 50)")
            return time.time()

        bet = int(parts[1].strip())
        if bet <= 0:
            sendReplyToLiveChat(liveChatId, f"{userName} Bet must be a positive number!")
            return time.time()

        coins = load_coins()
        user_key = make_user_key(userName, userChannelId)
        record = get_user_record(coins, user_key, display_name=userName)

        if bet > record['balance']:
            sendReplyToLiveChat(liveChatId, f"{userName} You only have {record['balance']} NeilCoins — can't bet {bet}!")
            save_coins(coins)
            return time.time()

        won = random.choice([True, False])
        if won:
            record['balance'] += bet
            sendReplyToLiveChat(liveChatId, f"🎉 {userName} gambled {bet} and WON! New balance: {record['balance']} NeilCoins.")
        else:
            record['balance'] -= bet
            sendReplyToLiveChat(liveChatId, f"💀 {userName} gambled {bet} and LOST! New balance: {record['balance']} NeilCoins.")

        save_coins(coins)
        return time.time()

    elif lower_msg.startswith("!giftpoint"):
        parts = message_text.split(maxsplit=2)
        if len(parts) < 3 or not parts[2].strip().isdigit():
            sendReplyToLiveChat(liveChatId, f"{userName} Usage: !giftpoint @Username <points>")
            return time.time()

        target_raw = parts[1].strip()
        gift_amount = int(parts[2].strip())

        if gift_amount <= 0:
            sendReplyToLiveChat(liveChatId, f"{userName} Gift amount must be a positive number!")
            return time.time()

        # Clean `@` from both target and sender for a clean string check
        clean_target = target_raw.lstrip('@').strip().lower()
        clean_sender = userName.lstrip('@').strip().lower()

        if clean_target == clean_sender:
            sendReplyToLiveChat(liveChatId, f"{userName} You can't giftpoint yourself!")
            return time.time()

        coins = load_coins()
        sender_key = make_user_key(userName, userChannelId)
        sender_record = get_user_record(coins, sender_key, display_name=userName)

        if gift_amount > sender_record['balance']:
            sendReplyToLiveChat(liveChatId, f"{userName} You only have {sender_record['balance']} NeilCoins — can't gift {gift_amount}!")
            save_coins(coins)
            return time.time()

        # Search existing JSON records by stripping `@` from stored names to ensure a hit
        target_key = None
        for key, rec in coins.items():
            stored_name_clean = rec.get('name', '').lstrip('@').strip().lower()
            if stored_name_clean == clean_target:
                target_key = key
                break

        # If user isn't found in the database, refuse to create a dummy account and exit!
        if target_key is None:
            sendReplyToLiveChat(liveChatId, f"{userName} Couldn't find a user named '{target_raw}'. They must use !gamble or !coins first!")
            save_coins(coins)
            return time.time()

        # Credit the found existing account
        target_record = coins[target_key]

        sender_record['balance'] -= gift_amount
        target_record['balance'] += gift_amount
        save_coins(coins)

        sendReplyToLiveChat(liveChatId, f"🎁 {userName} gifted {gift_amount} NeilCoins to {target_record['name']}!")
        return time.time()

    elif lower_msg == "!resetcoins":
        coins = load_coins()
        user_key = make_user_key(userName, userChannelId)
        record = get_user_record(coins, user_key, display_name=userName)

        if record['balance'] != 0:
            sendReplyToLiveChat(liveChatId, f"{userName} !resetcoins only works when your balance is exactly 0 (you have {record['balance']}).")
            save_coins(coins)
            return time.time()

        if record.get('resetcoins_uses', 0) >= RESETCOINS_MAX_USES:
            sendReplyToLiveChat(liveChatId, f"{userName} You've already used !resetcoins {RESETCOINS_MAX_USES} times — no more resets for you!")
            save_coins(coins)
            return time.time()

        record['balance'] += RESETCOINS_GRANT
        record['resetcoins_uses'] = record.get('resetcoins_uses', 0) + 1
        save_coins(coins)

        uses_left = RESETCOINS_MAX_USES - record['resetcoins_uses']
        sendReplyToLiveChat(liveChatId, f"{userName} got +{RESETCOINS_GRANT} NeilCoins! Balance: {record['balance']} ({uses_left} reset(s) left).")
        return time.time()

    elif lower_msg == "!leaderboard":
        coins = load_coins()
        if not coins:
            sendReplyToLiveChat(liveChatId, "No NeilCoins data yet — be the first to !gamble or check !coins!")
            return time.time()

        top_5 = sorted(coins.values(), key=lambda r: r.get('balance', 0), reverse=True)[:5]
        leaderboard_str = " | ".join(
            f"{i+1}. {r['name']}: {r['balance']}" for i, r in enumerate(top_5)
        )
        sendReplyToLiveChat(liveChatId, f"🏆 NeilCoins Leaderboard — {leaderboard_str}")
        return time.time()

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
    active_youtube_backup = youtube_backup1

    liveChatId = None
    while liveChatId is None:
        if stop_flag.is_set():
            add_log(f"'{stream_name}' ({stream_id}) stopped before chat became available.")
            return
        try:
            liveChatId = getLiveChatId(stream_id)
            add_log(f"Started listening to '{stream_name}' ({stream_id})")
        except Exception as e:
            add_log(f"Error connecting to '{stream_name}' ({stream_id}): {e}")
            add_log(f"Chat not available yet for '{stream_name}'. Will retry in 60 seconds...")
            for _ in range(60):
                if stop_flag.is_set():
                    return
                time.sleep(1)

    BLOCKED_BOTS = {"nightbot", "streamelements", "moobot", "streamlabs", "thememasterbot", "nabatchatbot"}
    COOLDOWN_SECONDS = 10
    last_reply_time = 0

    use_api_fallback = False
    next_page_token = None
    chat = None

    last_pytchat_retry = 0
    PYTCHAT_RETRY_INTERVAL = 180
    pytchat_failed_attempts = 0
    
    # Stream-end detection: count consecutive API failures
    # If we get 3+ in a row, the stream has likely ended
    consecutive_api_failures = 0
    MAX_CONSECUTIVE_FAILURES = 3

    add_log(f"Connecting pytchat listener for '{stream_name}' (0 quota cost)...")
    try:
        chat = safe_pytchat_create(stream_id)
        if not chat.is_alive():
            raise Exception("Pytchat stream initialization failed.")
        add_log(f"Listening to '{stream_name}' via pytchat...")
    except Exception as e:
        add_log(f"Pytchat failed for '{stream_name}' ({e}). Switching to Backup API!")
        use_api_fallback = True

    while not stop_flag.is_set():
        try:
            current_time = time.time()

            if use_api_fallback and (current_time - last_pytchat_retry > PYTCHAT_RETRY_INTERVAL):
                add_log(f"Attempting to restore pytchat for '{stream_name}'...")
                last_pytchat_retry = current_time
                try:
                    test_chat = safe_pytchat_create(stream_id)
                    if test_chat.is_alive():
                        chat = test_chat
                        use_api_fallback = False
                        pytchat_failed_attempts = 0
                        next_page_token = None
                        add_log(f"Pytchat restored for '{stream_name}'!")
                        continue
                except Exception as e:
                    add_log(f"Pytchat restore failed for '{stream_name}': {e}")

            if not use_api_fallback:
                if not chat or not chat.is_alive():
                    pytchat_failed_attempts += 1
                    if pytchat_failed_attempts >= 3:
                        add_log(f"Switching '{stream_name}' to API Fallback...")
                        use_api_fallback = True
                        pytchat_failed_attempts = 0
                        next_page_token = None
                        last_pytchat_retry = time.time()
                    time.sleep(2)
                    continue

                pytchat_failed_attempts = 0

                for msg_item in chat.get().sync_items():
                    userName = msg_item.author.name
                    userChannelId = getattr(msg_item.author, 'channelId', '')
                    message_text = msg_item.message

                    last_reply_time = process_command(
                        userName, userChannelId, message_text, liveChatId,
                        last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS
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

                    last_reply_time = process_command(
                        userName, userChannelId, message_text, liveChatId,
                        last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS
                    )

                time.sleep(max(polling_millis / 1000.0, 10.0))

        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                if active_youtube_backup is youtube_backup1:
                    add_log(f"Backup 1 quota exceeded for '{stream_name}'. Switching to Backup 2...")
                    active_youtube_backup = youtube_backup2
                    next_page_token = None
                else:
                    add_log(f"Backup 2 quota exceeded for '{stream_name}'. Pausing 15 min...")
                    time.sleep(900)
                    active_youtube_backup = youtube_backup1
                    next_page_token = None
                consecutive_api_failures = 0  # Quota errors don't mean the stream ended
            elif e.resp.status == 404:
                # 404 = live chat no longer exists → stream ended
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
            time.sleep(5)

    add_log(f"'{stream_name}' ({stream_id}) listener stopped (removed via /live dashboard).")


def main():
    """Load saved streams on startup and start a listener for each."""
    add_log("=" * 60)
    add_log("🤖 YouTube Bot Starting...")
    add_log("=" * 60)

    streams = load_streams()

    if not streams:
        add_log("⚠️  No streams configured yet.")
        add_log("👉 Go to: https://youtube-livestream-chatbot.onrender.com/live")
        add_log("📝 Add a video ID there to start listening!")
    else:
        add_log(f"✅ Found {len(streams)} saved stream(s). Starting listeners...")
        for video_id, data in streams.items():
            label = data.get('label') or video_id
            start_stream_listener(video_id, label)

    # Keep the main thread alive; the HTTP server and stream listener
    # threads are all daemon threads doing the real work in the background.
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
