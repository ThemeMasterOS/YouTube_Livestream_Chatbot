import os
import json
import time
import random
import threading
import urllib.parse
import urllib.request
import pytchat
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from auth import Authorize

# --- Global Log Storage (Holds last 100 entries) ---
chat_logs = deque(maxlen=100)

def add_log(message):
    """Formats logs with a 24-hour UTC timestamp and pushes to memory and stdout."""
    utc_timestamp = time.strftime("[%H:%M:%S UTC]", time.gmtime())
    entry = f"{utc_timestamp} {message}"
    chat_logs.append(entry)
    print(entry, flush=True)

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
                <table>
                    <tr><th>Command</th><th>Description</th></tr>
                    <tr><td><code>hello / hi / hey</code></td><td>Greets the user.</td></tr>
                    <tr><td><code>!revertical</code></td><td>dawg WHO said “revertical” 😭✌️</td></tr>
                    <tr><td><code>!random / !rand</code></td><td>Tells a random joke.</td></tr>
                    <tr><td><code>!chatmbr &lt;query&gt; / !ai &lt;query&gt;</code></td><td>Asks ChatMBR a question.</td></tr>
                    <tr><td><code>!commands / !help</code></td><td>Displays this command list page.</td></tr>
                </table>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode("utf-8"))

        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot is active!")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def start_health_check_server():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    server.serve_forever()

threading.Thread(target=start_health_check_server, daemon=True).start()
# -----------------------------------------------

# 1. Main Project YouTube Client (Used for sending replies)
credentials_main = Authorize('client_secret.json', 'token.json')
youtube_main = build('youtube', 'v3', credentials=credentials_main)

# 2. Backup Project YouTube Client (Used strictly for fallback chat reading)
credentials_backup = Authorize('backup_client_secret.json', 'backup_token.json')
youtube_backup = build('youtube', 'v3', credentials=credentials_backup)


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
        return last_reply_time

    message_text = message_text.strip()
    add_log(f"New chat message from {userName}: {message_text}")

    lower_msg = message_text.lower()
    is_command = (
        lower_msg in ["hello", "hi", "hey", "!revertical", "!random", "!rand", "!commands", "!help"]
        or lower_msg.startswith(("!chatmbr", "!ai"))
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
        dad_jokes = [
            "Why do fathers take an extra pair of socks when they go golfing? In case they get a hole in one!",
            "Dear Math, grow up and solve your own problems.",
            "What has more letters than the alphabet? The post office!",
            "Why are elevator jokes so classic and good? They work on so many levels!",
            "What do you call a fake noodle? An impasta!",
            "What do you call a belt made out of watches? A waist of time!",
            "Why did the scarecrow win an award? Because he was outstanding in his field!",
            "Why don't skeletons ever go trick or treating? Because they have no body to go with!",
            "What's brown and sticky? A stick!"
        ]
        joke = random.choice(dad_jokes)
        sendReplyToLiveChat(liveChatId, joke)
        return time.time()

    elif lower_msg in ["!commands", "!help"]:
        cmd_url = "https://youtube-livestream-chatbot.onrender.com/commands"
        sendReplyToLiveChat(liveChatId, f"{userName} -> The bot commands are available at {cmd_url}")
        return time.time()

    elif lower_msg.startswith(("!chatmbr", "!ai")):
        query = message_text[8:].strip() if lower_msg.startswith("!chatmbr") else message_text[3:].strip()

        if not query:
            sendReplyToLiveChat(liveChatId, f"{userName} Please provide a query! Usage: !chatmbr <question> or !ai <question>")
            return time.time()
        else:
            try:
                encoded_query = urllib.parse.quote(query)
                api_url = f"https://chatmbr-bot.vercel.app/api/chat?query={encoded_query}"
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

    return last_reply_time


def main():
    LIVE_STREAM_ID = os.getenv('LIVE_STREAM_ID')

    if not LIVE_STREAM_ID:
        add_log("ERROR: LIVE_STREAM_ID environment variable is missing!")
        raise Exception("LIVE_STREAM_ID environment variable missing.")

    liveChatId = getLiveChatId(LIVE_STREAM_ID)

    BLOCKED_BOTS = {"nightbot", "streamelements", "moobot"}
    COOLDOWN_SECONDS = 10
    last_reply_time = 0

    use_api_fallback = False
    next_page_token = None
    chat = None

    last_pytchat_retry = 0
    PYTCHAT_RETRY_INTERVAL = 180  # Retries pytchat every 3 minutes
    pytchat_failed_attempts = 0   # Track strikes before triggering fallback

    add_log("Connecting pytchat listener (0 quota cost)...")
    try:
        chat = pytchat.create(video_id=LIVE_STREAM_ID)
        if not chat.is_alive():
            raise Exception("Pytchat stream initialization failed.")
        add_log("Bot is listening for messages via pytchat...")
    except Exception as e:
        add_log(f"Pytchat connection failed ({e}). Switching to Backup Project API Fallback Mode!")
        use_api_fallback = True

    while True:
        try:
            current_time = time.time()

            # --- AUTO-RECOVERY: Periodically try restoring pytchat if in fallback mode ---
            if use_api_fallback and (current_time - last_pytchat_retry > PYTCHAT_RETRY_INTERVAL):
                add_log("Attempting to restore pytchat connection...")
                last_pytchat_retry = current_time
                try:
                    test_chat = pytchat.create(video_id=LIVE_STREAM_ID)
                    if test_chat.is_alive():
                        chat = test_chat
                        use_api_fallback = False
                        pytchat_failed_attempts = 0
                        next_page_token = None
                        add_log("Successfully reconnected pytchat! Exiting API Fallback Mode (0 quota active).")
                        continue
                except Exception as e:
                    add_log(f"Pytchat reconnection attempt failed ({e}). Remaining on Backup API.")

            # --- MODE 1: pytchat (0 Quota) ---
            if not use_api_fallback:
                if not chat or not chat.is_alive():
                    pytchat_failed_attempts += 1
                    add_log(f"Pytchat connection check failed ({pytchat_failed_attempts}/3)...")

                    if pytchat_failed_attempts >= 3:
                        add_log("Pytchat failed 3 consecutive times. Switching to Backup API Fallback...")
                        use_api_fallback = True
                        pytchat_failed_attempts = 0
                        next_page_token = None  # Force anchor on next API fallback start
                        last_pytchat_retry = time.time()

                    time.sleep(2)
                    continue

                # Reset strikes on a healthy stream connection
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

            # --- MODE 2: Backup YouTube API Fallback ---
            else:
                # STEP 1: Skip historical messages on first request by grabbing initial token
                if not next_page_token:
                    response = youtube_backup.liveChatMessages().list(
                        liveChatId=liveChatId,
                        part="snippet"
                    ).execute()
                    next_page_token = response.get('nextPageToken')
                    add_log("Anchored YouTube API to live edge (skipped historical chat history).")
                    time.sleep(5)
                    continue

                # STEP 2: Pull only NEW incoming live chat messages
                request_args = {
                    "liveChatId": liveChatId,
                    "part": "snippet,authorDetails",
                    "pageToken": next_page_token
                }

                response = youtube_backup.liveChatMessages().list(**request_args).execute()
                next_page_token = response.get('nextPageToken')
                polling_millis = response.get('pollingIntervalMillis', 5000)

                for item in response.get('items', []):
                    userName = item['authorDetails']['displayName']
                    userChannelId = item['authorDetails']['channelId']
                    message_text = item['snippet']['displayMessage']

                    last_reply_time = process_command(
                        userName, userChannelId, message_text, liveChatId,
                        last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS
                    )

                # Force polling to at least 10 seconds to save quota
                time.sleep(max(polling_millis / 1000.0, 10.0))

        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                add_log("CRITICAL: Backup YouTube API Quota Exceeded! Sleeping 15 mins before retrying pytchat...")
                time.sleep(900)
                last_pytchat_retry = 0  # Force immediate pytchat check on wake
                next_page_token = None
            else:
                add_log(f"YouTube API Error: {e}")
                time.sleep(10)
        except Exception as e:
            add_log(f"Error in chat loop ({e}). Staying on Backup API Fallback...")
            use_api_fallback = True
            time.sleep(5)


if __name__ == "__main__":
    main()
