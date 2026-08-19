import os
import json
import time
import random
import threading
import urllib.parse
import urllib.request
import pytchat
from http.server import HTTPServer, BaseHTTPRequestHandler
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from auth import Authorize

# --- Render Port Binding (Prevents Timeout) ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
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

    print(f"\nConnected to Live Chat ID: {liveChatId}")
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
        print(f"Bot replied: {message}")
    except Exception as e:
        print(f"Failed to send message: {e}")


def process_command(userName, userChannelId, message_text, liveChatId, last_reply_time, BLOCKED_BOTS, COOLDOWN_SECONDS):
    """Processes commands coming from either pytchat or backup API."""
    clean_name = userName.lower().replace('@', '')
    clean_handle = str(userChannelId).lower().replace('@', '')

    if any(bot in clean_name or bot in clean_handle for bot in BLOCKED_BOTS):
        return last_reply_time

    message_text = message_text.strip()
    print(f"New chat message from {userName}: {message_text}")

    lower_msg = message_text.lower()
    is_command = (
        lower_msg in ["hello", "hi", "hey", "!discord", "!disc", "!random", "!rand"]
        or lower_msg.startswith(("!chatmbr", "!ai"))
    )

    if not is_command:
        return last_reply_time

    current_time = time.time()
    if current_time - last_reply_time < COOLDOWN_SECONDS:
        time_left = int(COOLDOWN_SECONDS - (current_time - last_reply_time))
        print(f"Skipped reply to {userName}: Cooldown active ({time_left}s remaining)")
        return last_reply_time

    if lower_msg in ["hello", "hi", "hey"]:
        sendReplyToLiveChat(liveChatId, f"Hey {userName}! Welcome to the stream!")
        return time.time()

    elif lower_msg in ["!discord", "!disc"]:
        discord_link = "https://discord.gg/9tADYVHc3Y"
        sendReplyToLiveChat(liveChatId, f"Join our discord! {discord_link}")
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
                print(f"Error fetching from ChatMBR API: {e}")
                sendReplyToLiveChat(liveChatId, f"{userName} Unable to reach ChatMBR right now.")
                return time.time()

    return last_reply_time


def main():
    LIVE_STREAM_ID = os.getenv('LIVE_STREAM_ID')

    if not LIVE_STREAM_ID:
        print("\n" + "="*60)
        print("ERROR: LIVE_STREAM_ID environment variable is missing!")
        print("="*60 + "\n")
        raise Exception("LIVE_STREAM_ID environment variable missing.")

    liveChatId = getLiveChatId(LIVE_STREAM_ID)

    BLOCKED_BOTS = {"nightbot", "streamelements", "moobot"}
    COOLDOWN_SECONDS = 10
    last_reply_time = 0

    use_api_fallback = False
    next_page_token = None
    chat = None

    print("\nConnecting pytchat listener (0 quota cost)...")
    try:
        chat = pytchat.create(video_id=LIVE_STREAM_ID)
        if not chat.is_alive():
            raise Exception("Pytchat stream initialization failed.")
        print("Bot is listening for messages via pytchat...")
    except Exception as e:
        print(f"Pytchat connection failed ({e}). Switching to Backup Project API Fallback Mode!")
        use_api_fallback = True

    while True:
        try:
            # --- MODE 1: pytchat ---
            if not use_api_fallback:
                if not chat or not chat.is_alive():
                    print("\nPytchat stream disconnected. Switching to Backup Project API Fallback Mode...")
                    use_api_fallback = True
                    continue

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
                request_args = {"liveChatId": liveChatId, "part": "snippet,authorDetails"}
                if next_page_token:
                    request_args["pageToken"] = next_page_token

                # Reads chat using youtube_backup (Backup project quota)
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

                time.sleep(max(polling_millis / 1000.0, 5.0))

        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                print("CRITICAL: Backup YouTube API Daily Quota Exceeded! Sleeping for 1 hour...")
                time.sleep(3600)
            else:
                print(f"YouTube API Error: {e}")
                time.sleep(10)
        except Exception as e:
            print(f"Error in chat loop ({e}). Switching/staying on Backup API Fallback...")
            use_api_fallback = True
            time.sleep(5)


if __name__ == "__main__":
    main()
