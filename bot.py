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

# Start health check server in background thread
threading.Thread(target=start_health_check_server, daemon=True).start()
# -----------------------------------------------

# Authorize returns Credentials directly
credentials = Authorize('client_secret.json')

# Building the youtube object for sending replies
youtube = build('youtube', 'v3', credentials=credentials)


def getLiveChatId(LIVE_STREAM_ID):
    """
    Takes a live stream ID as input, and returns the active live chat ID.
    """
    stream = youtube.videos().list(
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
    """
    Sends a text message to the specified live chat (Costs 50 quota units).
    """
    try:
        reply = youtube.liveChatMessages().insert(
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


def main():
    LIVE_STREAM_ID = os.getenv('LIVE_STREAM_ID')

    if not LIVE_STREAM_ID:
        print("\n" + "="*60)
        print("ERROR: LIVE_STREAM_ID environment variable is missing!")
        print("Go to Render -> Environment -> Add 'LIVE_STREAM_ID'")
        print("="*60 + "\n")
        raise Exception("LIVE_STREAM_ID environment variable missing.")

    # Get chat ID for sending API replies
    liveChatId = getLiveChatId(LIVE_STREAM_ID)

    BLOCKED_BOTS = {"nightbot", "streamelements", "moobot"}
    COOLDOWN_SECONDS = 10
    last_reply_time = 0

    print("\nConnecting pytchat listener (0 quota cost)...")
    chat = pytchat.create(video_id=LIVE_STREAM_ID)
    print("Bot is listening for messages...")

    while chat.is_alive():
        try:
            for msg_item in chat.get().sync_items():
                userName = msg_item.author.name
                userChannelId = getattr(msg_item.author, 'channelId', '')

                clean_name = userName.lower().replace('@', '')
                clean_handle = str(userChannelId).lower().replace('@', '')

                # Skip blocked bots
                if any(bot in clean_name or bot in clean_handle for bot in BLOCKED_BOTS):
                    continue

                message_text = msg_item.message.strip()
                print(f"New chat message from {userName}: {message_text}")

                lower_msg = message_text.lower()
                is_command = (
                    lower_msg in ["hello", "hi", "hey", "!discord", "!disc", "!random", "!rand"]
                    or lower_msg.startswith("!chatmbr", "!ai")
                )

                if is_command:
                    current_time = time.time()

                    # Check global cooldown
                    if current_time - last_reply_time < COOLDOWN_SECONDS:
                        time_left = int(COOLDOWN_SECONDS - (current_time - last_reply_time))
                        print(f"Skipped reply to {userName}: Cooldown active ({time_left}s remaining)")
                        continue

                    if lower_msg in ["hello", "hi", "hey"]:
                        sendReplyToLiveChat(
                            liveChatId,
                            f"Hey {userName}! Welcome to the stream!"
                        )
                        last_reply_time = time.time()

                    elif lower_msg in ["!discord", "!disc"]:
                        discord_link = "https://discord.gg/9tADYVHc3Y"
                        sendReplyToLiveChat(
                            liveChatId,
                            f"Join our discord! {discord_link}"
                        )
                        last_reply_time = time.time()

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
                        last_reply_time = time.time()

                    elif lower_msg.startswith("!chatmbr", "!ai"):
                        query = message_text[8:].strip()
                        if not query:
                            sendReplyToLiveChat(
                                liveChatId,
                                f"{userName} Please provide a query! Usage: !chatmbr <question> or !ai <question>"
                            )
                            last_reply_time = time.time()
                        else:
                            try:
                                encoded_query = urllib.parse.quote(query)
                                api_url = f"https://chatmbr-bot.vercel.app/api/chat?query={encoded_query}"
                                req = urllib.request.Request(
                                    api_url, headers={"User-Agent": "Mozilla/5.0"}
                                )
                                with urllib.request.urlopen(req, timeout=8) as api_response:
                                    api_reply = api_response.read().decode("utf-8").strip()

                                if len(api_reply) > 200:
                                    api_reply = api_reply[:197] + "..."

                                sendReplyToLiveChat(liveChatId, api_reply)
                                last_reply_time = time.time()
                            except Exception as e:
                                print(f"Error fetching from ChatMBR API: {e}")
                                sendReplyToLiveChat(
                                    liveChatId, f"{userName} Unable to reach ChatMBR right now."
                                )
                                last_reply_time = time.time()

            time.sleep(1)

        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                print("CRITICAL: YouTube API Daily Quota Exceeded! Sleeping for 1 hour...")
                time.sleep(3600)
            else:
                print(f"YouTube API Error: {e}")
                time.sleep(10)
        except Exception as e:
            print(f"Error reading pytchat stream: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
