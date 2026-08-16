import os
import json
import time
import random
import threading
import urllib.parse
import urllib.request
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

# Building the youtube object:
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
    Sends a text message to the specified live chat.
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
    # Read stream ID from Render Environment Variables instead of input()
    LIVE_STREAM_ID = os.getenv('LIVE_STREAM_ID')

    if not LIVE_STREAM_ID:
        print("\n" + "="*60)
        print("ERROR: LIVE_STREAM_ID environment variable is missing!")
        print("Go to Render -> Environment -> Add 'LIVE_STREAM_ID'")
        print("="*60 + "\n")
        raise Exception("LIVE_STREAM_ID environment variable missing.")

    liveChatId = getLiveChatId(LIVE_STREAM_ID)

    # Blocklist for bot usernames (lowercase)
    BLOCKED_BOTS = {"nightbot", "streamelements", "moobot"}

    # Track processed messages using message IDs to prevent duplicate replies
    processed_message_ids = set()
    next_page_token = None

    print("\nBot is running and listening for stream messages...")

    while True:
        try:
            # Request authorDetails along with snippet to get usernames for free
            kwargs = {
                "liveChatId": liveChatId,
                "part": "snippet,authorDetails"
            }
            if next_page_token:
                kwargs["pageToken"] = next_page_token

            # Fetch messages
            liveChat = youtube.liveChatMessages().list(**kwargs)
            response = liveChat.execute()

            # YouTube specifies how long to wait before polling again
            next_page_token = response.get('nextPageToken')
            polling_interval = response.get('pollingIntervalMillis', 2000) / 1000.0

            allMessages = response.get('items', [])

            for msg_item in allMessages:
                msg_id = msg_item['id']

                # Process only unread messages
                if msg_id not in processed_message_ids:
                    processed_message_ids.add(msg_id)

                    # Extracted directly from authorDetails (0 extra API quota cost!)
                    author_details = msg_item.get('authorDetails', {})
                    userName = author_details.get('displayName', 'Viewer')

                    # Skip processing if the message is from a blocked bot/user
                    if userName.lower() in BLOCKED_BOTS:
                        print(f"Ignored message from blocked bot: {userName}")
                        continue

                    snippet = msg_item['snippet']
                    message_text = snippet['textMessageDetails']['messageText'].strip()

                    print(f'New chat message from {userName}: {message_text}')

                    # Command triggers
                    lower_msg = message_text.lower()

                    if lower_msg in ["hello", "hi", "hey"]:
                        sendReplyToLiveChat(
                            liveChatId,
                            f"Hey {userName}! Welcome to the stream!"
                        )

                    elif lower_msg in ["!discord", "!disc"]:
                        discord_link = "https://discord.gg/9tADYVHc3Y"
                        sendReplyToLiveChat(
                            liveChatId,
                            f"Join our discord! {discord_link}"
                        )

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

                    elif lower_msg.startswith("!chatmbr"):
                        query = message_text[8:].strip()
                        if not query:
                            sendReplyToLiveChat(
                                liveChatId,
                                f"@{userName} Please provide a query! Usage: !chatmbr <question>"
                            )
                        else:
                            try:
                                encoded_query = urllib.parse.quote(query)
                                api_url = f"https://chatmbr-bot.vercel.app/api/chat?query={encoded_query}"
                                req = urllib.request.Request(
                                    api_url, headers={"User-Agent": "Mozilla/5.0"}
                                )
                                with urllib.request.urlopen(req, timeout=8) as api_response:
                                    api_reply = api_response.read().decode("utf-8").strip()

                                # Truncate reply to avoid YouTube's 200-character chat limit
                                if len(api_reply) > 200:
                                    api_reply = api_reply[:197] + "..."

                                sendReplyToLiveChat(liveChatId, api_reply)
                            except Exception as e:
                                print(f"Error fetching from MBR API: {e}")
                                sendReplyToLiveChat(
                                    liveChatId, f"@{userName} Unable to reach MBR right now."
                                )

            # Prevent memory build-up
            if len(processed_message_ids) > 1000:
                processed_message_ids.clear()

            time.sleep(polling_interval)

        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                print("CRITICAL: YouTube API Daily Quota Exceeded! Sleeping for 1 hour to prevent crash loops...")
                time.sleep(3600)
            else:
                print(f"YouTube API Error: {e}")
                time.sleep(10)
        except Exception as e:
            print(f"Error reading chat: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
