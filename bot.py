import os
import json
import time
import random
from googleapiclient.discovery import build
from auth import Authorize

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


def getUserName(userId):
    """
    Takes a userId and returns the channel title.
    """
    try:
        channelDetails = youtube.channels().list(
            part="snippet",
            id=userId,
        )
        response = channelDetails.execute()
        return response['items'][0]['snippet']['title']
    except Exception:
        return "Viewer"


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

    # Track processed messages using message IDs to prevent duplicate replies
    processed_message_ids = set()
    next_page_token = None

    print("\nBot is running and listening for stream messages...")

    while True:
        try:
            # Prepare API request
            kwargs = {
                "liveChatId": liveChatId,
                "part": "snippet"
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

                    snippet = msg_item['snippet']
                    userId = snippet['authorChannelId']
                    message_text = snippet['textMessageDetails']['messageText'].strip()

                    userName = getUserName(userId)
                    print(f'New chat message from {userName}: {message_text}')

                    # Command triggers
                    lower_msg = message_text.lower()

                    if lower_msg in ["hello", "hi", "hey"]:
                        sendReplyToLiveChat(
                            liveChatId,
                            f"Hey {userName}! Welcome to the stream!"
                        )

                    elif lower_msg in ["!discord", "!disc"]:
                        discord_link = "https://discord.gg/"
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

            # Prevent memory build-up
            if len(processed_message_ids) > 1000:
                processed_message_ids.clear()

            time.sleep(polling_interval)

        except Exception as e:
            print(f"Error reading chat: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
