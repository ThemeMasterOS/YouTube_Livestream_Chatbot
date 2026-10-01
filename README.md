# YouTube Livestream Chatbot

A Python bot that connects to YouTube Live Stream chat, listens for commands, and replies in real time. Supports multiple streams at once, each running independently in its own thread.

This is the bot behind **Theme Master Bot**, running live on [@ThemeMasterEXE](https://www.youtube.com/@ThemeMasterEXE)'s streams.

Free to use, fork, and modify.

---

## Features

- Listens to one or more YouTube live chats simultaneously
- Custom chat commands (`!commands`, `!help`, `!leaderboard`, and more — fully customizable in `bot.py`)
- `!ai` / `!chatmbr` command powered by an AI chat API (optional, requires separate setup — see `chat.js`)
- Primary connection via [pytchat](https://github.com/taizan-hokuto/pytchat) (0 quota cost), with optional YouTube Data API fallback if pytchat fails
- Web dashboard at `/live` to add, label, and manage streams
- Automatic token refresh — no need to re-login every time you restart the bot

---

## Quick Start

1. **Download** this repo as a ZIP (Code → Download ZIP) and extract it somewhere on your computer.
2. **Install Python** (3.10 or newer) if you don't already have it — [python.org/downloads](https://www.python.org/downloads/). Make sure to check **"Add Python to PATH"** during install.
3. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```
4. **Get your own YouTube API credentials** from [Google Cloud Console](https://console.cloud.google.com/):
   - Enable the **YouTube Data API v3**
   - Create an **OAuth client ID** (Desktop app type)
   - Download it and rename it to `client_secret.json`, placed in the same folder as `bot.py`
5. **Generate your token:**
   ```
   python token_generator.py
   ```
   Choose option `1` (Main) and follow the browser login prompt. This creates `token.json`.
6. **Run the bot:**
   ```
   python bot.py
   ```
7. Open `http://localhost:10000/live` (or whatever port is shown in your terminal) to manage streams from the dashboard.

For a more detailed, beginner-friendly walkthrough, see `How_To_Make_Your_Own_Bot.txt` in this repo.

---

## Optional: Backup API Fallback

By default, the bot only needs **one** set of credentials (`client_secret.json` + `token.json`) to run. This is completely optional — if you don't configure them, the bot runs fine on pytchat alone.

Backups exist so that if pytchat has issues, the bot can fall back to the YouTube Data API using a **separate** Google Cloud project's quota, instead of eating into your main project's quota:

| Slot | Files needed | Required? |
|---|---|---|
| Main | `client_secret.json`, `token.json` | ✅ Yes |
| Backup 1 | `backup_client_secret.json`, `backup_token.json` | ❌ Optional |
| Backup 2 | `backup2_client_secret.json`, `backup2_token.json` | ❌ Optional |

To set up a backup, create a **second** (and/or third) OAuth client in Google Cloud Console — can be a separate project if you want fully separate quota — rename the downloaded file accordingly, and run `token_generator.py` again, choosing option `2` or `3`.

If any backup's files are missing, the bot detects this automatically at startup, logs that it's skipping that fallback, and continues running normally with whatever is configured.

---

## Deploying (e.g. Render)

`auth.py` also supports reading credentials from environment variables (`TOKEN`, `TOKEN_BACKUP`, `TOKEN_BACKUP2`) as a fallback if the token files aren't present on disk — useful for hosting platforms where you can't easily upload files but can set secret environment variables. Each variable should contain the full contents of the matching `token.json` file as text.

---

## Files

| File | Purpose |
|---|---|
| `bot.py` | Main bot — chat listening, commands, dashboard |
| `auth.py` | Handles Google OAuth login and token refresh |
| `token_generator.py` | Standalone script to generate `token.json` and backups |
| `pytchat_oembed_patch.py` | Patch required for pytchat to keep working |
| `requirements.txt` | Python dependencies |
| `How_To_Make_Your_Own_Bot.txt` | Full beginner setup guide |

---

## License

This project is licensed under the [MIT License](LICENSE) — free to use, modify, and distribute.

---

## Questions / Issues?

Message **@ThemeMasterEXE** on YouTube for help setting this up.
