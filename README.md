# Muzza Telegram Music Bot

A Telegram music bot with YouTube, Telegram, and local playback, queue, seek, and thumbnail features.

## Features
- Play music from YouTube, Telegram, or local files
- Queue management
- Seek/seekback for all sources
- MongoDB state and cache
- Custom thumbnails with views/channel/title

## Deploy on Railway
1. Fork this repo and push your code to GitHub
2. Click "Deploy on Railway" or connect your repo at https://railway.app/
3. Set environment variables in Railway:
   - `BOT_TOKEN` — Telegram bot token
   - `API_ID` — Telegram API ID
   - `API_HASH` — Telegram API hash
   - `MONGO_URL` — MongoDB connection string
   - (optional) `OWNER_ID`, `LOG_CHAT_ID`, etc.
4. Railway will auto-install dependencies from `requirements.txt` and run `python bot.py`

## Local run
```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
python bot.py
```

## Notes
- ffmpeg and ffprobe must be available in PATH (Railway has them preinstalled)
- All fonts needed for thumbnails must be in the `fonts/` folder
- MongoDB must be accessible from Railway

---
MIT License
