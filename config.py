import os

BOT_TOKEN = os.getenv("BOT_TOKEN") or "YOUR_BOT_TOKEN_HERE"

# yahan apna Telegram user id daalo (super admin)
SUPER_ADMINS = [int(id) for id in os.getenv("SUPER_ADMINS", "").split(",") if id]

# Log channel for temporary messages
LOG_CHANNEL = os.getenv("LOG_CHANNEL", "-1001234567890")

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "telegram_forward_bot"

# Bot settings
MAX_REPLACEMENTS = 5000000
MAX_SYNC_MESSAGES = 5000000
