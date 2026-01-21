import os

BOT_TOKEN = os.getenv("BOT_TOKEN") or "YOUR_BOT_TOKEN_HERE"

# yahan apna Telegram user id daalo (super admin)
SUPER_ADMINS = [int(id) for id in os.getenv("SUPER_ADMINS", "").split(",") if id]

# Log channel for temporary messages
LOG_CHANNEL = os.getenv("LOG_CHANNEL", "")

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "telegram_forward_bot"

# Bot settings
MAX_REPLACEMENTS = 5000000
MAX_SYNC_MESSAGES = 5000000
# Rate limiting settings
MAX_CONCURRENT_JOBS_PER_USER = 3  # Maximum concurrent jobs per user
MIN_DELAY_BETWEEN_MESSAGES = 2.0  # Minimum seconds between messages
MAX_DELAY_BETWEEN_MESSAGES = 5.0  # Maximum seconds during errors
