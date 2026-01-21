from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from pymongo import MongoClient

from config import SUPER_ADMINS, MONGO_URI, DB_NAME

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col_users = db["authorized_users"]

def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMINS

async def add_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add authorized user (super admin only)"""
    if not update.effective_chat or update.effective_chat.type != "private":
        return
    
    if not is_super_admin(update.effective_user.id):
        await update.message.reply_text("❌ Sirf super admin is command ko use kar sakta hai.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /adduser <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        col_users.update_one(
            {"_id": user_id},
            {"$set": {"added_by": update.effective_user.id, "timestamp": update.message.date}},
            upsert=True
        )
        await update.message.reply_text(f"✅ User {user_id} ko access de diya gaya.")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def list_users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all authorized users"""
    if not update.effective_chat or update.effective_chat.type != "private":
        return
    
    if not is_super_admin(update.effective_user.id):
        await update.message.reply_text("❌ Sirf super admin is command ko use kar sakta hai.")
        return
    
    users = list(col_users.find({}, {"_id": 1, "added_by": 1, "timestamp": 1}))
    if not users:
        await update.message.reply_text("❌ Koi authorized user nahi hai.")
        return
    
    text = "📋 Authorized Users:\n\n"
    for user in users:
        text += f"• User ID: `{user['_id']}`\n"
        text += f"  Added by: `{user.get('added_by', 'Unknown')}`\n"
        text += f"  Added on: {user.get('timestamp', 'Unknown')}\n\n"
    
    await update.message.reply_text(text, parse_mode="Markdown")

def is_authorized(user_id: int) -> bool:
    """Check if user is authorized"""
    if is_super_admin(user_id):
        return True
    return col_users.find_one({"_id": user_id}) is not None

def get_auth_handlers():
    return [
        CommandHandler("adduser", add_user_cmd),
        CommandHandler("listusers", list_users_cmd),
    ]
