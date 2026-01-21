import os
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)
from telegram.constants import ParseMode

from config import BOT_TOKEN, SUPER_ADMINS
from auth import get_auth_handlers, is_authorized
from forwarding import forwarding_manager
from utils import parse_forward_request

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Store forward requests temporarily
pending_requests = {}

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    welcome_text = """
🤖 *Telegram Forward Bot*

*How to use:*

1️⃣ Send me a message in this format:
https://t.me/c/3586558422/1641
https://t.me/c/3586558422/26787
-1003586558422
'old word' 'new word'
'another' 'replacement'
    
2️⃣ Reply to that message with `/forward` command

*Format Explained:*
• Line 1: Start message link
• Line 2: End message link  
• Line 3: Target group ID
• Line 4+: Word replacements (optional)

*Available Commands:*
/start - Show this help
/forward - Start forwarding (reply to formatted message)
/cancel - Cancel ongoing forwarding
/stats - Show forwarding statistics
/help - Show detailed help

⚠️ *Note:* You need to be authorized to use this bot.
"""
    
    await update.message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detailed help command"""
    help_text = """
📖 *Detailed Help Guide*

*Forwarding Process:*

1. *Prepare your request:*
    https://t.me/c/CHAT_ID/START_MSG_ID
https://t.me/c/CHAT_ID/END_MSG_ID
TARGET_GROUP_ID
'word to replace' 'new word'
'another word' 'replacement'
    
2. *Send the request* to me as a text message

3. *Reply* to that message with `/forward`

*Examples:*

*Basic forwarding:*
    https://t.me/c/1234567890/100
https://t.me/c/1234567890/200
-1009876543210
    
*With replacements:*
    https://t.me/c/1234567890/100
https://t.me/c/1234567890/200
-1009876543210
'old.com' 'new.com'
'@olduser' '@newuser'
    
*Important Notes:*
• Bot must be admin in target group
• Target group must be a supergroup
• Forum topics will be created automatically from "Topic:" in captions
• Maximum 5000 messages per request
• Failed messages will be skipped

*Troubleshooting:*
• Make sure links are valid
• Check bot admin permissions
• Verify target group ID is correct
"""
    
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages"""
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    
    # Check authorization
    if not is_authorized(user_id):
        await update.message.reply_text(
            "❌ You are not authorized to use this bot.\n"
            "Please contact admin to get access."
        )
        return
    
    # Store message for potential forwarding
    message_text = update.message.text
    
    # Check if it looks like a forward request
    if message_text and 't.me' in message_text and len(message_text.split('\n')) >= 3:
        try:
            # Try to parse the request
            request_data = parse_forward_request(message_text)
            
            # Store in pending requests
            pending_requests[user_id] = {
                'data': request_data,
                'message_id': update.message.message_id,
                'timestamp': datetime.utcnow()
            }
            
            # Send confirmation
            response = f"""
✅ *Forward Request Received*

*Parsed Information:*
• Start Link: `{request_data['start_link']}`
• End Link: `{request_data['end_link']}`
• Target Group: `{request_data['target_group']}`
• Replacements: {len(request_data['replacements'])} pairs

To start forwarding, reply to this message with `/forward`

To cancel, use `/cancel`
"""
            
            await update.message.reply_text(
                response,
                parse_mode=ParseMode.MARKDOWN,
                reply_to_message_id=update.message.message_id
            )
            
        except Exception as e:
            logger.error(f"Error parsing request: {e}")
            await update.message.reply_text(
                "❌ Invalid format. Please check the format and try again.\n"
                "Use /help to see the correct format."
            )

async def forward_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /forward command"""
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    
    # Check authorization
    if not is_authorized(user_id):
        await update.message.reply_text("❌ You are not authorized to use this bot.")
        return
    
    # Check if replying to a message
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Please reply to a forward request message with /forward\n\n"
            "Example:\n"
            "1. Send me the forward request (4 lines)\n"
            "2. Reply to that message with /forward"
        )
        return
    
    # Check if there's a pending request
    if user_id not in pending_requests:
        await update.message.reply_text(
            "❌ No pending forward request found.\n"
            "Please send a forward request first, then reply to it with /forward"
        )
        return
    
    # Get the request data
    request_info = pending_requests[user_id]
    
    # Check if replying to correct message
    if update.message.reply_to_message.message_id != request_info['message_id']:
        await update.message.reply_text(
            "❌ Please reply to your original forward request message."
        )
        return
    
    # Check if already processing
    if forwarding_manager.active_jobs.get(user_id):
        await update.message.reply_text("⚠️ You already have an active forwarding job.")
        return
    
    # Start forwarding
    try:
        await update.message.reply_text("🔄 Starting forwarding process...")
        
        await forwarding_manager.process_forward_request(
            update=update,
            context=context,
            request_data=request_info['data'],
            original_message=update.message.reply_to_message
        )
        
        # Clear pending request
        if user_id in pending_requests:
            del pending_requests[user_id]
            
    except Exception as e:
        logger.error(f"Error in forward_cmd: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /cancel command"""
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    
    # Cancel forwarding if active
    if forwarding_manager.active_jobs.get(user_id):
        forwarding_manager.active_jobs[user_id] = False
        await update.message.reply_text("🛑 Forwarding cancelled.")
    else:
        # Clear pending request
        if user_id in pending_requests:
            del pending_requests[user_id]
            await update.message.reply_text("🗑️ Pending request cleared.")
        else:
            await update.message.reply_text("ℹ️ No active job or pending request found.")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics"""
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    
    if not is_authorized(user_id):
        await update.message.reply_text("❌ You are not authorized.")
        return
    
    # Get user's stats from database
    from pymongo import MongoClient
    from config import MONGO_URI, DB_NAME
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col_stats = db["forward_stats"]
    
    user_stats = list(col_stats.find({"user_id": user_id}).sort("timestamp", -1).limit(5))
    
    if not user_stats:
        await update.message.reply_text("📊 No statistics available yet.")
        return
    
    stats_text = "📊 *Your Forwarding Statistics*\n\n"
    
    for stat in user_stats:
        stats_text += f"• *Date:* {stat['timestamp'].strftime('%Y-%m-%d %H:%M')}\n"
        stats_text += f"  Messages: {stat.get('successful', 0)}✅ / {stat.get('failed', 0)}❌\n"
        stats_text += f"  Target: `{stat.get('target_chat', 'N/A')}`\n"
        stats_text += f"  Range: {stat.get('message_range', 'N/A')}\n\n"
    
    total_success = sum(s.get('successful', 0) for s in user_stats)
    total_failed = sum(s.get('failed', 0) for s in user_stats)
    
    stats_text += f"*Totals:* {total_success}✅ / {total_failed}❌"
    
    await update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(f"Update {update} caused error {context.error}")
    
    if update and update.effective_chat:
        try:
            await update.effective_message.reply_text(
                "❌ An error occurred. Please try again later."
            )
        except:
            pass

def main():
    """Main function to start the bot"""
    # Create application
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add command handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("forward", forward_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    
    # Add auth handlers
    for handler in get_auth_handlers():
        app.add_handler(handler)
    
    # Add message handler for forward requests
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message))
    
    # Add error handler
    app.add_error_handler(error_handler)
    
    # Start bot
    logger.info("Bot is starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
