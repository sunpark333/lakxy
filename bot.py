import os
import logging
import threading
import asyncio
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter

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

class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks"""
    
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Telegram Forward Bot is running')
        elif self.path == '/ping':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'pong')
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'healthy')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Disable access logging
        pass

def start_health_server():
    """Start HTTP server for health checks"""
    try:
        # Render provides PORT environment variable
        port = int(os.environ.get('PORT', '8080'))
        server = HTTPServer(('0.0.0.0', port), HealthHandler)
        
        def run_server():
            logger.info(f"Health server started on port {port}")
            print(f"✅ Health check endpoint: http://0.0.0.0:{port}/ping")
            print(f"✅ Root endpoint: http://0.0.0.0:{port}/")
            server.serve_forever()
        
        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()
        return port
    except Exception as e:
        logger.error(f"Failed to start health server: {e}")
        return None

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
/status - Check current job status
/help - Show detailed help
/adduser - Add authorized user (admin only)
/listusers - List authorized users (admin only)

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
• First message in each topic will be pinned automatically
• Maximum 5000000 messages per request
• Failed messages will be skipped
• Multiple forwarding jobs can run simultaneously

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
                'timestamp': datetime.now(timezone.utc)
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
                f"❌ Invalid format. Error: {str(e)}\n"
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
    
    # Check if user has too many active jobs (limit to 3 concurrent jobs)
    active_jobs_count = len(forwarding_manager.get_user_active_jobs(user_id))
    if active_jobs_count >= 3:
        await update.message.reply_text(
            "⚠️ You already have 3 active forwarding jobs.\n"
            "Please wait for one to complete before starting another."
        )
        return
    
    # Start forwarding as a separate task
    try:
        await update.message.reply_text("🔄 Starting forwarding process...")
        
        # Create a unique job ID for this user
        job_id = f"{user_id}_{int(datetime.now(timezone.utc).timestamp())}"
        
        # Start forwarding in background
        task = asyncio.create_task(
            forwarding_manager.process_forward_request(
                update=update,
                context=context,
                request_data=request_info['data'],
                original_message=update.message.reply_to_message,
                job_id=job_id,
                user_id=user_id
            )
        )
        
        # Store the task
        forwarding_manager.user_tasks[user_id] = forwarding_manager.user_tasks.get(user_id, [])
        forwarding_manager.user_tasks[user_id].append({
            'task': task,
            'job_id': job_id,
            'start_time': datetime.now(timezone.utc)
        })
        
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
    
    # Check if user has pending request
    if user_id in pending_requests:
        del pending_requests[user_id]
        await update.message.reply_text("✅ Pending request cancelled.")
        return
    
    # Cancel all user's forwarding tasks
    cancelled_count = forwarding_manager.stop_all_user_jobs(user_id)
    
    if cancelled_count > 0:
        await update.message.reply_text(f"🛑 Cancelled {cancelled_count} active job(s).")
    else:
        await update.message.reply_text("ℹ️ No active forwarding jobs found.")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current status of user's jobs"""
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    
    if not is_authorized(user_id):
        await update.message.reply_text("❌ You are not authorized.")
        return
    
    from pymongo import MongoClient
    from config import MONGO_URI, DB_NAME
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col_jobs = db["forward_jobs"]
    
    # Get active jobs from database
    user_jobs = list(col_jobs.find({
        "user_id": user_id,
        "status": {"$in": ["started", "processing"]}
    }).sort("start_time", -1).limit(5))
    
    # Get user's active tasks from manager
    active_tasks = forwarding_manager.get_user_active_jobs(user_id)
    
    if not user_jobs and not active_tasks:
        await update.message.reply_text("ℹ️ No active jobs found.")
        return
    
    status_text = "🔄 *Your Active Jobs*\n\n"
    
    if user_jobs:
        for job in user_jobs:
            elapsed = (datetime.now(timezone.utc) - job.get('start_time', datetime.now(timezone.utc))).seconds
            progress = job.get('progress', 0)
            
            # Calculate estimated time remaining if we have progress
            eta = ""
            if progress > 0 and elapsed > 0:
                total_time_estimated = (elapsed * 100) / progress
                remaining = total_time_estimated - elapsed
                if remaining > 0:
                    eta = f" (~{int(remaining//60)} min {int(remaining%60)} sec remaining)"
            
            status_text += f"• *Job ID:* `{job.get('_id', 'N/A')}`\n"
            status_text += f"  *Status:* {job.get('status', 'Unknown')}\n"
            status_text += f"  *Progress:* {progress}%\n"
            status_text += f"  *Running:* {elapsed} seconds{eta}\n"
            
            if job.get('current_message'):
                status_text += f"  *Current:* {job.get('current_message')}\n"
            
            status_text += "\n"
    
    # Show active task count
    status_text += f"*Active tasks in memory:* {len(active_tasks)}/3"
    
    # Show pending request if exists
    if user_id in pending_requests:
        status_text += "\n\n📝 *You have a pending request waiting for /forward*"
    
    await update.message.reply_text(status_text, parse_mode=ParseMode.MARKDOWN)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(f"Update {update} caused error {context.error}")
    
    # Handle rate limiting
    if isinstance(context.error, RetryAfter):
        retry_after = context.error.retry_after
        logger.warning(f"Rate limited. Retry after {retry_after} seconds")
        
        if update and update.effective_chat:
            try:
                await update.effective_message.reply_text(
                    f"⚠️ Rate limit reached. Please wait {retry_after} seconds before trying again."
                )
            except:
                pass
        return
    
    # Handle other errors
    if update and update.effective_chat:
        try:
            await update.effective_message.reply_text(
                "❌ An error occurred. Please try again later."
            )
        except:
            pass

def main():
    """Main function to start the bot"""
    # Start health check server (for Render)
    port = start_health_server()
    
    if port:
        logger.info(f"Health server started on port {port}")
    else:
        logger.warning("Health server failed to start")
    
    # Create application
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add command handlers (order matters - add specific commands first)
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("forward", forward_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    
    # Add auth handlers
    for handler in get_auth_handlers():
        app.add_handler(handler)
    
    # Add message handler for forward requests (must be after command handlers)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, 
        handle_message
    ))
    
    # Add error handler
    app.add_error_handler(error_handler)
    
    # Start bot
    logger.info("Bot is starting...")
    print("=" * 50)
    print("🤖 Telegram Forward Bot Started!")
    print("✅ Features: Multi-tasking, Auto-pin, Fixed Commands")
    print("✅ Fixed: Cancel command, Removed /stats, Rate limiting handled")
    print(f"📍 PORT: {port if port else 'Not started'}")
    print("=" * 50)
    
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Bot failed to start: {e}")

if __name__ == "__main__":
    main()
