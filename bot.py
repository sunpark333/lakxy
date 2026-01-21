import os
import logging
import json
import threading
from datetime import datetime
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

# Bot status tracking
bot_status = {
    "start_time": datetime.utcnow(),
    "total_requests": 0,
    "active_jobs": 0
}

class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for health checks and monitoring"""
    
    def do_GET(self):
        if self.path == '/health':
            self._handle_health()
        elif self.path == '/status':
            self._handle_status()
        elif self.path == '/':
            self._handle_root()
        elif self.path == '/ping':
            self._handle_ping()
        elif self.path == '/uptime':
            self._handle_uptime()
        else:
            self._handle_404()
    
    def _handle_health(self):
        """Comprehensive health check for UptimeRobot"""
        try:
            uptime = (datetime.utcnow() - bot_status["start_time"]).total_seconds()
            
            health_data = {
                "status": "healthy",
                "service": "telegram-forward-bot",
                "timestamp": datetime.utcnow().isoformat(),
                "uptime_seconds": uptime,
                "total_requests": bot_status["total_requests"],
                "active_jobs": len(forwarding_manager.active_jobs),
                "pending_requests": len(pending_requests),
                "bot_status": "running"
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(health_data, indent=2).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            error_data = {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
            self.wfile.write(json.dumps(error_data).encode())
    
    def _handle_status(self):
        """Detailed status endpoint"""
        uptime = datetime.utcnow() - bot_status["start_time"]
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        status_data = {
            "service": "Telegram Forward Bot",
            "status": "🟢 Running",
            "started_at": bot_status["start_time"].isoformat(),
            "uptime": f"{days}d {hours}h {minutes}m {seconds}s",
            "uptime_seconds": uptime.total_seconds(),
            "statistics": {
                "total_requests": bot_status["total_requests"],
                "active_jobs": len(forwarding_manager.active_jobs),
                "pending_requests": len(pending_requests),
                "super_admins": len(SUPER_ADMINS),
                "bot_token_set": bool(BOT_TOKEN and BOT_TOKEN != "YOUR_BOT_TOKEN_HERE")
            },
            "endpoints": {
                "/health": "Comprehensive health check",
                "/status": "Detailed status information",
                "/ping": "Simple ping response",
                "/uptime": "Uptime information"
            }
        }
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(status_data, indent=2).encode())
    
    def _handle_ping(self):
        """Simple ping endpoint - returns plain text"""
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'pong')
    
    def _handle_uptime(self):
        """Uptime information"""
        uptime = datetime.utcnow() - bot_status["start_time"]
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        uptime_data = {
            "uptime": {
                "days": days,
                "hours": hours,
                "minutes": minutes,
                "seconds": seconds,
                "total_seconds": uptime.total_seconds()
            },
            "start_time": bot_status["start_time"].isoformat(),
            "current_time": datetime.utcnow().isoformat()
        }
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(uptime_data, indent=2).encode())
    
    def _handle_root(self):
        """Root endpoint with HTML info page"""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Telegram Forward Bot</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
                .status {{ padding: 10px; border-radius: 5px; margin: 10px 0; }}
                .healthy {{ background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }}
                .info {{ background-color: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }}
                .endpoints {{ margin-top: 20px; }}
                .endpoint {{ padding: 5px; background: #f8f9fa; margin: 5px 0; border-left: 4px solid #007bff; }}
                h1 {{ color: #333; }}
                a {{ color: #007bff; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            <h1>🤖 Telegram Forward Bot</h1>
            
            <div class="status healthy">
                <strong>Status:</strong> 🟢 Running
            </div>
            
            <div class="status info">
                <strong>Uptime:</strong> {(datetime.utcnow() - bot_status["start_time"]).days} days, 
                {divmod((datetime.utcnow() - bot_status["start_time"]).seconds, 3600)[0]} hours, 
                {divmod(divmod((datetime.utcnow() - bot_status["start_time"]).seconds, 3600)[1], 60)[0]} minutes
            </div>
            
            <h2>📊 Statistics</h2>
            <ul>
                <li>Total Requests: {bot_status["total_requests"]}</li>
                <li>Active Jobs: {len(forwarding_manager.active_jobs)}</li>
                <li>Pending Requests: {len(pending_requests)}</li>
                <li>Super Admins: {len(SUPER_ADMINS)}</li>
            </ul>
            
            <div class="endpoints">
                <h2>🔗 Available Endpoints</h2>
                <div class="endpoint"><a href="/health">/health</a> - Comprehensive health check (JSON)</div>
                <div class="endpoint"><a href="/status">/status</a> - Detailed status information (JSON)</div>
                <div class="endpoint"><a href="/ping">/ping</a> - Simple ping response (text)</div>
                <div class="endpoint"><a href="/uptime">/uptime</a> - Uptime information (JSON)</div>
            </div>
            
            <h2>📝 For UptimeRobot</h2>
            <p>Use any of these endpoints for monitoring:</p>
            <ul>
                <li><code>https://your-render-url.onrender.com/health</code></li>
                <li><code>https://your-render-url.onrender.com/ping</code></li>
            </ul>
            
            <hr>
            <p><small>Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</small></p>
        </body>
        </html>
        """
        
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())
    
    def _handle_404(self):
        """Handle 404 errors"""
        self.send_response(404)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        error_data = {
            "error": "Endpoint not found",
            "available_endpoints": [
                "/",
                "/health",
                "/status",
                "/ping",
                "/uptime"
            ]
        }
        self.wfile.write(json.dumps(error_data).encode())
    
    def log_message(self, format, *args):
        # Disable access logging to reduce noise
        pass

def start_health_server():
    """Start a simple HTTP server for health checks"""
    try:
        port = int(os.getenv("PORT", "8080"))
        server = HTTPServer(('0.0.0.0', port), HealthHandler)
        
        def run_server():
            logger.info(f"Health check server started on port {port}")
            server.serve_forever()
        
        # Start server in a separate thread
        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()
        return True
    except Exception as e:
        logger.error(f"Failed to start health server: {e}")
        return False

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
• Maximum 5000000 messages per request
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
    
    # Track request
    bot_status["total_requests"] += 1
    
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
    
    # Track request
    bot_status["total_requests"] += 1
    
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
    
    # Track request
    bot_status["total_requests"] += 1
    
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
    
    # Track request
    bot_status["total_requests"] += 1
    
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

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bot status command"""
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    
    if not is_authorized(user_id):
        await update.message.reply_text("❌ You are not authorized.")
        return
    
    uptime = datetime.utcnow() - bot_status["start_time"]
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    status_text = f"""
🤖 *Bot Status Report*

*Uptime:* {days}d {hours}h {minutes}m {seconds}s
*Started:* {bot_status["start_time"].strftime('%Y-%m-%d %H:%M:%S UTC')}

*Statistics:*
• Total Requests: {bot_status["total_requests"]}
• Active Jobs: {len(forwarding_manager.active_jobs)}
• Pending Requests: {len(pending_requests)}
• Super Admins: {len(SUPER_ADMINS)}

*HTTP Endpoints:*
• `/health` - Health check
• `/status` - Detailed status
• `/ping` - Simple ping
• `/uptime` - Uptime info

*Bot is:* 🟢 Running
"""
    
    await update.message.reply_text(status_text, parse_mode=ParseMode.MARKDOWN)

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
    # Start health check server (for Render)
    health_server_started = start_health_server()
    
    if health_server_started:
        logger.info("Health check server started successfully")
    else:
        logger.warning("Health check server failed to start")
    
    # Create application
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add command handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("forward", forward_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("status", status_cmd))  # New status command
    
    # Add auth handlers
    for handler in get_auth_handlers():
        app.add_handler(handler)
    
    # Add message handler for forward requests
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message))
    
    # Add error handler
    app.add_error_handler(error_handler)
    
    # Start bot
    logger.info("Bot is starting...")
    print("=" * 60)
    print("🤖 TELEGRAM FORWARD BOT")
    print("=" * 60)
    print(f"Health check endpoint: http://0.0.0.0:{os.getenv('PORT', '8080')}/health")
    print(f"Status endpoint: http://0.0.0.0:{os.getenv('PORT', '8080')}/status")
    print(f"Simple ping: http://0.0.0.0:{os.getenv('PORT', '8080')}/ping")
    print("=" * 60)
    print("For UptimeRobot monitoring, use:")
    print(f"https://your-render-url.onrender.com/ping")
    print("or")
    print(f"https://your-render-url.onrender.com/health")
    print("=" * 60)
    
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
