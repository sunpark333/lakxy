import asyncio
import re
from datetime import datetime, timezone
from telegram import Update, Message
from telegram.ext import ContextTypes
from telegram.error import RetryAfter
import logging

from config import MONGO_URI, DB_NAME
from pymongo import MongoClient

logger = logging.getLogger(__name__)

class ForwardingManager:
    def __init__(self):
        self.active_jobs = {}
        self.user_tasks = {}  # Store user tasks for cancellation
        self.cancelled_jobs = set()
        
        # MongoDB setup
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        self.col_jobs = db["forward_jobs"]
        self.col_stats = db["forward_stats"]
    
    def stop_all_user_jobs(self, user_id: int) -> int:
        """Stop all jobs for a user and return count of cancelled jobs"""
        cancelled_count = 0
        
        # Cancel tasks in memory
        if user_id in self.user_tasks:
            for task_info in self.user_tasks[user_id]:
                if not task_info['task'].done():
                    task_info['task'].cancel()
                    cancelled_count += 1
            
            # Clear user tasks
            self.user_tasks[user_id] = []
        
        # Mark jobs as cancelled in database
        self.col_jobs.update_many(
            {"user_id": user_id, "status": {"$in": ["started", "processing"]}},
            {"$set": {"status": "cancelled", "end_time": datetime.now(timezone.utc)}}
        )
        
        # Add to cancelled set
        for job_id in list(self.active_jobs.keys()):
            if str(user_id) in job_id:
                self.cancelled_jobs.add(job_id)
                if job_id in self.active_jobs:
                    del self.active_jobs[job_id]
        
        return cancelled_count
    
    def get_user_active_jobs(self, user_id: int) -> list:
        """Get active jobs for a user"""
        active_jobs = []
        
        # Check tasks in memory
        if user_id in self.user_tasks:
            for task_info in self.user_tasks[user_id]:
                if not task_info['task'].done():
                    active_jobs.append(task_info)
        
        return active_jobs
    
    async def process_forward_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                      request_data: dict, original_message: Message, 
                                      job_id: str, user_id: int):
        """Process forwarding request with rate limiting handling"""
        
        try:
            # Store job in database
            job_data = {
                "_id": job_id,
                "user_id": user_id,
                "status": "started",
                "start_time": datetime.now(timezone.utc),
                "progress": 0,
                "request_data": request_data
            }
            self.col_jobs.insert_one(job_data)
            
            # Extract data from request
            start_link = request_data['start_link']
            end_link = request_data['end_link']
            target_group = request_data['target_group']
            replacements = request_data['replacements']
            
            # Extract chat_id and message_ids from links
            start_chat_id, start_msg_id = self._extract_from_link(start_link)
            end_chat_id, end_msg_id = self._extract_from_link(end_link)
            
            if start_chat_id != end_chat_id:
                await update.message.reply_text("❌ Start and end links must be from the same chat.")
                return
            
            source_chat = start_chat_id
            
            # Calculate total messages
            total_messages = end_msg_id - start_msg_id + 1
            
            if total_messages > 5000000:
                await update.message.reply_text("❌ Maximum 5000000 messages allowed per request.")
                return
            
            # Send initial status
            status_msg = await original_message.reply_text(
                f"🔄 *Forwarding Started*\n"
                f"• From: `{source_chat}`\n"
                f"• To: `{target_group}`\n"
                f"• Messages: {total_messages}\n"
                f"• Status: Starting...\n\n"
                f"Use `/cancel` to stop this job.",
                parse_mode="Markdown"
            )
            
            # Store in active jobs
            self.active_jobs[job_id] = {
                "status": "processing",
                "progress": 0,
                "current_message": f"{start_msg_id} of {end_msg_id}",
                "user_id": user_id
            }
            
            # Update database
            self.col_jobs.update_one(
                {"_id": job_id},
                {"$set": {"status": "processing", "current_message": f"{start_msg_id} of {end_msg_id}"}}
            )
            
            # Forward messages with rate limiting
            successful = 0
            failed = 0
            last_update_time = datetime.now(timezone.utc)
            
            for msg_id in range(start_msg_id, end_msg_id + 1):
                # Check if job was cancelled
                if job_id in self.cancelled_jobs:
                    await status_msg.edit_text(
                        f"🛑 *Forwarding Cancelled*\n"
                        f"• Forwarded: {successful} messages\n"
                        f"• Failed: {failed} messages\n"
                        f"• Cancelled by user."
                    )
                    
                    # Update database
                    self.col_jobs.update_one(
                        {"_id": job_id},
                        {"$set": {
                            "status": "cancelled",
                            "end_time": datetime.now(timezone.utc),
                            "stats": {"successful": successful, "failed": failed}
                        }}
                    )
                    
                    # Save stats
                    self._save_stats(user_id, successful, failed, source_chat, target_group)
                    return
                
                try:
                    # Get source message
                    source_msg = await context.bot.forward_message(
                        chat_id=target_group,
                        from_chat_id=source_chat,
                        message_id=msg_id
                    )
                    
                    # Apply replacements if any
                    if replacements and source_msg.caption:
                        new_caption = source_msg.caption
                        for old, new in replacements:
                            new_caption = new_caption.replace(old, new)
                        
                        # Edit caption with replacements
                        await source_msg.edit_caption(new_caption)
                    
                    successful += 1
                    
                except RetryAfter as e:
                    # Handle rate limiting
                    logger.warning(f"Rate limited. Waiting {e.retry_after} seconds")
                    await asyncio.sleep(e.retry_after)
                    
                    # Retry the same message
                    try:
                        source_msg = await context.bot.forward_message(
                            chat_id=target_group,
                            from_chat_id=source_chat,
                            message_id=msg_id
                        )
                        
                        if replacements and source_msg.caption:
                            new_caption = source_msg.caption
                            for old, new in replacements:
                                new_caption = new_caption.replace(old, new)
                            await source_msg.edit_caption(new_caption)
                        
                        successful += 1
                    except Exception as e:
                        logger.error(f"Failed to forward message {msg_id}: {e}")
                        failed += 1
                
                except Exception as e:
                    logger.error(f"Failed to forward message {msg_id}: {e}")
                    failed += 1
                
                # Update progress every 10 messages or every 10 seconds
                current_time = datetime.now(timezone.utc)
                if successful % 10 == 0 or (current_time - last_update_time).seconds >= 10:
                    progress = int(((msg_id - start_msg_id + 1) / total_messages) * 100)
                    
                    # Update active jobs
                    self.active_jobs[job_id] = {
                        "status": "processing",
                        "progress": progress,
                        "current_message": f"{msg_id} of {end_msg_id}",
                        "user_id": user_id
                    }
                    
                    # Update database
                    self.col_jobs.update_one(
                        {"_id": job_id},
                        {"$set": {
                            "progress": progress,
                            "current_message": f"{msg_id} of {end_msg_id}"
                        }}
                    )
                    
                    # Update status message
                    try:
                        await status_msg.edit_text(
                            f"🔄 *Forwarding in Progress*\n"
                            f"• Progress: {progress}%\n"
                            f"• Forwarded: {successful} ✅\n"
                            f"• Failed: {failed} ❌\n"
                            f"• Current: Message {msg_id}\n\n"
                            f"Use `/cancel` to stop.",
                            parse_mode="Markdown"
                        )
                    except:
                        pass
                    
                    last_update_time = current_time
            
            # Job completed
            completion_text = (
                f"✅ *Forwarding Completed*\n"
                f"• Total: {successful + failed} messages\n"
                f"• Successful: {successful} ✅\n"
                f"• Failed: {failed} ❌\n"
                f"• Success Rate: {((successful/(successful+failed))*100):.1f}%"
            )
            
            await status_msg.edit_text(completion_text, parse_mode="Markdown")
            
            # Update database
            self.col_jobs.update_one(
                {"_id": job_id},
                {"$set": {
                    "status": "completed",
                    "end_time": datetime.now(timezone.utc),
                    "stats": {"successful": successful, "failed": failed}
                }}
            )
            
            # Remove from active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
            
            # Save statistics
            self._save_stats(user_id, successful, failed, source_chat, target_group)
            
            # Clean up user tasks
            if user_id in self.user_tasks:
                self.user_tasks[user_id] = [t for t in self.user_tasks[user_id] if t['job_id'] != job_id]
        
        except Exception as e:
            logger.error(f"Error in process_forward_request: {e}")
            
            # Update database on error
            self.col_jobs.update_one(
                {"_id": job_id},
                {"$set": {
                    "status": "failed",
                    "end_time": datetime.now(timezone.utc),
                    "error": str(e)
                }}
            )
            
            # Remove from active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
            
            if update and update.effective_chat:
                try:
                    await update.message.reply_text(f"❌ Error: {str(e)}")
                except:
                    pass
    
    def _extract_from_link(self, link: str):
        """Extract chat_id and message_id from t.me link"""
        # Pattern for t.me/c/chat_id/message_id
        pattern = r't\.me/c/(\d+)/(\d+)'
        match = re.search(pattern, link)
        
        if match:
            chat_id = int("-100" + match.group(1))
            message_id = int(match.group(2))
            return chat_id, message_id
        
        raise ValueError(f"Invalid link format: {link}")
    
    def _save_stats(self, user_id: int, successful: int, failed: int, source_chat: str, target_chat: str):
        """Save statistics to database"""
        stat_data = {
            "user_id": user_id,
            "successful": successful,
            "failed": failed,
            "total": successful + failed,
            "source_chat": source_chat,
            "target_chat": target_chat,
            "timestamp": datetime.now(timezone.utc)
        }
        
        try:
            self.col_stats.insert_one(stat_data)
        except Exception as e:
            logger.error(f"Error saving stats: {e}")

# Global instance
forwarding_manager = ForwardingManager()
