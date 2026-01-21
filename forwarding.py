import asyncio
import logging
import time
from typing import Dict, Optional, List
from datetime import datetime, timezone

from telegram import Update, Message, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TelegramError
from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, LOG_CHANNEL, MAX_SYNC_MESSAGES
from utils import (
    extract_message_info_from_link,
    extract_topic_from_caption,
    apply_replacements
)

# Setup logging
logger = logging.getLogger(__name__)

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col_topics = db["topics"]
col_jobs = db["forward_jobs"]
col_stats = db["forward_stats"]
col_pinned = db["pinned_messages"]

class ForwardingManager:
    def __init__(self):
        # Store active jobs per user: {user_id: {job_id: is_active}}
        self.active_jobs: Dict[int, Dict[str, bool]] = {}
        # Store pinned messages: {(chat_id, thread_id): message_id}
        self.pinned_messages_cache = {}
    
    def get_user_active_jobs(self, user_id: int) -> List[str]:
        """Get list of active job IDs for a user"""
        return [job_id for job_id, is_active in self.active_jobs.get(user_id, {}).items() if is_active]
    
    def is_job_active(self, user_id: int, job_id: str) -> bool:
        """Check if a specific job is active"""
        return self.active_jobs.get(user_id, {}).get(job_id, False)
    
    def set_job_active(self, user_id: int, job_id: str, is_active: bool = True):
        """Set job active status"""
        if user_id not in self.active_jobs:
            self.active_jobs[user_id] = {}
        self.active_jobs[user_id][job_id] = is_active
    
    def stop_all_user_jobs(self, user_id: int):
        """Stop all jobs for a user"""
        if user_id in self.active_jobs:
            for job_id in self.active_jobs[user_id]:
                self.active_jobs[user_id][job_id] = False
    
    async def get_or_create_topic(self, bot, chat_id: int, topic_name: str, job_id: str) -> tuple:
        """Get existing topic thread_id or create new one"""
        if not topic_name:
            return None, False
        
        # Check in cache/database
        doc = col_topics.find_one({
            "chat_id": chat_id,
            "topic_name": topic_name
        })
        
        if doc:
            return doc["thread_id"], False
        
        # Create new forum topic
        try:
            topic = await bot.create_forum_topic(
                chat_id=chat_id,
                name=topic_name[:128]
            )
            thread_id = topic.message_thread_id
            
            # Save to database
            col_topics.insert_one({
                "chat_id": chat_id,
                "topic_name": topic_name,
                "thread_id": thread_id,
                "created_at": datetime.now(timezone.utc),
                "created_by_job": job_id
            })
            
            logger.info(f"Created new topic '{topic_name}' in chat {chat_id}")
            return thread_id, True  # True = newly created
            
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            return None, False
    
    async def pin_first_message_in_topic(self, bot, chat_id: int, thread_id: int, message_id: int, job_id: str):
        """Pin the first message in a topic"""
        try:
            # Check if we've already pinned a message in this topic
            cache_key = (chat_id, thread_id)
            if cache_key in self.pinned_messages_cache:
                return
            
            # Check database
            pinned_doc = col_pinned.find_one({
                "chat_id": chat_id,
                "thread_id": thread_id
            })
            
            if pinned_doc:
                self.pinned_messages_cache[cache_key] = pinned_doc["message_id"]
                return
            
            # Pin the message
            await bot.pin_chat_message(
                chat_id=chat_id,
                message_id=message_id,
                disable_notification=True
            )
            
            # Cache and store in database
            self.pinned_messages_cache[cache_key] = message_id
            col_pinned.insert_one({
                "chat_id": chat_id,
                "thread_id": thread_id,
                "message_id": message_id,
                "pinned_at": datetime.now(timezone.utc),
                "pinned_by_job": job_id
            })
            
            logger.info(f"Pinned first message {message_id} in topic {thread_id} of chat {chat_id}")
            
        except TelegramError as e:
            # Log but don't fail if pinning fails (might not have permission)
            logger.warning(f"Could not pin message {message_id} in topic {thread_id}: {e}")
        except Exception as e:
            logger.error(f"Error pinning message: {e}")
    
    async def forward_message(self, bot, source_chat_id: int, message_id: int,
                             target_chat_id: int, replacements: Dict[str, str],
                             job_id: str, original_request_msg: Message = None) -> tuple:
        """Forward a single message with processing"""
        try:
            # Get original message
            try:
                if LOG_CHANNEL:
                    # Use LOG_CHANNEL if configured
                    msg = await bot.forward_message(
                        chat_id=LOG_CHANNEL,
                        from_chat_id=source_chat_id,
                        message_id=message_id,
                        disable_notification=True
                    )
                else:
                    # Directly get the message if LOG_CHANNEL is not configured
                    msg = await bot.copy_message(
                        chat_id=original_request_msg.chat_id if original_request_msg else target_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=message_id,
                        disable_notification=True
                    )
            except Exception as e:
                logger.error(f"Failed to get message {message_id}: {e}")
                return False, None, None
            
            # Extract topic from caption
            caption = msg.caption or msg.text or ""
            topic_name = extract_topic_from_caption(caption)
            
            # Get thread_id for topic
            thread_id = None
            is_new_topic = False
            if topic_name:
                thread_id, is_new_topic = await self.get_or_create_topic(bot, target_chat_id, topic_name, job_id)
            
            # Apply replacements to caption
            if caption and replacements:
                caption = apply_replacements(caption, replacements)
            
            # Forward to target with proper thread
            try:
                if msg.photo or msg.video or msg.document:
                    # For media messages, use copy with caption
                    forwarded_msg = await bot.copy_message(
                        chat_id=target_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=message_id,
                        caption=caption[:1024] if caption else None,
                        message_thread_id=thread_id,
                        parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                        disable_notification=True
                    )
                elif caption:
                    # For text messages
                    forwarded_msg = await bot.send_message(
                        chat_id=target_chat_id,
                        text=caption,
                        message_thread_id=thread_id,
                        parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                        disable_notification=True
                    )
                else:
                    # If no caption and not media, just forward
                    forwarded_msg = await bot.forward_message(
                        chat_id=target_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=message_id,
                        message_thread_id=thread_id,
                        disable_notification=True
                    )
                
                # Pin first message if this is a new topic
                if is_new_topic and thread_id and forwarded_msg:
                    asyncio.create_task(
                        self.pin_first_message_in_topic(bot, target_chat_id, thread_id, forwarded_msg.message_id, job_id)
                    )
                
                return True, forwarded_msg.message_id if forwarded_msg else None, thread_id
                
            except RetryAfter as e:
                # Handle Telegram rate limiting
                wait_time = e.retry_after
                logger.warning(f"Rate limited for {wait_time} seconds. Waiting...")
                await asyncio.sleep(wait_time + 1)
                
                # Retry once after waiting
                try:
                    if msg.photo or msg.video or msg.document:
                        forwarded_msg = await bot.copy_message(
                            chat_id=target_chat_id,
                            from_chat_id=source_chat_id,
                            message_id=message_id,
                            caption=caption[:1024] if caption else None,
                            message_thread_id=thread_id,
                            parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                            disable_notification=True
                        )
                    elif caption:
                        forwarded_msg = await bot.send_message(
                            chat_id=target_chat_id,
                            text=caption,
                            message_thread_id=thread_id,
                            parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                            disable_notification=True
                        )
                    else:
                        forwarded_msg = await bot.forward_message(
                            chat_id=target_chat_id,
                            from_chat_id=source_chat_id,
                            message_id=message_id,
                            message_thread_id=thread_id,
                            disable_notification=True
                        )
                    
                    # Pin first message if this is a new topic
                    if is_new_topic and thread_id and forwarded_msg:
                        asyncio.create_task(
                            self.pin_first_message_in_topic(bot, target_chat_id, thread_id, forwarded_msg.message_id, job_id)
                        )
                    
                    return True, forwarded_msg.message_id if forwarded_msg else None, thread_id
                except Exception as retry_e:
                    logger.error(f"Failed to forward message {message_id} after retry: {retry_e}")
                    return False, None, None
                    
            except Exception as e:
                logger.error(f"Failed to forward message {message_id}: {e}")
                return False, None, None
                
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            return False, None, None
    
    async def process_forward_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                     request_data: Dict, original_message: Message, job_id: str = None):
        """Process complete forward request"""
        user_id = update.effective_user.id
        
        if not job_id:
            job_id = f"{user_id}_{int(time.time())}"
        
        # Mark job as started
        self.set_job_active(user_id, job_id, True)
        
        # Initialize job tracking in database
        col_jobs.update_one(
            {"_id": job_id},
            {"$set": {
                "_id": job_id,
                "user_id": user_id,
                "status": "started",
                "start_time": datetime.now(timezone.utc),
                "request_data": request_data,
                "progress": 0,
                "current_message": 0,
                "total_messages": 0
            }},
            upsert=True
        )
        
        # Extract information from links
        start_chat_id, start_msg_id = extract_message_info_from_link(request_data['start_link'])
        end_chat_id, end_msg_id = extract_message_info_from_link(request_data['end_link'])
        
        if not all([start_chat_id, start_msg_id, end_chat_id, end_msg_id]):
            await original_message.reply_text("❌ Invalid message links. Please check format.")
            self.set_job_active(user_id, job_id, False)
            return
        
        if start_chat_id != end_chat_id:
            await original_message.reply_text("❌ Start and end links must be from same chat.")
            self.set_job_active(user_id, job_id, False)
            return
        
        try:
            target_chat_id = int(request_data['target_group'])
        except ValueError:
            await original_message.reply_text("❌ Invalid target group ID.")
            self.set_job_active(user_id, job_id, False)
            return
        
        # Validate message range
        if start_msg_id > end_msg_id:
            start_msg_id, end_msg_id = end_msg_id, start_msg_id
        
        total_messages = end_msg_id - start_msg_id + 1
        
        if total_messages > MAX_SYNC_MESSAGES:
            await original_message.reply_text(
                f"❌ Too many messages ({total_messages}). Maximum is {MAX_SYNC_MESSAGES}."
            )
            self.set_job_active(user_id, job_id, False)
            return
        
        # Update job with total count
        col_jobs.update_one(
            {"_id": job_id},
            {"$set": {
                "total_messages": total_messages,
                "current_message": 0
            }}
        )
        
        # Start forwarding
        status_msg = await original_message.reply_text(
            f"🔄 Forwarding started (Job: {job_id[:8]}...)\n"
            f"• Messages: {start_msg_id} to {end_msg_id}\n"
            f"• Total: {total_messages} messages\n"
            f"• Target: {target_chat_id}\n"
            f"• Topics: Auto-created & auto-pinned\n"
            f"⏳ Please wait..."
        )
        
        bot = context.bot
        successful = 0
        failed = 0
        created_topics = 0
        pinned_messages = 0
        start_time = time.time()
        
        for idx, msg_id in enumerate(range(start_msg_id, end_msg_id + 1), 1):
            # Check if job is still active
            if not self.is_job_active(user_id, job_id):
                await status_msg.edit_text(f"❌ Forwarding cancelled (Job: {job_id[:8]}...)")
                break
            
            success, forwarded_msg_id, thread_id = await self.forward_message(
                bot=bot,
                source_chat_id=start_chat_id,
                message_id=msg_id,
                target_chat_id=target_chat_id,
                replacements=request_data['replacements'],
                job_id=job_id,
                original_request_msg=original_message
            )
            
            if success:
                successful += 1
            else:
                failed += 1
            
            # Update progress in database
            progress_percent = int((idx / total_messages) * 100)
            col_jobs.update_one(
                {"_id": job_id},
                {"$set": {
                    "progress": progress_percent,
                    "current_message": idx,
                    "successful": successful,
                    "failed": failed
                }}
            )
            
            # Update status every 20 messages to reduce API calls
            if idx % 20 == 0 or idx == total_messages:
                try:
                    if not self.is_job_active(user_id, job_id):
                        break
                    
                    elapsed_time = time.time() - start_time
                    messages_per_minute = (idx / elapsed_time * 60) if elapsed_time > 0 else 0
                    
                    await status_msg.edit_text(
                        f"🔄 Forwarding in progress (Job: {job_id[:8]}...)\n"
                        f"• Processed: {idx}/{total_messages}\n"
                        f"✅ Successful: {successful}\n"
                        f"❌ Failed: {failed}\n"
                        f"⏳ Progress: {progress_percent}%\n"
                        f"📊 Speed: {messages_per_minute:.1f} msg/min\n"
                        f"⏱️ Time: {elapsed_time:.0f}s\n"
                        f"📌 Topics: {created_topics} created"
                    )
                except:
                    pass
            
            # IMPORTANT: Increased delay to avoid rate limiting
            # 1.2 seconds gap between messages = ~50 messages per minute
            await asyncio.sleep(1.2)
        
        # Calculate total time
        total_time = time.time() - start_time
        
        # Update job status
        col_jobs.update_one(
            {"_id": job_id},
            {"$set": {
                "status": "completed",
                "end_time": datetime.now(timezone.utc),
                "progress": 100,
                "stats": {
                    "successful": successful,
                    "failed": failed,
                    "total": total_messages,
                    "created_topics": created_topics,
                    "pinned_messages": pinned_messages
                }
            }}
        )
        
        # Save stats
        col_stats.insert_one({
            "user_id": user_id,
            "job_id": job_id,
            "timestamp": datetime.now(timezone.utc),
            "source_chat": start_chat_id,
            "target_chat": target_chat_id,
            "message_range": f"{start_msg_id}-{end_msg_id}",
            "successful": successful,
            "failed": failed,
            "total_messages": total_messages,
            "total_time_seconds": total_time,
            "messages_per_minute": (successful / total_time * 60) if total_time > 0 else 0,
            "replacements_count": len(request_data['replacements']),
            "created_topics": created_topics,
            "pinned_messages": pinned_messages
        })
        
        # Mark job as inactive
        self.set_job_active(user_id, job_id, False)
        
        # Send completion message
        completion_text = f"""
✅ Forwarding completed (Job: `{job_id}`)!

📊 Statistics:
• Total messages: {total_messages}
• ✅ Successful: {successful}
• ❌ Failed: {failed}
• ⏱️ Time taken: {total_time:.1f} seconds
• 📈 Speed: {(successful / total_time * 60):.1f} messages/minute
• 🔄 Replacements applied: {len(request_data['replacements'])}
• 🎯 Target group: {target_chat_id}
• 📌 Topics created: {created_topics}
• 📍 Messages pinned: {pinned_messages}

🔧 Features enabled:
✓ Multi-tasking support
✓ Auto-topic creation
✓ Auto-pin first message
✓ Concurrent job support
"""
        
        await status_msg.edit_text(
            completion_text,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def cancel_forward(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel ongoing forward job for user"""
        user_id = update.effective_user.id
        
        active_jobs = self.get_user_active_jobs(user_id)
        if active_jobs:
            self.stop_all_user_jobs(user_id)
            await update.message.reply_text(f"🛑 Cancelled {len(active_jobs)} active job(s).")
        else:
            await update.message.reply_text("ℹ️ No active forwarding jobs found.")

# Global instance
forwarding_manager = ForwardingManager()
