import asyncio
import logging
import time
from typing import Dict, Optional
from datetime import datetime, timezone
import random

from telegram import Update, Message
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram.error import RetryAfter
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

class ForwardingManager:
    def __init__(self):
        self.active_jobs: Dict[int, bool] = {}
        self.message_queue = asyncio.Queue()
        self.processing = False
        self.message_counter = 0
        self.last_minute = time.time() // 60
    
    async def get_or_create_topic(self, bot, chat_id: int, topic_name: str) -> int:
        """Get existing topic thread_id or create new one"""
        if not topic_name:
            return None
        
        # Check in cache/database
        doc = col_topics.find_one({
            "chat_id": chat_id,
            "topic_name": topic_name
        })
        
        if doc:
            return doc["thread_id"]
        
        # Add prefix and emoji to topic name
        formatted_topic_name = f"📌 topic: {topic_name}"[:128]
        
        # Create new forum topic
        try:
            topic = await bot.create_forum_topic(
                chat_id=chat_id,
                name=formatted_topic_name
            )
            thread_id = topic.message_thread_id
            
            # Save to database
            col_topics.insert_one({
                "chat_id": chat_id,
                "topic_name": topic_name,
                "formatted_topic_name": formatted_topic_name,
                "thread_id": thread_id,
                "created_at": datetime.now(timezone.utc),
                "first_message_pinned": False
            })
            
            logger.info(f"Created new topic '{formatted_topic_name}' in chat {chat_id}")
            
            return thread_id
            
        except Exception as e:
            logger.error(f"Failed to create topic '{formatted_topic_name}': {e}")
            return None
    
    async def pin_first_message_for_topic(self, bot, chat_id: int, thread_id: int, topic_name: str):
        """Pin first message in a topic"""
        try:
            # Send a welcome message in the topic
            welcome_msg = await bot.send_message(
                chat_id=chat_id,
                text=f"📌 **{topic_name}** - Topic started",
                message_thread_id=thread_id,
                parse_mode=ParseMode.MARKDOWN,
                disable_notification=True
            )
            
            # Pin the welcome message
            await bot.pin_chat_message(
                chat_id=chat_id,
                message_id=welcome_msg.message_id,
                disable_notification=True
            )
            
            # Update database that first message is pinned
            col_topics.update_one(
                {
                    "chat_id": chat_id,
                    "thread_id": thread_id
                },
                {"$set": {"first_message_pinned": True}}
            )
            
            logger.info(f"Pinned first message in topic '{topic_name}' (thread: {thread_id})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to pin first message for topic '{topic_name}': {e}")
            return False
    
    async def rate_limit_controller(self):
        """Control message sending rate to 20 messages per minute"""
        current_minute = time.time() // 60
        
        if current_minute != self.last_minute:
            # New minute, reset counter
            self.message_counter = 0
            self.last_minute = current_minute
        
        if self.message_counter >= 20:
            # Limit reached for this minute
            wait_time = 60 - (time.time() % 60) + 1  # Wait for next minute
            logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
            await asyncio.sleep(wait_time)
            self.message_counter = 0
            self.last_minute = time.time() // 60
        
        self.message_counter += 1
        
        # Add random small delay to avoid burst
        await asyncio.sleep(random.uniform(0.1, 0.3))
    
    async def message_processor(self, bot):
        """Background processor for sending messages with rate limiting"""
        self.processing = True
        
        while self.processing or not self.message_queue.empty():
            try:
                # Get next message from queue
                message_data = await asyncio.wait_for(
                    self.message_queue.get(), 
                    timeout=1.0
                )
                
                if message_data is None:
                    continue
                
                (source_chat_id, message_id, target_chat_id, 
                 thread_id, caption, msg_type) = message_data
                
                # Apply rate limiting
                await self.rate_limit_controller()
                
                # Send the message
                try:
                    if msg_type == "media":
                        await bot.copy_message(
                            chat_id=target_chat_id,
                            from_chat_id=source_chat_id,
                            message_id=message_id,
                            caption=caption[:1024] if caption else None,
                            message_thread_id=thread_id,
                            parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                            disable_notification=True
                        )
                    elif caption:
                        await bot.send_message(
                            chat_id=target_chat_id,
                            text=caption,
                            message_thread_id=thread_id,
                            parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                            disable_notification=True
                        )
                    else:
                        await bot.forward_message(
                            chat_id=target_chat_id,
                            from_chat_id=source_chat_id,
                            message_id=message_id,
                            message_thread_id=thread_id,
                            disable_notification=True
                        )
                    
                    self.message_queue.task_done()
                    return True
                    
                except RetryAfter as e:
                    wait_time = e.retry_after
                    logger.warning(f"Rate limited for {wait_time} seconds. Waiting...")
                    await asyncio.sleep(wait_time + 1)
                    
                    # Retry after waiting
                    try:
                        if msg_type == "media":
                            await bot.copy_message(
                                chat_id=target_chat_id,
                                from_chat_id=source_chat_id,
                                message_id=message_id,
                                caption=caption[:1024] if caption else None,
                                message_thread_id=thread_id,
                                parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                                disable_notification=True
                            )
                        elif caption:
                            await bot.send_message(
                                chat_id=target_chat_id,
                                text=caption,
                                message_thread_id=thread_id,
                                parse_mode=ParseMode.HTML if caption and ('<' in caption or '>' in caption) else None,
                                disable_notification=True
                            )
                        else:
                            await bot.forward_message(
                                chat_id=target_chat_id,
                                from_chat_id=source_chat_id,
                                message_id=message_id,
                                message_thread_id=thread_id,
                                disable_notification=True
                            )
                        
                        self.message_queue.task_done()
                        return True
                        
                    except Exception as retry_e:
                        logger.error(f"Failed after retry: {retry_e}")
                        self.message_queue.task_done()
                        return False
                        
                except Exception as e:
                    logger.error(f"Failed to send message: {e}")
                    self.message_queue.task_done()
                    return False
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in message processor: {e}")
                continue
    
    async def forward_message(self, bot, source_chat_id: int, message_id: int,
                             target_chat_id: int, replacements: Dict[str, str],
                             original_request_msg: Message = None) -> bool:
        """Queue a message for forwarding with processing"""
        try:
            # Get original message
            try:
                if LOG_CHANNEL:
                    msg = await bot.forward_message(
                        chat_id=LOG_CHANNEL,
                        from_chat_id=source_chat_id,
                        message_id=message_id,
                        disable_notification=True
                    )
                else:
                    msg = await bot.copy_message(
                        chat_id=original_request_msg.chat_id if original_request_msg else target_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=message_id,
                        disable_notification=True
                    )
            except Exception as e:
                logger.error(f"Failed to get message {message_id}: {e}")
                return False
            
            # Extract topic from caption
            caption = msg.caption or msg.text or ""
            topic_name = extract_topic_from_caption(caption)
            
            # Get thread_id for topic
            thread_id = None
            if topic_name:
                thread_id = await self.get_or_create_topic(bot, target_chat_id, topic_name)
                
                # Check if first message needs to be pinned
                if thread_id:
                    doc = col_topics.find_one({
                        "chat_id": target_chat_id,
                        "thread_id": thread_id,
                        "first_message_pinned": False
                    })
                    
                    if doc:
                        # Pin first message for this topic
                        await self.pin_first_message_for_topic(
                            bot, target_chat_id, thread_id, topic_name
                        )
            
            # Apply replacements to caption
            if caption and replacements:
                caption = apply_replacements(caption, replacements)
            
            # Determine message type
            msg_type = "media" if (msg.photo or msg.video or msg.document) else "text"
            
            # Add message to queue for processing
            await self.message_queue.put((
                source_chat_id, message_id, target_chat_id, 
                thread_id, caption, msg_type
            ))
            
            return True
                
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            return False
    
    async def process_forward_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                     request_data: Dict, original_message: Message):
        """Process complete forward request"""
        user_id = update.effective_user.id
        job_id = f"{user_id}_{int(time.time())}"
        
        # Start message processor if not already running
        if not self.processing:
            asyncio.create_task(self.message_processor(context.bot))
        
        # Mark job as started
        self.active_jobs[user_id] = True
        col_jobs.insert_one({
            "_id": job_id,
            "user_id": user_id,
            "status": "started",
            "start_time": datetime.now(timezone.utc),
            "request_data": request_data
        })
        
        # Extract information from links
        start_chat_id, start_msg_id = extract_message_info_from_link(request_data['start_link'])
        end_chat_id, end_msg_id = extract_message_info_from_link(request_data['end_link'])
        
        if not all([start_chat_id, start_msg_id, end_chat_id, end_msg_id]):
            await original_message.reply_text("❌ Invalid message links. Please check format.")
            self.active_jobs[user_id] = False
            return
        
        if start_chat_id != end_chat_id:
            await original_message.reply_text("❌ Start and end links must be from same chat.")
            self.active_jobs[user_id] = False
            return
        
        try:
            target_chat_id = int(request_data['target_group'])
        except ValueError:
            await original_message.reply_text("❌ Invalid target group ID.")
            self.active_jobs[user_id] = False
            return
        
        # Validate message range
        if start_msg_id > end_msg_id:
            start_msg_id, end_msg_id = end_msg_id, start_msg_id
        
        total_messages = end_msg_id - start_msg_id + 1
        
        if total_messages > MAX_SYNC_MESSAGES:
            await original_message.reply_text(
                f"❌ Too many messages ({total_messages}). Maximum is {MAX_SYNC_MESSAGES}."
            )
            self.active_jobs[user_id] = False
            return
        
        # Start forwarding
        status_msg = await original_message.reply_text(
            f"🔄 Forwarding started...\n"
            f"• Messages: {start_msg_id} to {end_msg_id}\n"
            f"• Total: {total_messages} messages\n"
            f"• Target: {target_chat_id}\n"
            f"• Rate Limit: 20 messages/minute\n"
            f"⏳ Please wait..."
        )
        
        bot = context.bot
        successful = 0
        failed = 0
        start_time = time.time()
        
        # Process all messages
        for idx, msg_id in enumerate(range(start_msg_id, end_msg_id + 1), 1):
            if not self.active_jobs.get(user_id, True):
                await status_msg.edit_text("❌ Forwarding cancelled.")
                break
            
            success = await self.forward_message(
                bot=bot,
                source_chat_id=start_chat_id,
                message_id=msg_id,
                target_chat_id=target_chat_id,
                replacements=request_data['replacements'],
                original_request_msg=original_message
            )
            
            if success:
                successful += 1
            else:
                failed += 1
            
            # Update status every 5 messages to reduce API calls
            if idx % 5 == 0 or idx == total_messages:
                try:
                    elapsed_time = time.time() - start_time
                    current_minute = int(elapsed_time / 60) + 1
                    estimated_total_minutes = (total_messages / 20) + 1
                    remaining_minutes = max(0, estimated_total_minutes - current_minute)
                    
                    await status_msg.edit_text(
                        f"🔄 Forwarding in progress...\n"
                        f"• Processed: {idx}/{total_messages}\n"
                        f"✅ Successful: {successful}\n"
                        f"❌ Failed: {failed}\n"
                        f"⏳ Progress: {((idx) / total_messages * 100):.1f}%\n"
                        f"📊 Speed: 20 messages/minute\n"
                        f"⏱️ Elapsed: {int(elapsed_time/60)}m {int(elapsed_time%60)}s\n"
                        f"⏰ Estimated remaining: ~{remaining_minutes:.0f} minutes\n"
                        f"📝 Queue size: {self.message_queue.qsize()}"
                    )
                except:
                    pass
        
        # Wait for all messages to be processed
        while not self.message_queue.empty():
            await asyncio.sleep(1)
        
        # Stop processor if no other jobs
        if all(not active for active in self.active_jobs.values()):
            self.processing = False
        
        # Calculate total time
        total_time = time.time() - start_time
        
        # Update job status
        col_jobs.update_one(
            {"_id": job_id},
            {"$set": {
                "status": "completed",
                "end_time": datetime.now(timezone.utc),
                "stats": {"successful": successful, "failed": failed, "total": total_messages}
            }}
        )
        
        # Save stats
        col_stats.insert_one({
            "user_id": user_id,
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
            "rate_limit_enforced": True
        })
        
        # Send completion message
        await status_msg.edit_text(
            f"✅ Forwarding completed!\n\n"
            f"📊 Statistics:\n"
            f"• Total messages: {total_messages}\n"
            f"• ✅ Successful: {successful}\n"
            f"• ❌ Failed: {failed}\n"
            f"• ⏱️ Time taken: {total_time:.1f} seconds\n"
            f"• 📈 Speed: {(successful / total_time * 60):.1f} messages/minute\n"
            f"• 🔄 Replacements applied: {len(request_data['replacements'])}\n"
            f"• 🎯 Target group: {target_chat_id}\n"
            f"• ⚡ Rate Limit: 20 messages/minute\n\n"
            f"Job ID: `{job_id}`",
            parse_mode=ParseMode.MARKDOWN
        )
        
        self.active_jobs[user_id] = False
    
    async def cancel_forward(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel ongoing forward job"""
        user_id = update.effective_user.id
        
        if self.active_jobs.get(user_id):
            self.active_jobs[user_id] = False
            await update.message.reply_text("🛑 Forwarding cancelled.")
            
            # Clear queue for this user
            if self.message_queue.empty():
                self.processing = False
        else:
            await update.message.reply_text("ℹ️ No active forwarding job found.")

# Global instance
forwarding_manager = ForwardingManager()
