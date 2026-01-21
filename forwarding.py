import asyncio
import logging
import time
import random
from typing import Dict, Optional, List
from datetime import datetime, timezone

from telegram import Update, Message, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest
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
col_pinned_messages = db["pinned_messages"]  # New collection for tracking pinned messages

class ForwardingManager:
    def __init__(self):
        self.active_jobs: Dict[int, bool] = {}
        self.rate_limit_wait = 2.0  # Minimum wait between messages (seconds)
        self.consecutive_errors = 0  # Track consecutive errors
        self.max_retries = 3  # Maximum retries per message
        self.topic_first_messages: Dict[str, int] = {}  # Cache for first message IDs in topics
        
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
                "created_at": datetime.now(timezone.utc)
            })
            
            logger.info(f"Created new topic '{topic_name}' in chat {chat_id}")
            return thread_id
            
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            return None
    
    async def pin_first_message_in_topic(self, bot, chat_id: int, thread_id: int, message_id: int):
        """Pin the first message in a forum topic"""
        try:
            # Check if already pinned in this topic
            pinned_doc = col_pinned_messages.find_one({
                "chat_id": chat_id,
                "thread_id": thread_id,
                "is_pinned": True
            })
            
            if pinned_doc:
                logger.info(f"Topic {thread_id} already has a pinned message")
                return True
            
            # Pin the message
            await bot.pin_chat_message(
                chat_id=chat_id,
                message_id=message_id,
                disable_notification=True
            )
            
            # Save to database
            col_pinned_messages.insert_one({
                "chat_id": chat_id,
                "thread_id": thread_id,
                "message_id": message_id,
                "is_pinned": True,
                "pinned_at": datetime.now(timezone.utc)
            })
            
            logger.info(f"Pinned first message {message_id} in topic {thread_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to pin message {message_id} in topic {thread_id}: {e}")
            return False
    
    async def forward_message(self, bot, source_chat_id: int, message_id: int,
                             target_chat_id: int, replacements: Dict[str, str],
                             original_request_msg: Message = None) -> bool:
        """Forward a single message with processing"""
        max_retries = self.max_retries
        retry_count = 0
        forwarded_message_id = None  # Store the ID of forwarded message
        
        while retry_count < max_retries:
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
                    return False
                
                # Extract topic from caption
                caption = msg.caption or msg.text or ""
                topic_name = extract_topic_from_caption(caption)
                
                # Get thread_id for topic
                thread_id = None
                if topic_name:
                    thread_id = await self.get_or_create_topic(bot, target_chat_id, topic_name)
                
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
                    
                    # Store the forwarded message ID
                    forwarded_message_id = forwarded_msg.message_id
                    
                    # Check if this is the first message in this topic
                    if thread_id and forwarded_message_id:
                        topic_key = f"{target_chat_id}_{thread_id}"
                        
                        # If this is the first message in this topic, pin it
                        if topic_key not in self.topic_first_messages:
                            self.topic_first_messages[topic_key] = forwarded_message_id
                            
                            # Pin the message (with delay to avoid rate limiting)
                            await asyncio.sleep(1)  # Small delay before pinning
                            
                            pin_success = await self.pin_first_message_in_topic(
                                bot, target_chat_id, thread_id, forwarded_message_id
                            )
                            
                            if pin_success:
                                logger.info(f"First message pinned in topic {thread_id}")
                        
                        # Also check database if this topic has a pinned message
                        else:
                            # Check if already pinned
                            pinned_doc = col_pinned_messages.find_one({
                                "chat_id": target_chat_id,
                                "thread_id": thread_id,
                                "is_pinned": True
                            })
                            
                            if not pinned_doc and forwarded_message_id:
                                # If not pinned yet, pin this message
                                await asyncio.sleep(1)
                                pin_success = await self.pin_first_message_in_topic(
                                    bot, target_chat_id, thread_id, forwarded_message_id
                                )
                    
                    # Reset consecutive errors counter on success
                    self.consecutive_errors = 0
                    return True
                    
                except RetryAfter as e:
                    # Handle Telegram rate limiting
                    wait_time = e.retry_after
                    logger.warning(f"Rate limited for {wait_time} seconds. Waiting...")
                    self.consecutive_errors += 1
                    
                    # If multiple consecutive errors, increase wait time
                    extra_wait = self.consecutive_errors * 5
                    total_wait = wait_time + extra_wait + 2
                    
                    await original_request_msg.reply_text(
                        f"⚠️ Rate limit hit! Waiting {total_wait:.0f} seconds...\n"
                        f"Consecutive errors: {self.consecutive_errors}"
                    )
                    
                    await asyncio.sleep(total_wait)
                    
                    # Retry with exponential backoff
                    retry_count += 1
                    continue
                    
                except (TimedOut, NetworkError) as e:
                    # Handle network errors
                    logger.warning(f"Network error for message {message_id}: {e}")
                    self.consecutive_errors += 1
                    
                    wait_time = 5 + (retry_count * 10)
                    await asyncio.sleep(wait_time)
                    retry_count += 1
                    continue
                    
                except BadRequest as e:
                    # Handle bad requests (message not found, etc.)
                    logger.error(f"Bad request for message {message_id}: {e}")
                    if "Message to forward not found" in str(e) or "Message to copy not found" in str(e):
                        return False  # Don't retry for missing messages
                    
                    # Check if it's a pinning error
                    if "not enough rights to pin a message" in str(e).lower():
                        logger.error(f"Bot doesn't have permission to pin messages in chat {target_chat_id}")
                        await original_request_msg.reply_text(
                            "⚠️ Warning: Bot doesn't have permission to pin messages in target group. "
                            "Please grant 'Pin Messages' permission."
                        )
                        # Continue without pinning
                        return True
                    
                    retry_count += 1
                    await asyncio.sleep(5)
                    continue
                    
                except Exception as e:
                    logger.error(f"Failed to forward message {message_id}: {e}")
                    retry_count += 1
                    await asyncio.sleep(5)
                    continue
                    
            except Exception as e:
                logger.error(f"Error processing message {message_id}: {e}")
                retry_count += 1
                await asyncio.sleep(5)
                continue
        
        # If we've exhausted all retries
        logger.error(f"Failed to forward message {message_id} after {max_retries} retries")
        return False
    
    async def check_and_pin_existing_topics(self, bot, chat_id: int):
        """Check existing topics and pin their first messages if not already pinned"""
        try:
            # Get all topics from database
            topics = col_topics.find({"chat_id": chat_id})
            
            pinned_count = 0
            for topic in topics:
                thread_id = topic["thread_id"]
                topic_name = topic["topic_name"]
                
                # Check if already pinned
                pinned_doc = col_pinned_messages.find_one({
                    "chat_id": chat_id,
                    "thread_id": thread_id,
                    "is_pinned": True
                })
                
                if not pinned_doc:
                    try:
                        # Get forum topic info
                        topic_info = await bot.get_forum_topic(
                            chat_id=chat_id,
                            message_thread_id=thread_id
                        )
                        
                        # Try to get messages from this topic
                        # We'll try to pin a placeholder message if we can't find the first one
                        logger.info(f"Checking topic '{topic_name}' (ID: {thread_id}) for pinning")
                        
                        # Send a temporary message to pin
                        temp_msg = await bot.send_message(
                            chat_id=chat_id,
                            text=f"📌 **{topic_name}**\n\nThis is the pinned message for this topic. "
                                 "Tap here to jump to the beginning.",
                            message_thread_id=thread_id,
                            parse_mode=ParseMode.MARKDOWN,
                            disable_notification=True
                        )
                        
                        # Pin the temporary message
                        await self.pin_first_message_in_topic(bot, chat_id, thread_id, temp_msg.message_id)
                        pinned_count += 1
                        
                        # Wait a bit to avoid rate limiting
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"Failed to pin message in topic {thread_id}: {e}")
                        continue
            
            return pinned_count
            
        except Exception as e:
            logger.error(f"Error checking existing topics: {e}")
            return 0
    
    async def process_forward_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                     request_data: Dict, original_message: Message):
        """Process complete forward request"""
        user_id = update.effective_user.id
        job_id = f"{user_id}_{int(time.time())}"
        
        # Mark job as started
        self.active_jobs[user_id] = True
        col_jobs.insert_one({
            "_id": job_id,
            "user_id": user_id,
            "status": "started",
            "start_time": datetime.now(timezone.utc),
            "request_data": request_data,
            "pinning_enabled": True  # Flag to indicate pinning is enabled
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
        
        # Check if bot has permission to pin messages
        try:
            bot_member = await context.bot.get_chat_member(target_chat_id, context.bot.id)
            can_pin = bot_member.can_pin_messages if hasattr(bot_member, 'can_pin_messages') else False
            
            if not can_pin:
                await original_message.reply_text(
                    "⚠️ Warning: Bot doesn't have 'Pin Messages' permission in target group. "
                    "First messages won't be pinned automatically.\n"
                    "Please grant this permission for better user experience."
                )
        except Exception as e:
            logger.warning(f"Could not check pin permissions: {e}")
        
        # Start forwarding
        status_msg = await original_message.reply_text(
            f"🔄 Forwarding started...\n"
            f"• Messages: {start_msg_id} to {end_msg_id}\n"
            f"• Total: {total_messages} messages\n"
            f"• Target: {target_chat_id}\n"
            f"• 📌 Auto-pinning: ENABLED\n"
            f"⏳ Please wait..."
        )
        
        bot = context.bot
        successful = 0
        failed = 0
        topics_created = 0
        messages_pinned = 0
        start_time = time.time()
        last_status_update = 0
        
        # Reset topic cache for this job
        self.topic_first_messages = {}
        
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
                # Reset consecutive errors on success streak
                if successful % 10 == 0:
                    self.consecutive_errors = max(0, self.consecutive_errors - 2)
            else:
                failed += 1
            
            # Track topics created (rough estimate)
            if idx % 5 == 0:
                topics_created = len(self.topic_first_messages)
            
            # Update status every 10 messages or every 30 seconds
            current_time = time.time()
            if idx % 10 == 0 or idx == total_messages or (current_time - last_status_update) > 30:
                try:
                    elapsed_time = current_time - start_time
                    messages_per_minute = (idx / elapsed_time * 60) if elapsed_time > 0 else 0
                    
                    # Add random delay before updating status (to avoid API spam)
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    
                    status_text = (
                        f"🔄 Forwarding in progress...\n"
                        f"• Processed: {idx}/{total_messages}\n"
                        f"✅ Successful: {successful}\n"
                        f"❌ Failed: {failed}\n"
                        f"📁 Topics created: {topics_created}\n"
                        f"📌 Messages pinned: {len(self.topic_first_messages)}\n"
                        f"⏳ Progress: {((idx) / total_messages * 100):.1f}%\n"
                        f"📊 Speed: {messages_per_minute:.1f} msg/min\n"
                        f"⏱️ Time: {elapsed_time:.0f}s"
                    )
                    
                    await status_msg.edit_text(status_text)
                    last_status_update = current_time
                except Exception as e:
                    logger.warning(f"Failed to update status: {e}")
            
            # Dynamic delay based on consecutive errors
            base_delay = self.rate_limit_wait
            if self.consecutive_errors > 0:
                # Increase delay if we're hitting rate limits
                extra_delay = min(self.consecutive_errors * 0.5, 5.0)  # Max 5 seconds extra
                base_delay += extra_delay
            
            # Add random jitter to avoid pattern detection
            jitter = random.uniform(0.1, 0.5)
            total_delay = base_delay + jitter
            
            await asyncio.sleep(total_delay)
            
            # Every 50 messages, take a longer break
            if idx % 50 == 0 and idx < total_messages:
                long_break = 10 + (self.consecutive_errors * 5)
                await status_msg.edit_text(
                    f"⏸️ Taking a {long_break} second break to avoid rate limiting...\n"
                    f"Processed {idx}/{total_messages} messages so far."
                )
                await asyncio.sleep(long_break)
        
        # Calculate total time
        total_time = time.time() - start_time
        
        # Count total pinned messages from database
        total_pinned = col_pinned_messages.count_documents({
            "chat_id": target_chat_id,
            "is_pinned": True
        })
        
        # Update job status
        col_jobs.update_one(
            {"_id": job_id},
            {"$set": {
                "status": "completed",
                "end_time": datetime.now(timezone.utc),
                "stats": {
                    "successful": successful,
                    "failed": failed,
                    "total": total_messages,
                    "topics_created": topics_created,
                    "messages_pinned": total_pinned
                }
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
            "consecutive_errors": self.consecutive_errors,
            "messages_pinned": total_pinned,
            "topics_created": topics_created
        })
        
        # Reset error counter for next job
        self.consecutive_errors = 0
        
        # Send completion message
        completion_text = (
            f"✅ Forwarding completed!\n\n"
            f"📊 Statistics:\n"
            f"• Total messages: {total_messages}\n"
            f"• ✅ Successful: {successful}\n"
            f"• ❌ Failed: {failed}\n"
            f"• 📁 Topics created: {topics_created}\n"
            f"• 📌 Messages pinned: {total_pinned}\n"
            f"• ⏱️ Time taken: {total_time:.1f} seconds\n"
            f"• 📈 Speed: {(successful / total_time * 60):.1f} messages/minute\n"
            f"• 🔄 Replacements applied: {len(request_data['replacements'])}\n"
            f"• 🎯 Target group: {target_chat_id}\n\n"
            f"✅ First messages in each topic have been automatically pinned!\n"
            f"Users can now tap on topics to jump directly to the beginning.\n\n"
            f"Job ID: `{job_id}`"
        )
        
        try:
            await status_msg.edit_text(completion_text, parse_mode=ParseMode.MARKDOWN)
        except:
            await original_message.reply_text(completion_text, parse_mode=ParseMode.MARKDOWN)
        
        self.active_jobs[user_id] = False
    
    async def pin_all_topics_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to pin first messages in all existing topics"""
        user_id = update.effective_user.id
        
        if len(context.args) < 1:
            await update.message.reply_text(
                "Usage: /pinalltopics <chat_id>\n\n"
                "This will pin the first message in every topic of the specified group."
            )
            return
        
        try:
            chat_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ Invalid chat ID.")
            return
        
        # Check permissions
        try:
            bot_member = await context.bot.get_chat_member(chat_id, context.bot.id)
            can_pin = bot_member.can_pin_messages if hasattr(bot_member, 'can_pin_messages') else False
            
            if not can_pin:
                await update.message.reply_text(
                    "❌ Bot doesn't have 'Pin Messages' permission in this group."
                )
                return
        except Exception as e:
            logger.error(f"Permission check failed: {e}")
            await update.message.reply_text("❌ Could not check bot permissions.")
            return
        
        status_msg = await update.message.reply_text(
            f"🔍 Scanning topics in chat {chat_id}...\n"
            f"⏳ This may take a while..."
        )
        
        try:
            pinned_count = await self.check_and_pin_existing_topics(context.bot, chat_id)
            
            await status_msg.edit_text(
                f"✅ Topic pinning completed!\n"
                f"• Total messages pinned: {pinned_count}\n"
                f"• Chat ID: {chat_id}\n\n"
                f"Now users can tap on any topic to jump to its pinned first message!"
            )
            
        except Exception as e:
            logger.error(f"Failed to pin topics: {e}")
            await status_msg.edit_text(f"❌ Failed to pin topics: {e}")
    
    async def cancel_forward(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel ongoing forward job"""
        user_id = update.effective_user.id
        
        if self.active_jobs.get(user_id):
            self.active_jobs[user_id] = False
            await update.message.reply_text("🛑 Forwarding cancelled.")
        else:
            await update.message.reply_text("ℹ️ No active forwarding job found.")

# Global instance
forwarding_manager = ForwardingManager()
