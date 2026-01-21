import re
from typing import Optional, Tuple, List, Dict
from urllib.parse import urlparse
from datetime import datetime

def extract_message_info_from_link(link: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract chat_id and message_id from Telegram message link
    
    Supports formats:
    - https://t.me/c/1234567890/1234
    - https://t.me/username/1234
    - https://t.me/c/1234567890/1234?thread=567
    """
    if not link or "t.me" not in link:
        return None, None
    
    try:
        # Parse the URL
        parsed = urlparse(link)
        path_parts = parsed.path.strip('/').split('/')
        
        if len(path_parts) < 2:
            return None, None
        
        if path_parts[0] == 'c':
            # Channel/group link format: /c/chat_id/message_id
            if len(path_parts) >= 3:
                try:
                    chat_id = int(path_parts[1])
                    message_id = int(path_parts[2])
                    
                    # Convert to negative for group/channel
                    if chat_id > 0:
                        chat_id = -1000000000000 - chat_id
                    
                    return chat_id, message_id
                except ValueError:
                    return None, None
        else:
            # Username link format: /username/message_id
            try:
                # For username links, we need to resolve username to chat_id later
                message_id = int(path_parts[-1])
                return None, message_id  # chat_id will be resolved using bot
            except ValueError:
                return None, None
    
    except Exception:
        return None, None
    
    return None, None

def parse_forward_request(text: str) -> Dict:
    """
    Parse forward request from user message
    
    Format:
    https://t.me/c/3586558422/1641
    https://t.me/c/3586558422/26787
    -1003586558422
    'old word' 'new word'
    'another' 'replacement'
    """
    lines = [line.strip() for line in text.strip().split('\n') if line.strip()]
    
    if len(lines) < 3:
        raise ValueError("❌ Minimum 3 lines required: start link, end link, target group")
    
    result = {
        'start_link': lines[0],
        'end_link': lines[1],
        'target_group': lines[2],
        'replacements': {}
    }
    
    # Parse replacements (lines 4+)
    for line in lines[3:]:
        line = line.strip()
        if line:
            # Try to parse 'old' 'new' format
            parts = line.split("'")
            if len(parts) >= 5:
                old_word = parts[1]
                new_word = parts[3]
                result['replacements'][old_word] = new_word
    
    return result

def extract_topic_from_caption(caption: Optional[str]) -> Optional[str]:
    """Extract topic from caption using Topic: prefix"""
    if not caption:
        return None
    
    match = re.search(r'Topic:\s*(.+)', caption, re.IGNORECASE)
    if not match:
        return None
    
    topic = match.group(1).split('\n')[0].strip()
    return topic if topic else None

def apply_replacements(text: str, replacements: Dict[str, str]) -> str:
    """Apply word replacements to text"""
    if not text or not replacements:
        return text
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    return text
