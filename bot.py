import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
import threading

# Reddit API
import praw
from praw.models import Submission

# Telegram Bot API
import telegram
from telegram import Bot
from telegram.error import TelegramError

# Web service for Render.com
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

# Environment variables
from dotenv import load_dotenv
import urllib.request
import tempfile
import uuid

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database (NeonDB) - Optional
try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    logger.warning("asyncpg not available, using file storage only")

# Configuration
# IMPORTANT: Read sensitive tokens from environment variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # Channel or group ID to post to

# Reddit configuration
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USERNAME = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD = os.getenv("REDDIT_PASSWORD")
REDDIT_USER_AGENT = "python:bf6-telegram-bot:v1.0.0 (by /u/BFHaber_Bot)"

# Database configuration (NeonDB)
DATABASE_URL = os.getenv("DATABASE_URL")
USE_DB_FOR_POSTED_IDS = bool(DATABASE_URL) and ASYNCPG_AVAILABLE
FAIL_IF_DB_UNAVAILABLE = os.getenv("FAIL_IF_DB_UNAVAILABLE", "false").lower() == "true"  # Default false for compatibility

# Bot settings
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # 5 minutes default
MAX_POSTS_PER_CHECK = int(os.getenv("MAX_POSTS_PER_CHECK", "5"))
SUBREDDIT_NAME = os.getenv("SUBREDDIT_NAME", "bf6_tr")
TARGET_USER = "bfhaber_bot"
POSTED_IDS_RETENTION = int(os.getenv("POSTED_IDS_RETENTION", "10"))  # Keep last 10 posts

# Storage for processed posts (fallback)
PROCESSED_POSTS_FILE = "processed_posts.json"

# Database table SQL
_POSTED_IDS_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS posted_reddit_ids (\n"
    "    id VARCHAR(50) PRIMARY KEY,\n"
    "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n"
    ")"
)

async def _db_connect():
    """Get an asyncpg connection using DATABASE_URL."""
    if not ASYNCPG_AVAILABLE:
        raise ImportError("asyncpg not available")
    
    try:
        dsn = DATABASE_URL
        if not dsn:
            raise ValueError("DATABASE_URL is empty")
        
        # Clean up common misconfigurations
        if dsn.startswith('DATABASE_URL='):
            dsn = dsn[13:]
        dsn = dsn.strip('\'"')
        
        conn = await asyncpg.connect(dsn)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

async def _ensure_posted_ids_table():
    """Ensure posted_reddit_ids table exists."""
    conn = await _db_connect()
    try:
        await conn.execute(_POSTED_IDS_TABLE_SQL)
    finally:
        await conn.close()

async def _db_load_posted_ids():
    """Load all posted Reddit IDs from database."""
    conn = await _db_connect()
    try:
        rows = await conn.fetch("SELECT id FROM posted_reddit_ids ORDER BY created_at DESC")
        return [row['id'] for row in rows]
    finally:
        await conn.close()

async def _db_save_posted_id(post_id: str):
    """Save a posted Reddit ID to database."""
    conn = await _db_connect()
    try:
        await conn.execute(
            "INSERT INTO posted_reddit_ids (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
            post_id
        )
    finally:
        await conn.close()

async def _db_prune_posted_ids_keep_latest(limit: int = 10):
    """Keep only the latest 'limit' records in posted_reddit_ids table."""
    conn = await _db_connect()
    try:
        # Delete all but the most recent 'limit' records
        await conn.execute("""
            DELETE FROM posted_reddit_ids 
            WHERE id NOT IN (
                SELECT id FROM posted_reddit_ids 
                ORDER BY created_at DESC 
                LIMIT $1
            )
        """, limit)
        logger.info(f"Database pruned, keeping latest {limit} records")
    finally:
        await conn.close()

class RedditToTelegramBot:
    def __init__(self):
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.reddit = None
        self.processed_posts = set()  # Will be loaded async
        self.running = False
        
        # Initialize Reddit client
        self.init_reddit()
    
    async def initialize_async(self):
        """Initialize async components"""
        self.processed_posts = await self.load_processed_posts()
        
    def init_reddit(self):
        """Initialize Reddit client"""
        try:
            self.reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                username=REDDIT_USERNAME,
                password=REDDIT_PASSWORD,
                user_agent=REDDIT_USER_AGENT,
                ratelimit_seconds=60,
                timeout=30,
                check_for_updates=False,
                check_for_async=False
            )
            logger.info("Reddit client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Reddit client: {e}")
            raise
    
    async def load_processed_posts(self) -> set:
        """Load processed post IDs from database or file fallback"""
        # 1) Try database first
        if USE_DB_FOR_POSTED_IDS:
            try:
                await _ensure_posted_ids_table()
                ids = await _db_load_posted_ids()
                logger.info(f"Loaded {len(ids)} processed post IDs from database")
                return set(ids)
            except Exception as e:
                if FAIL_IF_DB_UNAVAILABLE:
                    raise RuntimeError(f"Database required but unavailable (load): {e}")
                logger.warning(f"Database load failed, falling back to file: {e}")
        
        # 2) File fallback
        try:
            if os.path.exists(PROCESSED_POSTS_FILE):
                with open(PROCESSED_POSTS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    posts = set(data.get('posts', []))
                    logger.info(f"Loaded {len(posts)} processed post IDs from file")
                    return posts
        except Exception as e:
            logger.error(f"Error loading processed posts from file: {e}")
        return set()
    
    async def save_processed_post(self, post_id: str):
        """Save a single processed post ID to database or file"""
        # 1) Try database first
        if USE_DB_FOR_POSTED_IDS:
            try:
                await _ensure_posted_ids_table()
                await _db_save_posted_id(post_id)
                # Prune old records, keep only latest 10
                await _db_prune_posted_ids_keep_latest(POSTED_IDS_RETENTION)
                logger.info(f"Saved post ID to database: {post_id}")
                return
            except Exception as e:
                if FAIL_IF_DB_UNAVAILABLE:
                    raise RuntimeError(f"Database required but unavailable (save): {e}")
                logger.warning(f"Database save failed, falling back to file: {e}")
        
        # 2) File fallback
        try:
            self.processed_posts.add(post_id)
            # Keep only last POSTED_IDS_RETENTION posts
            posts_list = list(self.processed_posts)[-POSTED_IDS_RETENTION:]
            self.processed_posts = set(posts_list)
            
            data = {
                'posts': posts_list,
                'last_updated': datetime.now().isoformat()
            }
            with open(PROCESSED_POSTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved post ID to file: {post_id}")
        except Exception as e:
            logger.error(f"Error saving processed post to file: {e}")
    
    def get_bot_comment_continuation(self, submission: Submission) -> str:
        """Get BFHaber_Bot's comment if post text is truncated"""
        try:
            # Refresh submission to get comments
            submission.comments.replace_more(limit=0)
            
            for comment in submission.comments:
                if (comment.author and 
                    comment.author.name.lower() == TARGET_USER.lower() and
                    len(comment.body.strip()) > 10):  # Meaningful comment
                    return comment.body.strip()
            return ""
        except Exception as e:
            logger.warning(f"Error getting bot comment: {e}")
            return ""
    
    def format_post_for_telegram(self, submission: Submission) -> tuple[str, list]:
        """Format Reddit post for Telegram. Returns (message, media_urls)"""
        title = submission.title
        
        # Start with just the title (no emoji)
        message = f"**{title}**\n\n"
        
        # Get post content
        content = ""
        if submission.selftext and len(submission.selftext.strip()) > 0:
            content = submission.selftext.strip()
            
            # Check if text is truncated (ends with ...)
            if content.endswith("...") or len(content) > 800:
                # Try to get continuation from bot's comment
                bot_comment = self.get_bot_comment_continuation(submission)
                if bot_comment:
                    # Remove the "..." and add the comment
                    if content.endswith("..."):
                        content = content[:-3].strip()
                    content += f"\n\n{bot_comment}"
            
            message += f"{content}\n\n"
        
        # Get media URLs
        media_urls = []
        
        # Check for images/videos in post
        if hasattr(submission, 'url') and submission.url:
            url = submission.url.lower()
            # Image formats
            if any(url.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                media_urls.append(submission.url)
            # Video formats  
            elif any(url.endswith(ext) for ext in ['.mp4', '.webm', '.mov']):
                media_urls.append(submission.url)
            # Reddit video
            elif 'v.redd.it' in url:
                media_urls.append(submission.url)
        
        # Improve Reddit video handling: prefer fallback_url (direct mp4)
        try:
            if getattr(submission, 'is_video', False) or ('v.redd.it' in (submission.url.lower() if hasattr(submission, 'url') and submission.url else '')):
                rv = None
                if getattr(submission, 'media', None) and isinstance(submission.media, dict):
                    rv = submission.media.get('reddit_video')
                if not rv and getattr(submission, 'secure_media', None) and isinstance(submission.secure_media, dict):
                    rv = submission.secure_media.get('reddit_video')
                if rv and isinstance(rv, dict) and rv.get('fallback_url'):
                    fallback = rv.get('fallback_url')
                    # Put fallback first so sender tries it first
                    media_urls = [fallback] + [u for u in media_urls if u != fallback]
        except Exception as e:
            logger.warning(f"Error extracting reddit video fallback: {e}")

        # Reddit gallery
        if hasattr(submission, 'is_gallery') and submission.is_gallery:
            if hasattr(submission, 'media_metadata'):
                for item_id in submission.media_metadata:
                    item = submission.media_metadata[item_id]
                    if 's' in item and 'u' in item['s']:
                        # Convert preview URL to full resolution
                        img_url = item['s']['u'].replace('preview.redd.it', 'i.redd.it')
                        img_url = img_url.split('?')[0]  # Remove query parameters
                        media_urls.append(img_url)
        
        # No source link appended
        
        return message, media_urls
    
    async def send_to_telegram(self, message: str, media_urls: list = None):
        """Send message with media to Telegram"""
        try:
            if not TELEGRAM_CHAT_ID:
                logger.warning("TELEGRAM_CHAT_ID not set, cannot send message")
                return False
            
            # Send media first if available
            if media_urls:
                sent_fallback = False
                caption_attached = False
                for media_url in media_urls[:10]:  # Limit to 10 media items
                    try:
                        # Determine media type
                        url_lower = media_url.lower()
                        
                        if any(url_lower.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                            # Send as photo
                            try:
                                await self.telegram_bot.send_photo(
                                    chat_id=TELEGRAM_CHAT_ID,
                                    photo=media_url,
                                    caption=message if len(media_urls) == 1 else None,
                                    parse_mode='Markdown'
                                )
                            except TelegramError as te:
                                logger.warning(f"Photo caption Markdown failed, retrying without parse_mode: {te}")
                                await self.telegram_bot.send_photo(
                                    chat_id=TELEGRAM_CHAT_ID,
                                    photo=media_url,
                                    caption=message if len(media_urls) == 1 else None,
                                    parse_mode=None
                                )
                        elif any(url_lower.endswith(ext) for ext in ['.mp4', '.webm', '.mov']) or 'v.redd.it' in url_lower:
                            # Send as video: try to download and upload to Telegram (more reliable)
                            video_path = None
                            download_attempts = 4
                            last_error = None
                            for attempt in range(1, download_attempts + 1):
                                try:
                                    def _download():
                                        tmp_dir = tempfile.gettempdir()
                                        ext = '.mp4'
                                        path = os.path.join(tmp_dir, f"reddit_vid_{uuid.uuid4().hex}{ext}")
                                        # Use browser-like headers; some CDNs block default urllib UA or missing referer
                                        req = urllib.request.Request(media_url, headers={
                                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36',
                                            'Referer': 'https://www.reddit.com/',
                                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                                            'Accept-Language': 'en-US,en;q=0.9'
                                        })
                                        with urllib.request.urlopen(req, timeout=120) as r, open(path, 'wb') as f:
                                            f.write(r.read())
                                        return path
                                    video_path = await asyncio.to_thread(_download)
                                    if os.path.exists(video_path) and os.path.getsize(video_path) > 0:
                                        break
                                except Exception as de:
                                    last_error = de
                                    logger.warning(f"Video download attempt {attempt}/{download_attempts} failed: {de}")
                                    await asyncio.sleep(2 * attempt)

                            if video_path and os.path.exists(video_path):
                                try:
                                    with open(video_path, 'rb') as f:
                                        try:
                                            # Always include caption for video to avoid textless video
                                            await self.telegram_bot.send_video(
                                                chat_id=TELEGRAM_CHAT_ID,
                                                video=f,
                                                caption=message,
                                                parse_mode='Markdown'
                                            )
                                        except TelegramError as te:
                                            logger.warning(f"Video caption Markdown failed, retrying without parse_mode: {te}")
                                            await self.telegram_bot.send_video(
                                                chat_id=TELEGRAM_CHAT_ID,
                                                video=f,
                                                caption=message,
                                                parse_mode=None
                                            )
                                        # Mark that we already sent caption with the video
                                        caption_attached = True
                                finally:
                                    try:
                                        os.remove(video_path)
                                    except Exception:
                                        pass
                            else:
                                # Could not download after retries: send title/content only, and include video source URL
                                fallback_text = message + ("\n\nVideo: " + media_url if media_url else "")
                                try:
                                    await self.telegram_bot.send_message(
                                        chat_id=TELEGRAM_CHAT_ID,
                                        text=fallback_text,
                                        parse_mode='Markdown',
                                        disable_web_page_preview=False
                                    )
                                except TelegramError as te:
                                    logger.warning(f"Fallback text Markdown failed, retrying without parse_mode: {te}")
                                    await self.telegram_bot.send_message(
                                        chat_id=TELEGRAM_CHAT_ID,
                                        text=fallback_text,
                                        parse_mode=None,
                                        disable_web_page_preview=False
                                    )
                                # Ensure only a single Telegram message is sent in this failure scenario
                                sent_fallback = True
                                break
                        else:
                            # Send as document for other formats
                            try:
                                await self.telegram_bot.send_document(
                                    chat_id=TELEGRAM_CHAT_ID,
                                    document=media_url,
                                    caption=message if len(media_urls) == 1 else None,
                                    parse_mode='Markdown'
                                )
                            except TelegramError as te:
                                logger.warning(f"Document caption Markdown failed, retrying without parse_mode: {te}")
                                await self.telegram_bot.send_document(
                                    chat_id=TELEGRAM_CHAT_ID,
                                    document=media_url,
                                    caption=message if len(media_urls) == 1 else None,
                                    parse_mode=None
                                )
                        
                        await asyncio.sleep(1)  # Rate limiting between media
                        
                    except Exception as media_error:
                        logger.warning(f"Failed to send media {media_url}: {media_error}")
                        continue
                
                # If multiple media items, send text separately unless caption already attached to the video
                if len(media_urls) > 1 and not sent_fallback and not caption_attached:
                    try:
                        await self.telegram_bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=message,
                            parse_mode='Markdown',
                            disable_web_page_preview=True
                        )
                    except TelegramError as te:
                        logger.warning(f"Text Markdown failed, retrying without parse_mode: {te}")
                        try:
                            await self.telegram_bot.send_message(
                                chat_id=TELEGRAM_CHAT_ID,
                                text=message,
                                parse_mode=None,
                                disable_web_page_preview=True
                            )
                        except TelegramError as te2:
                            logger.warning(f"Text retry without parse_mode failed: {te2}")
                            return False
            else:
                # No media, send text only
                max_length = 4096
                if len(message) > max_length:
                    # Send in chunks
                    for i in range(0, len(message), max_length):
                        chunk = message[i:i + max_length]
                        try:
                            await self.telegram_bot.send_message(
                                chat_id=TELEGRAM_CHAT_ID,
                                text=chunk,
                                parse_mode='Markdown',
                                disable_web_page_preview=True
                            )
                        except TelegramError as te:
                            logger.warning(f"Chunk Markdown failed, retrying without parse_mode: {te}")
                            try:
                                await self.telegram_bot.send_message(
                                    chat_id=TELEGRAM_CHAT_ID,
                                    text=chunk,
                                    parse_mode=None,
                                    disable_web_page_preview=True
                                )
                            except TelegramError as te2:
                                logger.warning(f"Chunk retry without parse_mode failed: {te2}")
                                return False
                else:
                    try:
                        await self.telegram_bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=message,
                            parse_mode='Markdown',
                            disable_web_page_preview=True
                        )
                    except TelegramError as te:
                        logger.warning(f"Text Markdown failed, retrying without parse_mode: {te}")
                        try:
                            await self.telegram_bot.send_message(
                                chat_id=TELEGRAM_CHAT_ID,
                                text=message,
                                parse_mode=None,
                                disable_web_page_preview=True
                            )
                        except TelegramError as te2:
                            logger.warning(f"Text retry without parse_mode failed: {te2}")
                            return False
            
            logger.info("Message sent to Telegram successfully")
            return True
            
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending to Telegram: {e}")
            return False
    
    def get_new_posts_from_user(self) -> List[Submission]:
        """Get new posts from target user"""
        try:
            subreddit = self.reddit.subreddit(SUBREDDIT_NAME)
            new_posts = []
            
            # Get recent posts from the subreddit
            for submission in subreddit.new(limit=50):
                # Check if post is from target user
                if (submission.author and 
                    submission.author.name.lower() == TARGET_USER.lower() and
                    submission.id not in self.processed_posts):
                    
                    # Check if post is recent (last 24 hours)
                    post_time = datetime.fromtimestamp(submission.created_utc)
                    if datetime.now() - post_time < timedelta(hours=24):
                        new_posts.append(submission)
            
            # Sort by creation time (oldest first)
            new_posts.sort(key=lambda x: x.created_utc)
            
            # Limit number of posts
            return new_posts[:MAX_POSTS_PER_CHECK]
            
        except Exception as e:
            logger.error(f"Error getting posts from Reddit: {e}")
            return []
    
    async def process_new_posts(self):
        """Process new posts and send to Telegram"""
        try:
            new_posts = self.get_new_posts_from_user()
            
            if not new_posts:
                logger.info("No new posts found")
                return
            
            logger.info(f"Found {len(new_posts)} new posts")
            
            for submission in new_posts:
                try:
                    # Format message and get media URLs
                    message, media_urls = self.format_post_for_telegram(submission)
                    
                    # Send to Telegram with media
                    success = await self.send_to_telegram(message, media_urls)
                    
                    if success:
                        # Mark as processed (save to database/file)
                        await self.save_processed_post(submission.id)
                        # Also add to memory set for current session
                        self.processed_posts.add(submission.id)
                        logger.info(f"Processed post: {submission.id} - {submission.title[:50]}...")
                    else:
                        logger.error(f"Failed to send post: {submission.id}")
                    
                    # Rate limiting between posts
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing post {submission.id}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error in process_new_posts: {e}")
    
    async def run_bot_loop(self):
        """Main bot loop"""
        logger.info("Starting Reddit to Telegram bot...")
        
        # Initialize async components
        await self.initialize_async()
        
        self.running = True
        
        while self.running:
            try:
                await self.process_new_posts()
                
                # Wait for next check
                logger.info(f"Waiting {CHECK_INTERVAL} seconds until next check...")
                await asyncio.sleep(CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in bot loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    def stop(self):
        """Stop the bot"""
        logger.info("Stopping bot...")
        self.running = False

# Global bot instance
bot_instance = None

# FastAPI app for web service
app = FastAPI(title="Reddit to Telegram Bot", version="1.0.0")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "ok", "message": "Reddit to Telegram Bot is running"}

@app.get("/ping")
async def ping():
    """Simple ping endpoint for uptime checks"""
    return {"status": "alive"}

@app.head("/ping")
async def ping_head():
    """HEAD support for /ping (returns 200 without body)"""
    return PlainTextResponse(status_code=200, content="")

@app.get("/health")
async def health_check():
    """Health check for Render.com"""
    global bot_instance
    status = "running" if bot_instance and bot_instance.running else "stopped"
    return {
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "processed_posts": len(bot_instance.processed_posts) if bot_instance else 0
    }

@app.head("/health")
async def health_head():
    """HEAD support for /health (returns 200 without body)"""
    return PlainTextResponse(status_code=200, content="")

@app.get("/stats")
async def get_stats():
    """Get bot statistics"""
    global bot_instance
    if not bot_instance:
        return {"error": "Bot not initialized"}
    
    # Get database stats if available
    db_stats = {}
    if USE_DB_FOR_POSTED_IDS:
        try:
            conn = await _db_connect()
            db_count = await conn.fetchval("SELECT COUNT(*) FROM posted_reddit_ids")
            last_post = await conn.fetchval("SELECT MAX(created_at) FROM posted_reddit_ids")
            await conn.close()
            db_stats = {
                "database_posts_count": db_count,
                "last_post_time": str(last_post) if last_post else None,
                "database_connected": True
            }
        except Exception as e:
            db_stats = {
                "database_connected": False,
                "database_error": str(e)
            }
    
    return {
        "processed_posts_count": len(bot_instance.processed_posts),
        "target_user": TARGET_USER,
        "subreddit": SUBREDDIT_NAME,
        "check_interval": CHECK_INTERVAL,
        "running": bot_instance.running,
        "database_url_set": bool(DATABASE_URL),
        **db_stats
    }

@app.get("/test-db")
async def test_database():
    """Test database connection and show recent posts"""
    if not DATABASE_URL:
        return {"error": "No DATABASE_URL configured"}
    
    try:
        await _ensure_posted_ids_table()
        conn = await _db_connect()
        rows = await conn.fetch("""
            SELECT id, created_at 
            FROM posted_reddit_ids 
            ORDER BY created_at DESC 
            LIMIT 10
        """)
        await conn.close()
        
        return {
            "status": "success",
            "message": "Database connection successful",
            "recent_posts": [
                {"id": row[0], "created_at": str(row[1])} 
                for row in rows
            ],
            "total_posts": len(rows)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Database connection failed: {e}"
        }

@app.post("/webhook")
async def webhook(request: Request):
    """Webhook endpoint (for future use)"""
    return {"status": "received"}

@app.post("/test-telegram")
async def test_telegram():
    """Send a test message to the configured Telegram chat to verify token/chat id/permissions"""
    try:
        if not TELEGRAM_BOT_TOKEN:
            return {"status": "error", "message": "TELEGRAM_BOT_TOKEN not set"}
        if not TELEGRAM_CHAT_ID:
            return {"status": "error", "message": "TELEGRAM_CHAT_ID not set"}
        global bot_instance
        if not bot_instance:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="Test message from Reddit->Telegram service", disable_web_page_preview=True)
        else:
            await bot_instance.telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="Test message from Reddit->Telegram service", disable_web_page_preview=True)
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Telegram test failed: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/process-once")
async def process_once(request: Request):
    """Process a single Reddit URL immediately, bypassing normal filters.
    Body JSON: {"url": str, "save": bool|optional}
    - save=false (default): do not store ID, so it won't affect future scans
    - save=true: store ID to prevent future duplicates
    """
    global bot_instance
    if not bot_instance:
        return {"error": "Bot not initialized"}
    try:
        payload = await request.json()
        url = (payload.get("url") or "").strip()
        save = bool(payload.get("save", False))
        if not url:
            return {"status": "error", "message": "'url' is required"}

        # Fetch submission directly by URL
        submission = bot_instance.reddit.submission(url=url)
        # Ensure data is loaded
        submission._fetch()

        # Format and send using existing logic (with robust media handling)
        message, media_urls = bot_instance.format_post_for_telegram(submission)
        sent = await bot_instance.send_to_telegram(message, media_urls)

        if sent and save:
            try:
                await bot_instance.save_processed_post(submission.id)
                bot_instance.processed_posts.add(submission.id)
            except Exception as se:
                logger.warning(f"Saving processed post failed: {se}")

        return {
            "status": "success" if sent else "failed",
            "id": submission.id,
            "title": submission.title,
            "permalink": f"https://reddit.com{submission.permalink}",
            "saved": bool(save and sent)
        }
    except Exception as e:
        logger.error(f"Error in process_once: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/trigger-scan")
async def trigger_scan():
    """Trigger a one-off scan and forward of new posts without waiting for the loop interval"""
    global bot_instance
    if not bot_instance:
        return {"error": "Bot not initialized"}
    # Run scan in the web server event loop
    try:
        asyncio.create_task(bot_instance.process_new_posts())
        return {"status": "scheduled"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/post_to_reddit")
async def post_to_reddit(request: Request):
    """Submit a post to Reddit.
    Body JSON: {"title": str, "selftext": str|optional, "url": str|optional, "subreddit": str|optional}
    """
    global bot_instance
    if not bot_instance:
        return {"error": "Bot not initialized"}
    try:
        payload = await request.json()
        title = (payload.get("title") or "").strip()
        selftext = (payload.get("selftext") or "").strip()
        url = (payload.get("url") or "").strip()
        subreddit_name = (payload.get("subreddit") or SUBREDDIT_NAME).strip()

        if not title:
            return {"status": "error", "message": "'title' is required"}
        if not selftext and not url:
            return {"status": "error", "message": "Provide either 'selftext' or 'url'"}

        subreddit = bot_instance.reddit.subreddit(subreddit_name)

        def _submit():
            if url:
                return subreddit.submit(title=title, url=url, resubmit=True)
            else:
                return subreddit.submit(title=title, selftext=selftext)

        submission = await asyncio.to_thread(_submit)
        return {
            "status": "success",
            "id": submission.id,
            "title": submission.title,
            "permalink": f"https://reddit.com{submission.permalink}",
            "shortlink": submission.shortlink,
            "subreddit": subreddit_name,
        }
    except Exception as e:
        logger.error(f"Error posting to Reddit: {e}")
        return {"status": "error", "message": str(e)}

def start_bot_in_thread():
    """Start bot in separate thread"""
    global bot_instance
    
    try:
        bot_instance = RedditToTelegramBot()
        
        # Run bot loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(bot_instance.run_bot_loop())
        
    except Exception as e:
        logger.error(f"Error starting bot: {e}")

def main():
    """Main function"""
    logger.info("Starting Reddit to Telegram Bot service...")
    
    # Validate required environment variables
    required_vars = [
        "REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", 
        "REDDIT_USERNAME", "REDDIT_PASSWORD", 
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.error("Please set these variables in Render.com Environment Variables section")
        return
    
    # Test database connection if DATABASE_URL is provided
    if DATABASE_URL:
        try:
            # Test database connection in async context
            async def test_db():
                await _ensure_posted_ids_table()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(test_db())
            loop.close()
            
            logger.info("✅ Database connection successful")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            if FAIL_IF_DB_UNAVAILABLE:
                logger.error("Database is required but unavailable. Exiting.")
                return
            logger.warning("Continuing without database (file fallback)")
    else:
        logger.warning("No DATABASE_URL provided, using file storage")
    
    # Start bot in background thread
    bot_thread = threading.Thread(target=start_bot_in_thread, daemon=True)
    bot_thread.start()
    
    # Start web service
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"Starting web service on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")

if __name__ == "__main__":
    main()
