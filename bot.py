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

# Database (NeonDB)
import psycopg2
from psycopg2.extras import RealDictCursor

# Environment variables
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TELEGRAM_BOT_TOKEN = "8343304363:AAH20G_levf2X_w0AhZq4Lz3ORNV-WSmlm4"
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # Channel or group ID to post to

# Reddit configuration
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USERNAME = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD = os.getenv("REDDIT_PASSWORD")
REDDIT_USER_AGENT = "python:bf6-telegram-bot:v1.0.0 (by /u/BFHaber_Bot)"

# Database configuration (NeonDB)
DATABASE_URL = os.getenv("DATABASE_URL")
USE_DB_FOR_POSTED_IDS = bool(DATABASE_URL)
FAIL_IF_DB_UNAVAILABLE = os.getenv("FAIL_IF_DB_UNAVAILABLE", "true").lower() == "true"

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

def _db_connect():
    """Get a psycopg2 connection using DATABASE_URL."""
    try:
        dsn = DATABASE_URL
        if not dsn:
            raise ValueError("DATABASE_URL is empty")
        
        # Clean up common misconfigurations
        if dsn.startswith('DATABASE_URL='):
            dsn = dsn[13:]
        dsn = dsn.strip('\'"')
        
        conn = psycopg2.connect(dsn)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def _ensure_posted_ids_table():
    """Ensure posted_reddit_ids table exists."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_POSTED_IDS_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()

def _db_load_posted_ids():
    """Load all posted Reddit IDs from database."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM posted_reddit_ids ORDER BY created_at DESC")
            rows = cur.fetchall()
            return [row[0] for row in rows]
    finally:
        conn.close()

def _db_save_posted_id(post_id: str):
    """Save a posted Reddit ID to database."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO posted_reddit_ids (id) VALUES (%s) ON CONFLICT (id) DO NOTHING",
                (post_id,)
            )
        conn.commit()
    finally:
        conn.close()

def _db_prune_posted_ids_keep_latest(limit: int = 10):
    """Keep only the latest 'limit' records in posted_reddit_ids table."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            # Delete all but the most recent 'limit' records
            cur.execute("""
                DELETE FROM posted_reddit_ids 
                WHERE id NOT IN (
                    SELECT id FROM posted_reddit_ids 
                    ORDER BY created_at DESC 
                    LIMIT %s
                )
            """, (limit,))
        conn.commit()
        logger.info(f"Database pruned, keeping latest {limit} records")
    finally:
        conn.close()

class RedditToTelegramBot:
    def __init__(self):
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.reddit = None
        self.processed_posts = self.load_processed_posts()
        self.running = False
        
        # Initialize Reddit client
        self.init_reddit()
        
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
    
    def load_processed_posts(self) -> set:
        """Load processed post IDs from database or file fallback"""
        # 1) Try database first
        if USE_DB_FOR_POSTED_IDS:
            try:
                _ensure_posted_ids_table()
                ids = _db_load_posted_ids()
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
    
    def save_processed_post(self, post_id: str):
        """Save a single processed post ID to database or file"""
        # 1) Try database first
        if USE_DB_FOR_POSTED_IDS:
            try:
                _ensure_posted_ids_table()
                _db_save_posted_id(post_id)
                # Prune old records, keep only latest 10
                _db_prune_posted_ids_keep_latest(POSTED_IDS_RETENTION)
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
        
        # Start with just the title
        message = f"🎮 **{title}**\n\n"
        
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
            # Reddit gallery
            elif hasattr(submission, 'is_gallery') and submission.is_gallery:
                if hasattr(submission, 'media_metadata'):
                    for item_id in submission.media_metadata:
                        item = submission.media_metadata[item_id]
                        if 's' in item and 'u' in item['s']:
                            # Convert preview URL to full resolution
                            img_url = item['s']['u'].replace('preview.redd.it', 'i.redd.it')
                            img_url = img_url.split('?')[0]  # Remove query parameters
                            media_urls.append(img_url)
        
        # Add source link at the end (smaller, less prominent)
        message += f"🔗 [Kaynak]({submission.url})"
        
        return message, media_urls
    
    async def send_to_telegram(self, message: str, media_urls: list = None):
        """Send message with media to Telegram"""
        try:
            if not TELEGRAM_CHAT_ID:
                logger.warning("TELEGRAM_CHAT_ID not set, cannot send message")
                return False
            
            # Send media first if available
            if media_urls:
                for media_url in media_urls[:10]:  # Limit to 10 media items
                    try:
                        # Determine media type
                        url_lower = media_url.lower()
                        
                        if any(url_lower.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                            # Send as photo
                            await self.telegram_bot.send_photo(
                                chat_id=TELEGRAM_CHAT_ID,
                                photo=media_url,
                                caption=message if len(media_urls) == 1 else None,
                                parse_mode='Markdown'
                            )
                        elif any(url_lower.endswith(ext) for ext in ['.mp4', '.webm', '.mov']) or 'v.redd.it' in url_lower:
                            # Send as video
                            await self.telegram_bot.send_video(
                                chat_id=TELEGRAM_CHAT_ID,
                                video=media_url,
                                caption=message if len(media_urls) == 1 else None,
                                parse_mode='Markdown'
                            )
                        else:
                            # Send as document for other formats
                            await self.telegram_bot.send_document(
                                chat_id=TELEGRAM_CHAT_ID,
                                document=media_url,
                                caption=message if len(media_urls) == 1 else None,
                                parse_mode='Markdown'
                            )
                        
                        await asyncio.sleep(1)  # Rate limiting between media
                        
                    except Exception as media_error:
                        logger.warning(f"Failed to send media {media_url}: {media_error}")
                        continue
                
                # If multiple media items, send text separately
                if len(media_urls) > 1:
                    await self.telegram_bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=message,
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
            else:
                # No media, send text only
                max_length = 4096
                if len(message) > max_length:
                    # Send in chunks
                    for i in range(0, len(message), max_length):
                        chunk = message[i:i + max_length]
                        await self.telegram_bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=chunk,
                            parse_mode='Markdown',
                            disable_web_page_preview=True
                        )
                        await asyncio.sleep(1)  # Rate limiting
                else:
                    await self.telegram_bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=message,
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
            
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
                        self.save_processed_post(submission.id)
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
            conn = _db_connect()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM posted_reddit_ids")
                db_count = cur.fetchone()[0]
                cur.execute("SELECT MAX(created_at) FROM posted_reddit_ids")
                last_post = cur.fetchone()[0]
            conn.close()
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
        _ensure_posted_ids_table()
        conn = _db_connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, created_at 
                FROM posted_reddit_ids 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            rows = cur.fetchall()
        conn.close()
        
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
        "REDDIT_USERNAME", "REDDIT_PASSWORD", "TELEGRAM_CHAT_ID"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.error("Please set these variables in Render.com Environment Variables section")
        return
    
    # Test database connection if DATABASE_URL is provided
    if DATABASE_URL:
        try:
            _ensure_posted_ids_table()
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
