import os, requests, time, logging, sqlite3, json, re, threading, subprocess
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import cloudinary, cloudinary.uploader
import yt_dlp
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, MessageHandler, filters, CommandHandler,
    CallbackQueryHandler, ContextTypes, ConversationHandler
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import asyncio
try:
    from google import genai as genai_new
    GENAI_NEW = True
except ImportError:
    GENAI_NEW = False
    genai_new = None

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN       = os.environ.get("BOT_TOKEN", "")
IG_USER_ID      = os.environ.get("IG_USER_ID", "")
IG_ACCESS_TOKEN = os.environ.get("IG_ACCESS_TOKEN", "")
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY", "")
YT_CLIENT_ID    = os.environ.get("YOUTUBE_CLIENT_ID", "")
YT_CLIENT_SECRET= os.environ.get("YOUTUBE_CLIENT_SECRET", "")
YT_REFRESH_TOKEN= os.environ.get("YOUTUBE_REFRESH_TOKEN", "")
FB_PAGE_ID         = os.environ.get("FB_PAGE_ID", "")
FB_PAGE_ACCESS_TOKEN = os.environ.get("FB_PAGE_ACCESS_TOKEN", "")
ADMIN_USER_ID   = int(os.environ.get("ADMIN_USER_ID", "0") or "0")

cloudinary.config(
    cloud_name=os.environ.get("CLOUDINARY_CLOUD_NAME", ""),
    api_key=os.environ.get("CLOUDINARY_API_KEY", ""),
    api_secret=os.environ.get("CLOUDINARY_API_SECRET", "")
)

gemini_client = None
if GEMINI_API_KEY and GENAI_NEW and genai_new:
    gemini_client = genai_new.Client(api_key=GEMINI_API_KEY)

_DATA_DIR = os.environ.get("BOT_DATA_DIR", ".")
os.makedirs(_DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(_DATA_DIR, "bot_data.db")

SUPPORTED_PLATFORMS = {
    "tiktok.com": "TikTok",
    "youtube.com": "YouTube",
    "youtu.be": "YouTube",
    "instagram.com": "Instagram",
    "facebook.com": "Facebook",
    "fb.watch": "Facebook",
    "twitter.com": "Twitter/X",
    "x.com": "Twitter/X",
    "reddit.com": "Reddit",
    "vimeo.com": "Vimeo",
    "dailymotion.com": "Dailymotion",
    "snapchat.com": "Snapchat",
}

WAITING_SCHEDULE_TIME = 1
WAITING_CAPTION = 2
WAITING_HASHTAGS = 3
WAITING_AI_PROMPT = 4

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT,
        platform TEXT,
        caption TEXT,
        ig_post_id TEXT,
        status TEXT,
        scheduled_at TEXT,
        created_at TEXT,
        user_id INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS analytics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        total_posts INTEGER DEFAULT 0,
        successful_posts INTEGER DEFAULT 0,
        failed_posts INTEGER DEFAULT 0,
        scheduled_posts INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS pending_videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        url TEXT,
        platform TEXT,
        caption TEXT,
        video_path TEXT,
        cloudinary_url TEXT,
        created_at TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT,
        platform TEXT,
        caption TEXT,
        cloudinary_url TEXT,
        user_id INTEGER,
        chat_id INTEGER,
        added_at TEXT,
        position INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        username    TEXT,
        full_name   TEXT,
        role        TEXT DEFAULT 'pending',
        added_at    TEXT,
        added_by    INTEGER
    )''')
    defaults = [
        ("default_caption_template",""),
        ("timezone",               "Africa/Cairo"),
        ("notify_success",         "true"),
        ("notify_fail",            "true"),
        ("ai_enhance_caption",     "false"),
        ("queue_interval_minutes", "120"),
        ("queue_is_running",       "false"),
        ("queue_chat_id",          ""),
        ("max_video_duration",     "0"),
        ("auto_vertical",          "false"),
        ("auto_first_comment",     "false"),
        ("youtube_shorts",         "false"),
        ("facebook_page",          "false"),
        ("require_approval",       "false"),
    ]
    for key, val in defaults:
        c.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (key, val))
    conn.commit()
    conn.close()

def get_setting(key):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def set_setting(key, value):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()

def get_ig_token():
    """يقرأ توكن إنستجرام من قاعدة البيانات أولاً، ثم من متغيرات البيئة"""
    return get_setting("ig_access_token_override") or IG_ACCESS_TOKEN

def get_fb_token():
    """يفضّل التوكن المخزن في قاعدة البيانات على المتغير البيئي"""
    return get_setting("fb_page_token_override") or FB_PAGE_ACCESS_TOKEN

def get_fb_page_id():
    """يفضّل Page ID المخزن في قاعدة البيانات على المتغير البيئي"""
    return get_setting("fb_page_id_override") or FB_PAGE_ID

def get_yt_refresh_token():
    """يقرأ refresh token يوتيوب من قاعدة البيانات أولاً، ثم من متغيرات البيئة"""
    return get_setting("yt_refresh_token_override") or YT_REFRESH_TOKEN

# ── إدارة المستخدمين ──────────────────────────────────────────────

def db_get_user(telegram_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT telegram_id, username, full_name, role FROM users WHERE telegram_id=?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    return row

def db_register_user(telegram_id, username, full_name, role="pending"):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO users (telegram_id, username, full_name, role, added_at) VALUES (?,?,?,?,?)",
        (telegram_id, username or "", full_name or "", role, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()

def db_set_user_role(telegram_id, role):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE users SET role=? WHERE telegram_id=?", (role, telegram_id))
    conn.commit()
    conn.close()

def db_get_all_users():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT telegram_id, username, full_name, role, added_at FROM users ORDER BY added_at DESC")
    rows = c.fetchall()
    conn.close()
    return rows

def is_admin(telegram_id):
    """تحقق إذا كان المستخدم admin"""
    if ADMIN_USER_ID and telegram_id == ADMIN_USER_ID:
        return True
    user = db_get_user(telegram_id)
    return user is not None and user[3] == "admin"

def get_user_role(telegram_id):
    """يرجع دور المستخدم: admin / user / pending / blocked / unknown"""
    if ADMIN_USER_ID and telegram_id == ADMIN_USER_ID:
        return "admin"
    user = db_get_user(telegram_id)
    return user[3] if user else "unknown"

def is_allowed(telegram_id):
    """تحقق إذا كان المستخدم مسموح له بالاستخدام (admin أو user)"""
    role = get_user_role(telegram_id)
    return role in ("admin", "user")

async def notify_admin_new_user(context, telegram_id, username, full_name):
    """يُرسل للـ admin إشعاراً بمستخدم جديد بانتظار الموافقة"""
    if not ADMIN_USER_ID:
        return
    kb = [
        [
            InlineKeyboardButton("✅ موافقة", callback_data=f"usr_approve_{telegram_id}"),
            InlineKeyboardButton("❌ رفض",    callback_data=f"usr_block_{telegram_id}"),
        ]
    ]
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=(
                f"👤 مستخدم جديد يطلب الوصول:\n\n"
                f"🆔 ID: {telegram_id}\n"
                f"👤 اسم المستخدم: @{username or 'بدون'}\n"
                f"📛 الاسم: {full_name or 'بدون'}\n\n"
                f"هل تريد السماح له باستخدام البوت؟"
            ),
            reply_markup=InlineKeyboardMarkup(kb)
        )
    except Exception as e:
        logger.error(f"notify_admin error: {e}")

def log_post(url, platform, caption, status, ig_post_id=None, scheduled_at=None, user_id=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute(
        "INSERT INTO posts (url, platform, caption, ig_post_id, status, scheduled_at, created_at, user_id) VALUES (?,?,?,?,?,?,?,?)",
        (url, platform, caption, ig_post_id, status, scheduled_at, datetime.now().isoformat(), user_id)
    )
    c.execute("INSERT OR IGNORE INTO analytics (date, total_posts, successful_posts, failed_posts, scheduled_posts) VALUES (?,0,0,0,0)", (today,))
    c.execute("UPDATE analytics SET total_posts = total_posts + 1 WHERE date=?", (today,))
    if status == "success":
        c.execute("UPDATE analytics SET successful_posts = successful_posts + 1 WHERE date=?", (today,))
    elif status == "failed":
        c.execute("UPDATE analytics SET failed_posts = failed_posts + 1 WHERE date=?", (today,))
    elif status == "scheduled":
        c.execute("UPDATE analytics SET scheduled_posts = scheduled_posts + 1 WHERE date=?", (today,))
    conn.commit()
    conn.close()

def detect_platform(url):
    for domain, name in SUPPORTED_PLATFORMS.items():
        if domain in url:
            return name
    return "Unknown"

def check_duplicate(url):
    """يرجع بيانات المنشور السابق لو الرابط اتنشر قبل كده، وإلا None"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, created_at, platform FROM posts WHERE url=? AND status='success' LIMIT 1",
        (url,)
    )
    row = c.fetchone()
    conn.close()
    return row

def convert_to_vertical(video_path):
    """يحوّل الفيديو لصيغة عمودية 9:16 (1080x1920) بخلفية ضبابية"""
    try:
        out_path = video_path.replace(".mp4", "_vertical.mp4")
        cmd = [
            "ffmpeg", "-y", "-i", video_path,
            "-vf",
            "scale=1080:1920:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,"
            "setsar=1",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            out_path
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=300)
        if result.returncode == 0 and os.path.exists(out_path):
            cleanup_file(video_path)
            return out_path
        else:
            logger.error(f"ffmpeg error: {result.stderr.decode()[:500]}")
            return video_path
    except Exception as e:
        logger.error(f"convert_to_vertical error: {e}")
        return video_path

def post_first_comment(ig_media_id, comment_text):
    """يضيف أول تعليق على المنشور بعد النشر مباشرة"""
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v18.0/{ig_media_id}/comments",
            data={"message": comment_text, "access_token": IG_ACCESS_TOKEN},
            timeout=30
        ).json()
        return "id" in resp
    except Exception as e:
        logger.error(f"first_comment error: {e}")
        return False

def get_youtube_access_token():
    """يجدد access token من refresh token (يقرأ التوكن من DB أولاً)"""
    rt = get_yt_refresh_token()
    if not (YT_CLIENT_ID and YT_CLIENT_SECRET and rt):
        return None
    try:
        r = requests.post("https://oauth2.googleapis.com/token", data={
            "client_id":     YT_CLIENT_ID,
            "client_secret": YT_CLIENT_SECRET,
            "refresh_token": rt,
            "grant_type":    "refresh_token",
        }, timeout=30).json()
        return r.get("access_token")
    except Exception as e:
        logger.error(f"YouTube token error: {e}")
        return None

def post_to_youtube_shorts(video_path, title, description):
    """يرفع الفيديو على YouTube Shorts"""
    access_token = get_youtube_access_token()
    if not access_token:
        return None, "يجب إعداد بيانات YouTube أولاً (YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN)"
    try:
        yt_title       = (title or "Shorts Video")[:100]
        yt_description = f"{description or ''}\n\n#Shorts"[:5000]
        metadata = json.dumps({
            "snippet": {
                "title":       yt_title,
                "description": yt_description,
                "categoryId":  "22",
            },
            "status": {"privacyStatus": "public"},
        })
        with open(video_path, "rb") as vf:
            r = requests.post(
                "https://www.googleapis.com/upload/youtube/v3/videos"
                "?part=snippet,status&uploadType=multipart",
                headers={"Authorization": f"Bearer {access_token}"},
                files={
                    "metadata": ("metadata", metadata, "application/json; charset=UTF-8"),
                    "video":    ("video.mp4", vf, "video/mp4"),
                },
                timeout=300
            ).json()
        if "id" in r:
            return r["id"], None
        return None, r.get("error", {}).get("message", "خطأ غير معروف")
    except Exception as e:
        logger.error(f"YouTube upload error: {e}")
        return None, str(e)

def post_to_facebook_page(video_url, description):
    """ينشر فيديو على صفحة فيسبوك باستخدام رابط Cloudinary"""
    page_id = get_fb_page_id()
    token   = get_fb_token()
    if not (page_id and token):
        return None, "يجب إعداد بيانات فيسبوك أولاً (FB_PAGE_ID, FB_PAGE_ACCESS_TOKEN)"
    try:
        r = requests.post(
            f"https://graph.facebook.com/v18.0/{page_id}/videos",
            data={
                "file_url":     video_url,
                "description":  (description or "")[:5000],
                "access_token": token
            },
            timeout=180
        ).json()
        if "id" in r:
            return r["id"], None
        return None, r.get("error", {}).get("message", "خطأ غير معروف")
    except Exception as e:
        logger.error(f"Facebook upload error: {e}")
        return None, str(e)

def queue_add(url, platform, caption, cloudinary_url, user_id, chat_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(position),0)+1 FROM queue")
    pos = c.fetchone()[0]
    c.execute(
        "INSERT INTO queue (url,platform,caption,cloudinary_url,user_id,chat_id,added_at,position) VALUES (?,?,?,?,?,?,?,?)",
        (url, platform, caption, cloudinary_url, user_id, chat_id, datetime.now().isoformat(), pos)
    )
    conn.commit()
    conn.close()

def queue_get_all():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id,platform,caption,added_at FROM queue ORDER BY position ASC")
    rows = c.fetchall()
    conn.close()
    return rows

def queue_count():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM queue")
    n = c.fetchone()[0]
    conn.close()
    return n

def queue_pop():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id,url,platform,caption,cloudinary_url,user_id,chat_id FROM queue ORDER BY position ASC LIMIT 1")
    row = c.fetchone()
    if row:
        c.execute("DELETE FROM queue WHERE id=?", (row[0],))
        conn.commit()
    conn.close()
    return row

def queue_remove(item_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM queue WHERE id=?", (item_id,))
    conn.commit()
    conn.close()

def queue_clear():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM queue")
    conn.commit()
    conn.close()

INTERVAL_OPTIONS = [
    ("30 دقيقة",  30),
    ("ساعة",      60),
    ("ساعتين",   120),
    ("3 ساعات",  180),
    ("6 ساعات",  360),
    ("12 ساعة",  720),
    ("يوم كامل",1440),
]

def download_video(url):
    output_path = f"temp_{int(time.time())}.mp4"
    ydl_opts = {
        'outtmpl': output_path.replace('.mp4', '.%(ext)s'),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'quiet': True,
        'no_warnings': True,
        'merge_output_format': 'mp4',
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get('title', '')
            description = info.get('description', '') or title
            uploader = info.get('uploader', '')
            actual_path = output_path.replace('.mp4', '.mp4')
            if not os.path.exists(actual_path):
                files = [f for f in os.listdir('.') if f.startswith(output_path.replace('.mp4', '')) and f.endswith('.mp4')]
                if files:
                    actual_path = files[0]
            return actual_path, title, description, uploader, info
    except Exception as e:
        logger.error(f"Download error: {e}")
        return None, None, None, None, None

def upload_to_cloudinary(video_path):
    try:
        up = cloudinary.uploader.upload(video_path, resource_type="video")
        return up.get("secure_url")
    except Exception as e:
        logger.error(f"Cloudinary error: {e}")
        return None

def post_to_instagram(video_url, caption):
    token = get_ig_token()
    try:
        ig = requests.post(
            f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media",
            data={
                "media_type": "REELS",
                "video_url": video_url,
                "caption": caption,
                "access_token": token
            },
            timeout=60
        ).json()
        if "id" not in ig:
            return None, ig.get('error', {}).get('message', 'خطأ غير معروف')
        creation_id = ig["id"]
        time.sleep(35)
        publish = requests.post(
            f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media_publish",
            data={"creation_id": creation_id, "access_token": token},
            timeout=60
        ).json()
        if "id" in publish:
            return publish["id"], None
        return None, publish.get('error', {}).get('message', 'فشل النشر')
    except Exception as e:
        return None, str(e)

def cleanup_file(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except:
        pass

def strip_hashtags(text):
    """يشيل كل الهاشتاجات (#كلمة) من النص ويرجّع النص نظيف"""
    if not text:
        return ""
    import re
    cleaned = re.sub(r'#\S+', '', text)
    cleaned = re.sub(r'\s{2,}', ' ', cleaned)
    return cleaned.strip()


def extract_overlay_title_from_video(video_path):
    """يستخرج النص/العنوان المكتوب على الفيديو نفسه باستخدام Gemini Vision.
    يرجع النص لو موجود، أو سلسلة فارغة لو مفيش نص على الفيديو."""
    if not gemini_client or not os.path.exists(video_path):
        return ""
    frames = []
    try:
        # نطلع 3 لقطات (بداية - منتصف - قرب النهاية)
        try:
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", video_path],
                capture_output=True, timeout=30
            )
            duration = float(probe.stdout.decode().strip() or 0)
        except Exception:
            duration = 0
        timestamps = [1, max(duration / 2, 2), max(duration - 1, 3)] if duration > 3 else [0.5, 1, 1.5]

        for i, ts in enumerate(timestamps):
            frame_path = f"frame_{int(time.time())}_{i}.jpg"
            cmd = ["ffmpeg", "-y", "-ss", str(ts), "-i", video_path,
                   "-frames:v", "1", "-q:v", "2", frame_path]
            r = subprocess.run(cmd, capture_output=True, timeout=30)
            if r.returncode == 0 and os.path.exists(frame_path):
                frames.append(frame_path)

        if not frames:
            return ""

        from google.genai import types as genai_types
        parts = []
        for fp in frames:
            with open(fp, "rb") as f:
                parts.append(genai_types.Part.from_bytes(data=f.read(), mime_type="image/jpeg"))

        prompt = (
            "انت بتفحص لقطات من فيديو قصير. مهمتك تستخرج النص/العنوان المكتوب على الفيديو نفسه "
            "(text overlay) لو موجود.\n\n"
            "قواعد:\n"
            "1. لو فيه نص واضح مكتوب على الفيديو (عنوان، تعليق، جملة) ارجع النص ده زي ما هو بالظبط.\n"
            "2. لو مفيش أي نص مكتوب على الفيديو ارجع كلمة واحدة فقط: NONE\n"
            "3. تجاهل أي علامات مائية، أسماء مستخدمين (@username)، شعارات المنصات (TikTok, Instagram).\n"
            "4. لو فيه أكتر من نص في لقطات مختلفة ادمجهم في عنوان واحد متناسق.\n"
            "5. ما تكتبش أي شرح أو مقدمة، النص فقط أو NONE."
        )
        parts.append(prompt)

        resp = gemini_client.models.generate_content(
            model="gemini-2.0-flash", contents=parts
        )
        text = (resp.text or "").strip()
        if not text or text.upper().startswith("NONE"):
            return ""
        return text
    except Exception as e:
        logger.error(f"Overlay OCR error: {e}")
        return ""
    finally:
        for fp in frames:
            cleanup_file(fp)

# ─────────────────────────────────────────────────────────────────────
# 🎙️ Voice Over (تعليق صوتي بالذكاء الاصطناعي)
# ─────────────────────────────────────────────────────────────────────
VOICE_OPTIONS = {
    "male":   ("ar-EG-ShakirNeural", "👨 صوت رجالي مصري"),
    "female": ("ar-EG-SalmaNeural",  "👩 صوت نسائي مصري"),
    "saudi_m":("ar-SA-HamedNeural",  "👨 صوت رجالي سعودي"),
    "saudi_f":("ar-SA-ZariyahNeural","👩 صوت نسائي سعودي"),
}

async def generate_voiceover_audio(text, voice="ar-EG-ShakirNeural", output_path=None):
    """يولّد ملف صوتي MP3 بالعربي من نص باستخدام edge-tts (مجاني)"""
    try:
        import edge_tts
        if not output_path:
            output_path = f"voiceover_{int(time.time())}.mp3"
        communicate = edge_tts.Communicate(text, voice)
        await communicate.save(output_path)
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return output_path
        return None
    except Exception as e:
        logger.error(f"Voiceover generation error: {e}")
        return None

def _get_media_duration(path):
    """يرجّع مدة ملف صوت/فيديو بالثواني (float) أو None"""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=30
        )
        return float(r.stdout.strip()) if r.stdout.strip() else None
    except Exception:
        return None

def _build_atempo_chain(ratio):
    """ffmpeg atempo بيقبل 0.5-2.0 بس، فلو الـ ratio خارج المدى نسلسل أكتر من فلتر"""
    # ratio = audio_dur / video_dur ; لو > 1 نسرّع، لو < 1 نبطّأ
    if 0.5 <= ratio <= 2.0:
        return f"atempo={ratio:.4f}"
    filters = []
    r = ratio
    while r > 2.0:
        filters.append("atempo=2.0")
        r /= 2.0
    while r < 0.5:
        filters.append("atempo=0.5")
        r /= 0.5
    if abs(r - 1.0) > 0.001:
        filters.append(f"atempo={r:.4f}")
    return ",".join(filters) if filters else "atempo=1.0"

def mix_voiceover_with_video(video_path, audio_path, output_path=None, mute_original=True):
    """
    يدمج التعليق الصوتي مع الفيديو ويمدّ/يقلّص الصوت ليغطي مدة الفيديو كاملة.
    mute_original=True: يكتم الصوت الأصلي تماماً.
    """
    try:
        if not output_path:
            output_path = video_path.replace(".mp4", "_voiced.mp4")

        video_dur = _get_media_duration(video_path)
        audio_dur = _get_media_duration(audio_path)

        # حساب نسبة التسريع/التبطيء عشان الصوت يغطي الفيديو من الأول للآخر
        atempo_chain = "atempo=1.0"
        if video_dur and audio_dur and video_dur > 0.5 and audio_dur > 0.5:
            ratio = audio_dur / video_dur  # لو الصوت أطول من الفيديو، نسرّعه
            atempo_chain = _build_atempo_chain(ratio)
            logger.info(f"Voiceover stretch: video={video_dur:.1f}s audio={audio_dur:.1f}s ratio={ratio:.3f} -> {atempo_chain}")

        if mute_original:
            # كتم الصوت الأصلي تماماً، استخدم الصوت الجديد بس بعد تعديل سرعته ليطابق الفيديو
            filter_complex = f"[1:a]{atempo_chain},apad[aout]"
        else:
            filter_complex = f"[0:a]volume=0.25[a0];[1:a]{atempo_chain},apad,volume=1.6[a1];[a0][a1]amix=inputs=2:duration=first:dropout_transition=2[aout]"

        cmd = [
            "ffmpeg", "-y",
            "-i", video_path,
            "-i", audio_path,
            "-filter_complex", filter_complex,
            "-map", "0:v",
            "-map", "[aout]",
            "-c:v", "copy",
            "-c:a", "aac",
            "-b:a", "192k",
            "-t", f"{video_dur:.3f}" if video_dur else "999",
            output_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return output_path
        logger.error(f"ffmpeg mix error: {result.stderr[:500]}")

        # Fallback: لو الفيديو أصلاً مفيش فيه صوت
        cmd2 = [
            "ffmpeg", "-y",
            "-i", video_path,
            "-i", audio_path,
            "-filter_complex", f"[1:a]{atempo_chain},apad[aout]",
            "-c:v", "copy",
            "-c:a", "aac",
            "-b:a", "192k",
            "-map", "0:v",
            "-map", "[aout]",
            "-t", f"{video_dur:.3f}" if video_dur else "999",
            output_path
        ]
        result2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=300)
        if result2.returncode == 0 and os.path.exists(output_path):
            return output_path
        return None
    except Exception as e:
        logger.error(f"Mix voiceover error: {e}")
        return None

def build_post_keyboard(include_voiceover=True, voiceover_done=False):
    """يبني لوحة أزرار النشر الموحّدة (مع زر التعليق الصوتي)"""
    yt_enabled = get_setting("youtube_shorts") == "true" and bool(YT_CLIENT_ID and YT_CLIENT_SECRET and get_yt_refresh_token())
    fb_enabled = get_setting("facebook_page") == "true" and bool(FB_PAGE_ID and FB_PAGE_ACCESS_TOKEN)
    rows = []
    if include_voiceover:
        if voiceover_done:
            rows.append([InlineKeyboardButton("✅ التعليق الصوتي مُضاف — اضغط لتغييره", callback_data="voiceover_start")])
        else:
            rows.append([InlineKeyboardButton("🎙️ أضف تعليق صوتي بالذكاء الاصطناعي", callback_data="voiceover_start")])
    rows.append([InlineKeyboardButton("🚀 إنستجرام فقط", callback_data="post_now")])
    if yt_enabled:
        rows.append([InlineKeyboardButton("▶️ YouTube فقط", callback_data="post_youtube")])
    if fb_enabled:
        rows.append([InlineKeyboardButton("📘 Facebook فقط", callback_data="post_facebook")])
    if yt_enabled:
        rows.append([InlineKeyboardButton("🚀 + ▶️ إنستجرام + YouTube", callback_data="post_ig_yt")])
    if fb_enabled:
        rows.append([InlineKeyboardButton("🚀 + 📘 إنستجرام + Facebook", callback_data="post_ig_fb")])
    if yt_enabled and fb_enabled:
        rows.append([InlineKeyboardButton("▶️ + 📘 YouTube + Facebook", callback_data="post_yt_fb")])
        rows.append([InlineKeyboardButton("🌟 نشر على كل المنصات (IG + YT + FB)", callback_data="post_all")])
    rows.append([
        InlineKeyboardButton("📋 أضف للقائمة", callback_data="add_to_queue"),
        InlineKeyboardButton("⏰ جدولة بتاريخ", callback_data="schedule_post"),
    ])
    rows.append([InlineKeyboardButton("❌ إلغاء", callback_data="cancel_post")])
    return rows, yt_enabled, fb_enabled

async def enhance_caption_with_ai(description, platform):
    """يولد عنواناً جذاباً للفيديو (عنوان قصير وجذاب)"""
    if not gemini_client:
        return None
    try:
        prompt = (
            f"أنت خبير محتوى على إنستجرام ويوتيوب.\n"
            f"المنصة الأصلية: {platform}\n"
            f"عنوان الفيديو الأصلي: {(description or 'بدون عنوان')[:300]}\n\n"
            f"اكتب عنواناً واحداً فقط (سطر واحد) جذاباً وقصيراً ومثيراً للفضول للفيديو.\n"
            f"يكون باللغة العربية أو الإنجليزية حسب المحتوى، بدون هاشتاقات، بدون شرح إضافي."
        )
        response = gemini_client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )
        return response.text.strip().split("\n")[0]  # سطر واحد فقط
    except Exception as e:
        logger.error(f"AI error: {e}")
        return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user     = update.effective_user
    uid      = user.id
    username = user.username or ""
    fullname = user.full_name or ""
    platforms_list = "\n".join([f"• {name}" for name in set(SUPPORTED_PLATFORMS.values())])

    # ── تسجيل المستخدم إذا لم يكن موجوداً ──
    existing = db_get_user(uid)
    if not existing:
        # لو مفيش أدمن خالص → المستخدم الأول يبقى admin تلقائياً
        no_admin_yet = (not ADMIN_USER_ID) and (not any(u[3] == "admin" for u in db_get_all_users()))
        require_approval = get_setting("require_approval") == "true"
        if no_admin_yet or is_admin(uid) or not require_approval:
            role = "admin" if (no_admin_yet or is_admin(uid)) else "user"
            db_register_user(uid, username, fullname, role)
            if no_admin_yet:
                await update.message.reply_text(
                    "👑 تم تسجيلك كمشرف (Admin) أول للبوت!\n\n"
                    "💡 يُنصح بضبط ADMIN_USER_ID في Secrets بمعرّفك:\n"
                    f"`{uid}`\n\n"
                    "استخدم /users لإدارة المستخدمين.",
                    parse_mode="Markdown"
                )
        else:
            db_register_user(uid, username, fullname, "pending")
            await notify_admin_new_user(context, uid, username, fullname)
            await update.message.reply_text(
                "⏳ طلبك قيد المراجعة من الـ Admin.\n"
                "سيتم إخطارك عند الموافقة."
            )
            return

    role = get_user_role(uid)
    if role == "blocked":
        await update.message.reply_text("🚫 أنت محظور من استخدام هذا البوت.")
        return
    if role == "pending":
        await update.message.reply_text("⏳ طلبك لا يزال قيد المراجعة. انتظر موافقة الـ Admin.")
        return

    admin_cmds = "\n/settings - الإعدادات\n/analytics - التحليل\n/scheduled - المجدولة\n/queue - القائمة\n/users - إدارة المستخدمين" if is_admin(uid) else ""
    await update.message.reply_text(
        f"مرحباً {fullname}! أنا بوت نشر الفيديوهات 🎬\n\n"
        f"📌 المنصات المدعومة:\n{platforms_list}\n\n"
        f"📤 أرسل رابط الفيديو وسأنشره على إنستجرام كـ Reel\n"
        f"⚙️ الأوامر:{admin_cmds}\n/help - المساعدة"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 دليل الاستخدام:\n\n"
        "1️⃣ أرسل رابط فيديو من أي منصة مدعومة\n"
        "2️⃣ اختر: نشر فوري أو جدولة\n"
        "3️⃣ يمكنك تعديل الوصف أو استخدام الذكاء الاصطناعي\n\n"
        "📌 المنشورات المجدولة:\n"
        "اكتب الوقت بصيغة: YYYY-MM-DD HH:MM\n"
        "مثال: 2024-12-25 20:00\n\n"
        "⚙️ الإعدادات:\n"
        "• تفعيل تحسين الوصف بالـ AI\n"
        "• ضبط المنطقة الزمنية"
    )

async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = (update.effective_user or update.callback_query.from_user).id
    if not is_admin(uid):
        msg = update.message or (update.callback_query and update.callback_query.message)
        if msg: await msg.reply_text("🚫 هذا الأمر للمشرف فقط.")
        return
    def s(k): return get_setting(k) == "true"
    def b(v): return "✅" if v else "❌"

    timezone    = get_setting("timezone") or "Africa/Cairo"
    max_dur     = int(get_setting("max_video_duration") or 0)
    max_dur_txt = f"{max_dur} ث" if max_dur > 0 else "بلا حد"
    yt_ready    = bool(YT_CLIENT_ID and YT_CLIENT_SECRET and YT_REFRESH_TOKEN)
    yt_lbl      = f"{b(s('youtube_shorts'))} YouTube Shorts" + ("" if yt_ready else " ⚠️")
    fb_ready    = bool(FB_PAGE_ID and FB_PAGE_ACCESS_TOKEN)
    fb_lbl      = f"{b(s('facebook_page'))} Facebook Page" + ("" if fb_ready else " ⚠️")

    keyboard = [
        [
            InlineKeyboardButton(f"{b(s('ai_enhance_caption'))} تحسين الوصف AI", callback_data="toggle_ai_enhance"),
            InlineKeyboardButton(f"{b(s('auto_first_comment'))} أول تعليق تلقائي",callback_data="toggle_auto_first_comment"),
        ],
        [
            InlineKeyboardButton(f"{b(s('auto_vertical'))} تحويل عمودي 9:16",    callback_data="toggle_auto_vertical"),
            InlineKeyboardButton(yt_lbl,                                           callback_data="toggle_youtube_shorts"),
        ],
        [
            InlineKeyboardButton(fb_lbl,                                           callback_data="toggle_facebook_page"),
        ],
        [
            InlineKeyboardButton(f"{b(s('notify_success'))} إشعار النجاح",       callback_data="toggle_notify_success"),
            InlineKeyboardButton(f"{b(s('notify_fail'))} إشعار الفشل",            callback_data="toggle_notify_fail"),
        ],
        [InlineKeyboardButton(f"⏱️ حد المدة: {max_dur_txt}",                      callback_data="set_max_duration")],
        [InlineKeyboardButton(f"🕐 المنطقة الزمنية: {timezone}",                  callback_data="set_timezone")],
        [InlineKeyboardButton("📋 قالب الوصف الافتراضي",                          callback_data="set_caption_template")],
        [InlineKeyboardButton(f"{b(s('require_approval'))} موافقة على المستخدمين الجدد", callback_data="toggle_require_approval")],
        [InlineKeyboardButton("❌ إغلاق",                                          callback_data="close_settings")],
    ]
    text = (
        "⚙️ الإعدادات\n\n"
        f"✅ = مفعّل  |  ❌ = معطّل\n"
        f"{'─'*28}\n"
    )
    if not yt_ready and s('youtube_shorts'):
        text += "⚠️ YouTube Shorts يحتاج إعداد Secrets (YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN)\n"
    if not fb_ready and s('facebook_page'):
        text += "⚠️ Facebook Page يحتاج إعداد Secrets (FB_PAGE_ID, FB_PAGE_ACCESS_TOKEN)\n"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    def toggle(key):
        set_setting(key, "false" if get_setting(key) == "true" else "true")

    if data == "toggle_ai_enhance":
        if get_setting("ai_enhance_caption") != "true" and not gemini_client:
            await query.answer("❌ يجب إضافة GEMINI_API_KEY أولاً!", show_alert=True); return
        toggle("ai_enhance_caption"); await settings_menu(update, context)
    elif data == "toggle_auto_first_comment":
        toggle("auto_first_comment"); await settings_menu(update, context)
    elif data == "toggle_auto_vertical":
        toggle("auto_vertical"); await settings_menu(update, context)
    elif data == "toggle_youtube_shorts":
        if get_setting("youtube_shorts") != "true" and not (YT_CLIENT_ID and YT_CLIENT_SECRET and YT_REFRESH_TOKEN):
            await query.answer(
                "❌ أضف أولاً في Secrets:\nYOUTUBE_CLIENT_ID\nYOUTUBE_CLIENT_SECRET\nYOUTUBE_REFRESH_TOKEN",
                show_alert=True
            ); return
        toggle("youtube_shorts"); await settings_menu(update, context)
    elif data == "toggle_facebook_page":
        if get_setting("facebook_page") != "true" and not (FB_PAGE_ID and FB_PAGE_ACCESS_TOKEN):
            await query.answer(
                "❌ أضف أولاً في Secrets:\nFB_PAGE_ID\nFB_PAGE_ACCESS_TOKEN",
                show_alert=True
            ); return
        toggle("facebook_page"); await settings_menu(update, context)
    elif data == "toggle_notify_success":
        toggle("notify_success"); await settings_menu(update, context)
    elif data == "toggle_notify_fail":
        toggle("notify_fail");       await settings_menu(update, context)
    elif data == "toggle_require_approval":
        toggle("require_approval");  await settings_menu(update, context)
    elif data == "close_settings":
        await query.delete_message()
    elif data == "set_max_duration":
        cur = int(get_setting("max_video_duration") or 0)
        await query.edit_message_text(
            f"⏱️ الحد الأقصى لمدة الفيديو الحالي: {cur} ثانية (0 = بلا حد)\n\n"
            "اكتب المدة بالثواني:\n"
            "مثال: 90 (لرفض الفيديوهات أطول من 90 ثانية)\n"
            "أكتب 0 لإلغاء الحد"
        )
        context.user_data["waiting_for"] = "max_duration"
    elif data == "set_timezone":
        await query.edit_message_text(
            "⏰ اكتب اسم المنطقة الزمنية:\n\n"
            "أمثلة:\n• Africa/Cairo\n• Asia/Riyadh\n• Asia/Dubai\n• Europe/London"
        )
        context.user_data["waiting_for"] = "timezone"
    elif data == "set_caption_template":
        current_template = get_setting("default_caption_template") or "(لا يوجد)"
        await query.edit_message_text(
            f"📝 القالب الحالي:\n{current_template}\n\n"
            "اكتب القالب الجديد:\n"
            "يمكن استخدام {description} و {platform}"
        )
        context.user_data["waiting_for"] = "caption_template"

async def analytics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 هذا الأمر للمشرف فقط.")
        return
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    c.execute("SELECT * FROM analytics WHERE date=?", (today,))
    today_data = c.fetchone()
    c.execute("SELECT SUM(total_posts), SUM(successful_posts), SUM(failed_posts), SUM(scheduled_posts) FROM analytics WHERE date >= ?", (week_ago,))
    week_data = c.fetchone()
    c.execute("SELECT platform, COUNT(*) FROM posts WHERE status='success' GROUP BY platform ORDER BY COUNT(*) DESC LIMIT 5")
    top_platforms = c.fetchall()
    c.execute("SELECT COUNT(*) FROM posts WHERE status='scheduled' AND scheduled_at > ?", (datetime.now().isoformat(),))
    pending_scheduled = c.fetchone()[0]
    c.execute("SELECT COUNT(*), platform FROM posts WHERE created_at >= ? GROUP BY platform", (
        (datetime.now() - timedelta(hours=24)).isoformat(),
    ))
    recent = c.fetchall()
    conn.close()

    today_total = today_data[2] if today_data else 0
    today_success = today_data[3] if today_data else 0
    today_fail = today_data[4] if today_data else 0
    today_sched = today_data[5] if today_data else 0

    week_total = week_data[0] or 0
    week_success = week_data[1] or 0
    week_fail = week_data[2] or 0

    success_rate_today = f"{(today_success/today_total*100):.0f}%" if today_total > 0 else "0%"
    success_rate_week = f"{(week_success/week_total*100):.0f}%" if week_total > 0 else "0%"

    platforms_text = "\n".join([f"  • {p[0]}: {p[1]} منشور" for p in top_platforms]) if top_platforms else "  لا يوجد بيانات"

    text = (
        f"📊 تحليل شامل لشغل البوت\n"
        f"{'='*30}\n\n"
        f"📅 اليوم ({today}):\n"
        f"  • إجمالي المنشورات: {today_total}\n"
        f"  • تم النشر بنجاح: {today_success} ✅\n"
        f"  • فشل: {today_fail} ❌\n"
        f"  • مجدولة: {today_sched} ⏰\n"
        f"  • نسبة النجاح: {success_rate_today}\n\n"
        f"📈 آخر 7 أيام:\n"
        f"  • إجمالي: {week_total}\n"
        f"  • نجاح: {week_success} ✅\n"
        f"  • فشل: {week_fail} ❌\n"
        f"  • نسبة النجاح: {success_rate_week}\n\n"
        f"⏳ منشورات مجدولة قادمة: {pending_scheduled}\n\n"
        f"🏆 أكثر المنصات استخداماً:\n{platforms_text}\n\n"
        f"⚙️ الإعدادات النشطة:\n"
        f"  • تحسين AI: {'✅' if get_setting('ai_enhance_caption')=='true' else '❌'}\n"
        f"  • المنطقة الزمنية: {get_setting('timezone')}\n"
    )
    await update.message.reply_text(text)

async def scheduled_posts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 هذا الأمر للمشرف فقط.")
        return
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, url, platform, scheduled_at, caption FROM posts WHERE status='scheduled' AND scheduled_at > ? ORDER BY scheduled_at ASC",
        (datetime.now().isoformat(),)
    )
    posts = c.fetchall()
    conn.close()
    if not posts:
        await update.message.reply_text("لا توجد منشورات مجدولة حالياً.")
        return
    text = "⏰ المنشورات المجدولة:\n\n"
    for post in posts:
        pid, url, platform, sched_at, caption = post
        try:
            dt = datetime.fromisoformat(sched_at)
            formatted_time = dt.strftime("%Y-%m-%d %H:%M")
        except:
            formatted_time = sched_at
        short_url = url[:40] + "..." if url and len(url) > 40 else url
        short_caption = caption[:30] + "..." if caption and len(caption) > 30 else (caption or "")
        text += f"🆔 #{pid} | {platform}\n📅 {formatted_time}\n🔗 {short_url}\n💬 {short_caption}\n/cancel_{pid}\n\n"
    await update.message.reply_text(text)

async def cancel_scheduled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    match = re.match(r'/cancel_(\d+)', text)
    if not match:
        return
    post_id = int(match.group(1))
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE posts SET status='cancelled' WHERE id=? AND status='scheduled'", (post_id,))
    affected = c.rowcount
    conn.commit()
    conn.close()
    if affected:
        try:
            scheduler.remove_job(f"post_{post_id}")
        except:
            pass
        await update.message.reply_text(f"✅ تم إلغاء المنشور #{post_id}")
    else:
        await update.message.reply_text(f"❌ لم يتم العثور على المنشور #{post_id}")

def get_queue_menu_text_and_keyboard():
    count   = queue_count()
    is_run  = get_setting("queue_is_running") == "true"
    mins    = int(get_setting("queue_interval_minutes") or 120)
    label   = next((l for l, m in INTERVAL_OPTIONS if m == mins), f"{mins} دقيقة")
    status  = "▶️ يعمل الآن" if is_run else "⏸ متوقف"

    text = (
        f"📋 قائمة الانتظار\n{'='*25}\n\n"
        f"📦 عدد الفيديوهات: {count}\n"
        f"⏱ الفاصل الزمني: كل {label}\n"
        f"🔄 الحالة: {status}\n\n"
        "اختر الفاصل الزمني ثم ابدأ النشر:"
    )
    # صفوف الفاصل الزمني (2 في كل صف)
    interval_rows = []
    row = []
    for lbl, m in INTERVAL_OPTIONS:
        tick = "✅ " if m == mins else ""
        row.append(InlineKeyboardButton(f"{tick}كل {lbl}", callback_data=f"qi_{m}"))
        if len(row) == 2:
            interval_rows.append(row)
            row = []
    if row:
        interval_rows.append(row)

    control_row = []
    if not is_run and count > 0:
        control_row.append(InlineKeyboardButton("▶️ ابدأ النشر التلقائي", callback_data="q_start"))
    if is_run:
        control_row.append(InlineKeyboardButton("⏸ إيقاف النشر", callback_data="q_stop"))

    keyboard = interval_rows
    if control_row:
        keyboard.append(control_row)
    keyboard.append([
        InlineKeyboardButton("👁 عرض الفيديوهات", callback_data="q_view"),
        InlineKeyboardButton("🗑 مسح القائمة",    callback_data="q_clear"),
    ])
    keyboard.append([InlineKeyboardButton("❌ إغلاق", callback_data="q_close")])
    return text, InlineKeyboardMarkup(keyboard)

async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 هذا الأمر للمشرف فقط.")
        return
    text, kb = get_queue_menu_text_and_keyboard()
    await update.message.reply_text(text, reply_markup=kb)

async def queue_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    if data.startswith("qi_"):
        mins = int(data[3:])
        set_setting("queue_interval_minutes", str(mins))
        text, kb = get_queue_menu_text_and_keyboard()
        await query.edit_message_text(text, reply_markup=kb)

    elif data == "q_start":
        if queue_count() == 0:
            await query.answer("❌ القائمة فارغة! أضف فيديوهات أولاً.", show_alert=True)
            return
        mins     = int(get_setting("queue_interval_minutes") or 120)
        chat_id  = query.message.chat_id
        set_setting("queue_is_running", "true")
        set_setting("queue_chat_id", str(chat_id))
        bot_loop = asyncio.get_event_loop()
        try:
            scheduler.remove_job("auto_queue")
        except:
            pass
        scheduler.add_job(
            auto_post_from_queue,
            'interval',
            minutes=mins,
            id="auto_queue",
            replace_existing=True,
            args=[chat_id, bot_loop],
            next_run_time=datetime.now()        # ينشر أول فيديو فوراً
        )
        label = next((l for l, m in INTERVAL_OPTIONS if m == mins), f"{mins} دقيقة")
        text, kb = get_queue_menu_text_and_keyboard()
        await query.edit_message_text(
            f"✅ بدأ النشر التلقائي! سينشر فيديو كل {label}.\n\n" + text,
            reply_markup=kb
        )

    elif data == "q_stop":
        set_setting("queue_is_running", "false")
        try:
            scheduler.remove_job("auto_queue")
        except:
            pass
        text, kb = get_queue_menu_text_and_keyboard()
        await query.edit_message_text("⏸ تم إيقاف النشر التلقائي.\n\n" + text, reply_markup=kb)

    elif data == "q_view":
        items = queue_get_all()
        if not items:
            await query.answer("القائمة فارغة.", show_alert=True)
            return
        lines = []
        for i, (qid, plat, cap, added) in enumerate(items, 1):
            short = (cap or "")[:40] + ("..." if len(cap or "") > 40 else "")
            lines.append(f"{i}. [{plat}] {short}")
        await query.answer()
        text_list = "📋 محتوى القائمة:\n\n" + "\n".join(lines)
        await query.message.reply_text(text_list[:4000])

    elif data == "q_clear":
        queue_clear()
        set_setting("queue_is_running", "false")
        try:
            scheduler.remove_job("auto_queue")
        except:
            pass
        text, kb = get_queue_menu_text_and_keyboard()
        await query.edit_message_text("🗑 تم مسح القائمة وإيقاف النشر.\n\n" + text, reply_markup=kb)

    elif data == "q_close":
        await query.delete_message()

def auto_post_from_queue(chat_id, bot_loop):
    item = queue_pop()
    if not item:
        set_setting("queue_is_running", "false")
        try:
            scheduler.remove_job("auto_queue")
        except:
            pass
        asyncio.run_coroutine_threadsafe(
            app.bot.send_message(chat_id=chat_id, text="✅ انتهت القائمة! تم نشر كل الفيديوهات."),
            bot_loop
        )
        return

    _, url, platform, caption, cloudinary_url, user_id, item_chat_id = item
    remaining = queue_count()

    asyncio.run_coroutine_threadsafe(
        app.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ جاري نشر فيديو من القائمة...\n📱 المنصة: {platform}\n📦 متبقي في القائمة: {remaining}"
        ),
        bot_loop
    )

    ig_id, error = post_to_instagram(cloudinary_url, caption)
    log_post(url, platform, caption, "success" if ig_id else "failed", ig_post_id=ig_id, user_id=user_id)

    if ig_id:
        # أول تعليق تلقائي من القائمة
        if get_setting("auto_first_comment") == "true":
            post_first_comment(ig_id, caption.strip())
        msg = f"✅ تم النشر من القائمة!\n📱 {platform}\n📦 متبقي: {remaining}"
        if remaining == 0:
            msg += "\n\n🎉 تم نشر كل الفيديوهات!"
            set_setting("queue_is_running", "false")
            try:
                scheduler.remove_job("auto_queue")
            except:
                pass
    else:
        msg = f"❌ فشل النشر من القائمة\n📱 {platform}\nالخطأ: {error}"

    asyncio.run_coroutine_threadsafe(
        app.bot.send_message(chat_id=chat_id, text=msg),
        bot_loop
    )

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    role = get_user_role(uid)
    if role == "blocked":
        await update.message.reply_text("🚫 أنت محظور من استخدام هذا البوت.")
        return
    if role in ("pending", "unknown"):
        await update.message.reply_text("⏳ طلبك قيد المراجعة أو لم تسجّل بعد. أرسل /start أولاً.")
        return
    # الرابط يأتي إما من _single_url أو من نص الرسالة مباشرة
    url = context.user_data.pop("_single_url", None) or update.message.text.strip()
    platform = detect_platform(url)
    if platform == "Unknown":
        return

    # ── منع التكرار ──────────────────────────────────────────────
    skip_dup = context.user_data.pop("_skip_dup", "")
    dup = None if (skip_dup == url) else check_duplicate(url)
    if dup:
        dup_id, dup_at, dup_platform = dup
        try:
            dup_date = datetime.fromisoformat(dup_at).strftime("%Y-%m-%d %H:%M")
        except:
            dup_date = dup_at
        kb = [
            [
                InlineKeyboardButton("🔄 نشر مرة أخرى", callback_data="force_repost"),
                InlineKeyboardButton("❌ إلغاء",          callback_data="cancel_post"),
            ]
        ]
        context.user_data["dup_url"]      = url
        context.user_data["dup_platform"] = platform
        await update.message.reply_text(
            f"⚠️ تنبيه: هذا الرابط سبق نشره!\n\n"
            f"🆔 رقم المنشور: #{dup_id}\n"
            f"📅 تاريخ النشر: {dup_date}\n"
            f"📱 المنصة: {dup_platform}\n\n"
            f"هل تريد نشره مرة أخرى؟",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    processing_msg = await update.message.reply_text(
        f"⏳ جاري تحليل الفيديو من {platform}..."
    )

    try:
        video_path, title, description, uploader, info = download_video(url)
        if not video_path or not os.path.exists(video_path):
            await processing_msg.edit_text(f"❌ فشل تحميل الفيديو من {platform}.\nتأكد من صحة الرابط.")
            return

        # ── التحقق من الحد الأقصى للمدة ─────────────────────────
        duration = int(info.get('duration', 0) if info else 0)
        max_dur  = int(get_setting("max_video_duration") or 0)
        if max_dur > 0 and duration > max_dur:
            cleanup_file(video_path)
            mins_d, secs_d = divmod(duration, 60)
            mins_m, secs_m = divmod(max_dur, 60)
            await processing_msg.edit_text(
                f"❌ الفيديو طويل جداً!\n\n"
                f"⏱️ مدة الفيديو: {mins_d}:{secs_d:02d}\n"
                f"🚫 الحد الأقصى: {mins_m}:{secs_m:02d}\n\n"
                f"غيّر الحد من /settings"
            )
            return

        # ── التحويل العمودي التلقائي ─────────────────────────────
        if get_setting("auto_vertical") == "true":
            await processing_msg.edit_text("🔄 جاري التحويل لصيغة عمودية 9:16...")
            video_path = convert_to_vertical(video_path)

        await processing_msg.edit_text("⬆️ جاري رفع الفيديو...")
        cloudinary_url = upload_to_cloudinary(video_path)

        if not cloudinary_url:
            cleanup_file(video_path)
            await processing_msg.edit_text("❌ فشل رفع الفيديو. حاول مرة أخرى.")
            return

        # ── استخراج النص المكتوب على الفيديو (OCR) ───────────────
        await processing_msg.edit_text("🔍 جاري قراءة العنوان المكتوب على الفيديو...")
        overlay_title = extract_overlay_title_from_video(video_path)
        cleanup_file(video_path)

        # تنظيف: نتجاهل أي نص يبدأ بـ http
        def _clean(t):
            t = (t or "").strip()
            return "" if t.startswith("http://") or t.startswith("https://") else t

        clean_title    = _clean(title)
        clean_desc     = _clean(description)
        extracted_text = strip_hashtags(overlay_title or clean_title or clean_desc or "")

        template = get_setting("default_caption_template") or ""
        if template:
            final_caption = template.format(
                description=extracted_text, platform=platform
            )
        else:
            final_caption = extracted_text

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "INSERT INTO pending_videos (user_id, url, platform, caption, cloudinary_url, created_at) VALUES (?,?,?,?,?,?)",
            (update.effective_user.id, url, platform, final_caption, cloudinary_url, datetime.now().isoformat())
        )
        pending_id = c.lastrowid
        conn.commit()
        conn.close()

        context.user_data["pending_id"]      = pending_id
        context.user_data["cloudinary_url"]  = cloudinary_url
        context.user_data["caption"]         = final_caption
        context.user_data["url"]             = url
        context.user_data["platform"]        = platform
        context.user_data["extracted_title"] = extracted_text
        context.user_data["video_title"]     = overlay_title or clean_title or extracted_text
        context.user_data["overlay_title"]   = overlay_title
        context.user_data["video_desc"]      = clean_desc  or ""
        mins, secs = divmod(duration, 60)
        if overlay_title:
            display_title = "📝 العنوان المكتوب على الفيديو:\n" + overlay_title[:250] + ("..." if len(overlay_title) > 250 else "")
        else:
            display_title = "ℹ️ مفيش عنوان مكتوب على الفيديو"
            if clean_title or clean_desc:
                fallback = (clean_title or clean_desc)[:200]
                display_title += f"\n(عنوان من المنصة: {fallback})"

        # ── إرسال الـ Thumbnail كمعاينة ──────────────────────────
        thumbnail_url = (info or {}).get("thumbnail") or ""
        if thumbnail_url:
            try:
                await update.message.reply_photo(
                    photo=thumbnail_url,
                    caption=f"🖼️ معاينة | {platform} | ⏱️ {mins}:{secs:02d}"
                )
            except Exception:
                pass  # إذا فشل الـ thumbnail لا يوقف التنفيذ

        keyboard = [
            [
                InlineKeyboardButton("✅ احتفظ بالعنوان",   callback_data="caption_ok"),
                InlineKeyboardButton("✏️ غيّر العنوان",     callback_data="edit_caption"),
            ],
            [
                InlineKeyboardButton("🤖 حسّن العنوان بـ AI", callback_data="ai_caption"),
                InlineKeyboardButton("❌ إلغاء",               callback_data="cancel_post"),
            ],
        ]

        await processing_msg.edit_text(
            f"✅ تم تحليل الفيديو بنجاح!\n\n"
            f"📱 {platform}  |  ⏱️ {mins}:{secs:02d}  |  👤 {uploader or 'غير معروف'}\n"
            f"{'🔄 تم التحويل لصيغة عمودية 9:16' if get_setting('auto_vertical') == 'true' else ''}\n\n"
            f"📌 عنوان الفيديو:\n"
            f"{'─' * 28}\n"
            f"{display_title or '(لا يوجد عنوان)'}\n"
            f"{'─' * 28}\n\n"
            f"هل تريد الاحتفاظ بهذا العنوان أم تغييره؟",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:
        logger.error(f"Error in handle_url: {e}")
        await processing_msg.edit_text(f"❌ حدث خطأ: {str(e)[:200]}")

async def handle_video_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يستقبل فيديو يبعته المستخدم مباشرة للبوت ويبدأ نفس فلوّ النشر."""
    uid  = update.effective_user.id
    role = get_user_role(uid)
    if role == "blocked":
        await update.message.reply_text("🚫 أنت محظور من استخدام هذا البوت.")
        return
    if role in ("pending", "unknown"):
        await update.message.reply_text("⏳ طلبك قيد المراجعة أو لم تسجّل بعد. أرسل /start أولاً.")
        return

    msg = update.message
    tg_video = msg.video or msg.animation
    tg_doc   = msg.document if (msg.document and (msg.document.mime_type or "").startswith("video/")) else None
    file_obj = tg_video or tg_doc
    if not file_obj:
        return

    # حد أقصى لحجم الملف من تيليجرام (~20MB للبوتات العادية، 2GB لو فيه local API)
    if file_obj.file_size and file_obj.file_size > 50 * 1024 * 1024:
        await msg.reply_text(
            "⚠️ الفيديو كبير شوية (أكبر من 50MB).\n"
            "تيليجرام بيحدد حجم اللي البوت يقدر يستقبله. جرّب فيديو أصغر أو ابعت رابط."
        )
        return

    processing_msg = await msg.reply_text("⏳ جاري استلام الفيديو من تيليجرام...")
    video_path = f"upload_{int(time.time())}.mp4"

    try:
        tg_file = await file_obj.get_file()
        await tg_file.download_to_drive(video_path)

        if not os.path.exists(video_path):
            await processing_msg.edit_text("❌ فشل تحميل الفيديو من تيليجرام.")
            return

        # ── المدة ─────────────────────────────────────────────
        duration = int(getattr(file_obj, "duration", 0) or 0)
        if duration == 0:
            try:
                probe = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                     "-of", "default=noprint_wrappers=1:nokey=1", video_path],
                    capture_output=True, timeout=30
                )
                duration = int(float(probe.stdout.decode().strip() or 0))
            except Exception:
                duration = 0

        max_dur = int(get_setting("max_video_duration") or 0)
        if max_dur > 0 and duration > max_dur:
            cleanup_file(video_path)
            mins_d, secs_d = divmod(duration, 60)
            mins_m, secs_m = divmod(max_dur, 60)
            await processing_msg.edit_text(
                f"❌ الفيديو طويل جداً!\n\n"
                f"⏱️ مدة الفيديو: {mins_d}:{secs_d:02d}\n"
                f"🚫 الحد الأقصى: {mins_m}:{secs_m:02d}"
            )
            return

        # ── التحويل العمودي ──────────────────────────────────
        if get_setting("auto_vertical") == "true":
            await processing_msg.edit_text("🔄 جاري التحويل لصيغة عمودية 9:16...")
            video_path = convert_to_vertical(video_path)

        await processing_msg.edit_text("⬆️ جاري رفع الفيديو...")
        cloudinary_url = upload_to_cloudinary(video_path)
        if not cloudinary_url:
            cleanup_file(video_path)
            await processing_msg.edit_text("❌ فشل رفع الفيديو. حاول مرة أخرى.")
            return

        # ── OCR للعنوان المكتوب على الفيديو ──────────────────
        await processing_msg.edit_text("🔍 جاري قراءة العنوان المكتوب على الفيديو...")
        overlay_title = extract_overlay_title_from_video(video_path)
        cleanup_file(video_path)

        # العنوان: من النص المكتوب على الفيديو، أو من caption الرسالة، أو فاضي
        msg_caption = (msg.caption or "").strip()
        extracted_text = strip_hashtags(overlay_title or msg_caption or "")
        platform = "Telegram Upload"

        template = get_setting("default_caption_template") or ""
        if template:
            final_caption = template.format(
                description=extracted_text, platform=platform
            )
        else:
            final_caption = extracted_text

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        upload_url = f"telegram://{file_obj.file_unique_id}"
        c.execute(
            "INSERT INTO pending_videos (user_id, url, platform, caption, cloudinary_url, created_at) VALUES (?,?,?,?,?,?)",
            (uid, upload_url, platform, final_caption, cloudinary_url, datetime.now().isoformat())
        )
        pending_id = c.lastrowid
        conn.commit()
        conn.close()

        context.user_data["pending_id"]      = pending_id
        context.user_data["cloudinary_url"]  = cloudinary_url
        context.user_data["caption"]         = final_caption
        context.user_data["url"]             = upload_url
        context.user_data["platform"]        = platform
        context.user_data["extracted_title"] = extracted_text
        context.user_data["video_title"]     = overlay_title or msg_caption or extracted_text
        context.user_data["overlay_title"]   = overlay_title
        context.user_data["video_desc"]      = msg_caption

        mins, secs = divmod(duration, 60)
        if overlay_title:
            display_title = "📝 العنوان المكتوب على الفيديو:\n" + overlay_title[:250]
        elif msg_caption:
            display_title = f"📝 من وصف الرسالة:\n{msg_caption[:250]}"
        else:
            display_title = "ℹ️ مفيش عنوان مكتوب على الفيديو"

        keyboard = [
            [
                InlineKeyboardButton("✅ احتفظ بالعنوان",   callback_data="caption_ok"),
                InlineKeyboardButton("✏️ غيّر العنوان",     callback_data="edit_caption"),
            ],
            [
                InlineKeyboardButton("🤖 حسّن العنوان بـ AI", callback_data="ai_caption"),
                InlineKeyboardButton("❌ إلغاء",               callback_data="cancel_post"),
            ],
        ]

        await processing_msg.edit_text(
            f"✅ تم استلام الفيديو وتحضيره!\n\n"
            f"📤 رفع مباشر  |  ⏱️ {mins}:{secs:02d}\n"
            f"{'🔄 تم التحويل لصيغة عمودية 9:16' if get_setting('auto_vertical') == 'true' else ''}\n\n"
            f"📌 العنوان:\n"
            f"{'─' * 28}\n"
            f"{display_title}\n"
            f"{'─' * 28}\n\n"
            f"هل تريد الاحتفاظ بهذا العنوان أم تغييره؟",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:
        logger.error(f"Error in handle_video_upload: {e}")
        cleanup_file(video_path)
        await processing_msg.edit_text(f"❌ حدث خطأ: {str(e)[:200]}")

async def video_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    cloudinary_url = context.user_data.get("cloudinary_url")
    caption = context.user_data.get("caption", "")
    url = context.user_data.get("url", "")
    platform = context.user_data.get("platform", "")

    if data == "force_repost":
        repost_url = context.user_data.pop("dup_url", "")
        context.user_data.clear()
        if not repost_url:
            await query.edit_message_text("❌ أرسل الرابط مجدداً.")
            return
        # ضع علامة skip_dup حتى لا يظهر تحذير التكرار
        context.user_data["_skip_dup"] = repost_url
        await query.edit_message_text(
            f"✅ سيتم تجاهل تحذير التكرار.\n\n"
            f"📨 أرسل الرابط مرة أخرى:\n{repost_url}"
        )
        return

    if data == "caption_ok":
        caption_preview = caption[:150] + ("..." if len(caption) > 150 else "")
        voiced = bool(context.user_data.get("voiceover_done"))
        rows, yt_enabled, fb_enabled = build_post_keyboard(voiceover_done=voiced)

        if yt_enabled or fb_enabled:
            extras = []
            if yt_enabled: extras.append("YouTube")
            if fb_enabled: extras.append("Facebook")
            posting_text = "اختر طريقة النشر:\n\n📌 المنصات الإضافية المفعّلة: " + " + ".join(extras)
        else:
            posting_text = "اختر طريقة النشر:"

        await query.edit_message_text(
            f"✅ تم تأكيد النص:\n\n"
            f"💬 {caption_preview}\n\n"
            f"{posting_text}",
            reply_markup=InlineKeyboardMarkup(rows)
        )
        return

    if data == "post_now":
        await query.edit_message_text("⏳ جاري النشر على إنستجرام... (انتظر 35-40 ثانية)")
        ig_id, error = post_to_instagram(cloudinary_url, caption)
        if ig_id:
            log_post(url, platform, caption, "success", ig_post_id=ig_id, user_id=query.from_user.id)
            # أول تعليق تلقائي
            first_comment_note = ""
            if get_setting("auto_first_comment") == "true":
                ok = post_first_comment(ig_id, caption.strip())
                first_comment_note = "\n💬 تم إضافة التعليق الأول." if ok else "\n⚠️ فشل إضافة التعليق الأول."
            await query.edit_message_text(
                f"✅ تم النشر بنجاح على إنستجرام!\n\n"
                f"📱 المنصة: {platform}\n"
                f"🆔 معرف المنشور: {ig_id}"
                f"{first_comment_note}"
            )
        else:
            log_post(url, platform, caption, "failed", user_id=query.from_user.id)
            await query.edit_message_text(f"❌ فشل النشر!\nالخطأ: {error}")

    elif data == "post_youtube":
        await query.edit_message_text("▶️ جاري الرفع على YouTube Shorts... قد يستغرق بعض الوقت")
        video_path_yt = f"yt_temp_{int(time.time())}.mp4"
        try:
            r = requests.get(cloudinary_url, timeout=120)
            with open(video_path_yt, "wb") as f:
                f.write(r.content)
            vid_title = context.user_data.get("video_title") or caption[:100]
            yt_id, yt_err = post_to_youtube_shorts(video_path_yt, vid_title, caption)
        finally:
            cleanup_file(video_path_yt)
        if yt_id:
            await query.edit_message_text(
                f"✅ تم الرفع على YouTube Shorts!\n\n"
                f"🔗 https://www.youtube.com/shorts/{yt_id}\n"
                f"📱 المنصة الأصلية: {platform}"
            )
        else:
            await query.edit_message_text(f"❌ فشل الرفع على YouTube!\nالخطأ: {yt_err}")

    elif data == "post_facebook":
        await query.edit_message_text("📘 جاري الرفع على Facebook Page...")
        fb_id, fb_err = post_to_facebook_page(cloudinary_url, caption)
        if fb_id:
            await query.edit_message_text(
                f"✅ تم النشر على Facebook!\n\n"
                f"🆔 معرف الفيديو: {fb_id}\n"
                f"📱 المنصة الأصلية: {platform}"
            )
        else:
            await query.edit_message_text(f"❌ فشل النشر على Facebook!\nالخطأ: {fb_err}")

    elif data in ("post_both", "post_all", "post_ig_yt", "post_ig_fb", "post_yt_fb"):
        do_ig = data in ("post_both", "post_all", "post_ig_yt", "post_ig_fb")
        do_yt = data in ("post_both", "post_all", "post_ig_yt", "post_yt_fb")
        do_fb = data in ("post_all", "post_ig_fb", "post_yt_fb")

        targets = []
        if do_ig: targets.append("إنستجرام")
        if do_yt: targets.append("YouTube")
        if do_fb: targets.append("Facebook")
        await query.edit_message_text("⏳ جاري النشر على " + " و ".join(targets) + "...")

        ig_id = ig_err = None
        if do_ig:
            ig_id, ig_err = post_to_instagram(cloudinary_url, caption)

        yt_id = yt_err = None
        if do_yt:
            video_path_yt = f"yt_temp_{int(time.time())}.mp4"
            try:
                r = requests.get(cloudinary_url, timeout=120)
                with open(video_path_yt, "wb") as f:
                    f.write(r.content)
                vid_title = context.user_data.get("video_title") or caption[:100]
                yt_id, yt_err = post_to_youtube_shorts(video_path_yt, vid_title, caption)
            except Exception as e:
                yt_err = str(e)
            finally:
                cleanup_file(video_path_yt)

        fb_id = fb_err = None
        if do_fb:
            fb_id, fb_err = post_to_facebook_page(cloudinary_url, caption)

        if do_ig and ig_id:
            log_post(url, platform, caption, "success", ig_post_id=ig_id, user_id=query.from_user.id)
            if get_setting("auto_first_comment") == "true":
                post_first_comment(ig_id, caption.strip())

        lines = []
        if do_ig:
            lines.append(f"✅ إنستجرام: {ig_id}" if ig_id else f"❌ إنستجرام: {ig_err}")
        if do_yt:
            lines.append(f"✅ YouTube: https://www.youtube.com/shorts/{yt_id}" if yt_id else f"❌ YouTube: {yt_err}")
        if do_fb:
            lines.append(f"✅ Facebook: {fb_id}" if fb_id else f"❌ Facebook: {fb_err}")
        await query.edit_message_text("📊 نتائج النشر:\n\n" + "\n".join(lines))

    elif data == "schedule_post":
        tz = get_setting("timezone") or "Africa/Cairo"
        await query.edit_message_text(
            f"⏰ اكتب وقت النشر بالصيغة التالية:\n"
            f"YYYY-MM-DD HH:MM\n\n"
            f"مثال: {(datetime.now() + timedelta(hours=2)).strftime('%Y-%m-%d %H:%M')}\n\n"
            f"⏱️ المنطقة الزمنية: {tz}"
        )
        context.user_data["waiting_for"] = "schedule_time"

    elif data == "edit_caption":
        current_title = context.user_data.get("video_title") or caption.split("\n\n")[0]
        await query.edit_message_text(
            f"✏️ العنوان الحالي:\n{current_title[:300]}\n\n"
            "اكتب العنوان الجديد (سطر واحد قصير):"
        )
        context.user_data["waiting_for"] = "custom_caption"

    elif data == "ai_caption":
        if not gemini_client:
            await query.answer("❌ الذكاء الاصطناعي غير مفعل. أضف GEMINI_API_KEY في إعدادات البيئة.", show_alert=True)
            return
        await query.edit_message_text("🤖 جاري توليد عنوان بالذكاء الاصطناعي...")
        original_title = context.user_data.get("video_title") or caption.split("\n\n")[0]
        ai_title = await enhance_caption_with_ai(original_title, platform)
        if ai_title:
            new_caption = ai_title
            context.user_data["video_title"] = ai_title
            context.user_data["caption"]     = new_caption
            keyboard = [
                [
                    InlineKeyboardButton("✅ احتفظ بهذا العنوان",  callback_data="caption_ok"),
                    InlineKeyboardButton("🔄 جرّب عنواناً آخر",   callback_data="ai_caption"),
                ],
                [
                    InlineKeyboardButton("✏️ عدّله يدوياً",       callback_data="edit_caption"),
                    InlineKeyboardButton("❌ إلغاء",                callback_data="cancel_post"),
                ],
            ]
            await query.edit_message_text(
                f"🤖 العنوان المُولَّد بالذكاء الاصطناعي:\n\n"
                f"{'─' * 28}\n"
                f"{ai_title}\n"
                f"{'─' * 28}\n\n"
                f"هل يعجبك هذا العنوان؟",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await query.edit_message_text("❌ فشل توليد العنوان. حاول مرة أخرى.")

    elif data == "add_to_queue":
        cloudinary_url = context.user_data.get("cloudinary_url")
        caption        = context.user_data.get("caption", "")
        url            = context.user_data.get("url", "")
        platform       = context.user_data.get("platform", "")
        queue_add(url, platform, caption, cloudinary_url,
                  query.from_user.id, query.message.chat_id)
        count = queue_count()
        is_running = get_setting("queue_is_running") == "true"
        status_line = "▶️ والنشر التلقائي يعمل!" if is_running else "⏸ النشر التلقائي متوقف."
        keyboard = [[InlineKeyboardButton("📋 إدارة القائمة", callback_data="open_queue")]]
        await query.edit_message_text(
            f"✅ تمت الإضافة للقائمة!\n\n"
            f"📦 إجمالي الفيديوهات في القائمة: {count}\n"
            f"🔄 {status_line}\n\n"
            f"اكتب /queue لإدارة القائمة والنشر التلقائي.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data.clear()

    elif data == "open_queue":
        text, kb = get_queue_menu_text_and_keyboard()
        await query.edit_message_text(text, reply_markup=kb)

    elif data == "voiceover_start":
        # عرض اختيار الصوت
        kb = [
            [InlineKeyboardButton(VOICE_OPTIONS["male"][1],    callback_data="voice_pick_male")],
            [InlineKeyboardButton(VOICE_OPTIONS["female"][1],  callback_data="voice_pick_female")],
            [InlineKeyboardButton(VOICE_OPTIONS["saudi_m"][1], callback_data="voice_pick_saudi_m")],
            [InlineKeyboardButton(VOICE_OPTIONS["saudi_f"][1], callback_data="voice_pick_saudi_f")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="caption_ok")],
        ]
        await query.edit_message_text(
            "🎙️ *اختر صوت التعليق:*\n\n"
            "بعد ما تختار الصوت، هطلب منك تكتب النص اللي عايزه يتقال.\n\n"
            "✅ *مفيش حد لطول النص* — البوت هيظبط سرعة الصوت تلقائياً عشان يغطي الفيديو من أوله لآخره.\n"
            "🔇 الصوت الأصلي للفيديو هيتكتم تماماً.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif data.startswith("voice_pick_"):
        voice_key = data.replace("voice_pick_", "")
        if voice_key not in VOICE_OPTIONS:
            await query.edit_message_text("❌ صوت غير معروف.")
            return
        voice_id, voice_label = VOICE_OPTIONS[voice_key]
        context.user_data["voiceover_voice"]    = voice_id
        context.user_data["voiceover_voice_lbl"]= voice_label
        context.user_data["waiting_for"]        = "voiceover_text"
        suggested = (context.user_data.get("video_title") or "")[:120]
        suggestion_line = f"\n\n💡 اقتراح من العنوان الحالي:\n_{suggested}_" if suggested else ""
        await query.edit_message_text(
            f"✍️ *اكتب النص اللي عايز التعليق الصوتي يقوله:*\n\n"
            f"الصوت المختار: {voice_label}{suggestion_line}\n\n"
            f"📝 ابعت النص في رسالة عادية، أو ابعت /skip لإلغاء.",
            parse_mode="Markdown"
        )

    elif data == "cancel_post":
        context.user_data.clear()
        await query.edit_message_text("✅ تم إلغاء العملية.")

async def apply_voiceover_to_video(update, context, text, voice_id):
    """ينزّل الفيديو من Cloudinary، يضيف تعليق صوتي، يرفعه تاني، ويبعت معاينة."""
    chat_id = update.effective_chat.id
    cloudinary_url = context.user_data.get("cloudinary_url")
    if not cloudinary_url:
        await update.message.reply_text("❌ مفيش فيديو حالياً. ابدأ من الأول بإرسال رابط أو فيديو.")
        return

    status = await update.message.reply_text("🎙️ جاري توليد التعليق الصوتي...")
    ts = int(time.time())
    src_video = f"vo_src_{ts}.mp4"
    audio_path = None
    mixed_path = None
    try:
        # 1) نزّل الفيديو الحالي
        r = requests.get(cloudinary_url, timeout=180)
        if r.status_code != 200:
            await status.edit_text("❌ فشل تحميل الفيديو من Cloudinary.")
            return
        with open(src_video, "wb") as f:
            f.write(r.content)

        # 2) ولّد الصوت
        await status.edit_text("🗣️ جاري توليد الصوت بـ AI...")
        audio_path = await generate_voiceover_audio(text, voice=voice_id, output_path=f"vo_audio_{ts}.mp3")
        if not audio_path:
            await status.edit_text("❌ فشل توليد الصوت. جرّب نص أقصر.")
            return

        # 3) ادمجه مع الفيديو
        await status.edit_text("🎬 جاري دمج الصوت مع الفيديو...")
        mixed_path = mix_voiceover_with_video(src_video, audio_path, output_path=f"vo_mixed_{ts}.mp4")
        if not mixed_path:
            await status.edit_text("❌ فشل دمج الصوت مع الفيديو.")
            return

        # 4) ارفع النسخة الجديدة على Cloudinary
        await status.edit_text("☁️ جاري رفع الفيديو الجديد...")
        new_url = upload_to_cloudinary(mixed_path)
        if not new_url:
            await status.edit_text("❌ فشل رفع الفيديو الجديد.")
            return

        # 5) حدّث context وأرسل المعاينة + الأزرار
        context.user_data["cloudinary_url"]   = new_url
        context.user_data["voiceover_done"]   = True
        context.user_data["voiceover_text"]   = text

        await status.edit_text("✅ تم إضافة التعليق الصوتي بنجاح! جاري إرسال المعاينة...")

        # ابعت الفيديو للمعاينة
        try:
            with open(mixed_path, "rb") as vf:
                await context.bot.send_video(
                    chat_id=chat_id,
                    video=vf,
                    caption=f"🎙️ معاينة الفيديو بعد إضافة التعليق الصوتي\n\n📝 النص: {text[:200]}",
                    supports_streaming=True
                )
        except Exception as e:
            logger.warning(f"Failed to send preview video: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ تم الدمج بنجاح (تعذّر إرسال المعاينة لكن النشر هيتم بالنسخة الجديدة)\n🔗 {new_url}"
            )

        # ابعت أزرار النشر تاني
        caption = context.user_data.get("caption", "")
        caption_preview = caption[:150] + ("..." if len(caption) > 150 else "")
        rows, yt_enabled, fb_enabled = build_post_keyboard(voiceover_done=True)
        if yt_enabled or fb_enabled:
            extras = []
            if yt_enabled: extras.append("YouTube")
            if fb_enabled: extras.append("Facebook")
            posting_text = "اختر طريقة النشر:\n\n📌 المنصات الإضافية المفعّلة: " + " + ".join(extras)
        else:
            posting_text = "اختر طريقة النشر:"
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"✅ التعليق الصوتي اتضاف!\n\n💬 الكابشن:\n{caption_preview}\n\n{posting_text}",
            reply_markup=InlineKeyboardMarkup(rows)
        )
    except Exception as e:
        logger.error(f"apply_voiceover error: {e}")
        try:
            await status.edit_text(f"❌ خطأ: {str(e)[:200]}")
        except:
            pass
    finally:
        cleanup_file(src_video)
        if audio_path: cleanup_file(audio_path)
        if mixed_path: cleanup_file(mixed_path)

async def handle_schedule_time_input(update, context, text):
    try:
        tz_name = get_setting("timezone") or "Africa/Cairo"
        tz = pytz.timezone(tz_name)
        scheduled_dt = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M")
        scheduled_dt = tz.localize(scheduled_dt)
        now = datetime.now(tz)
        if scheduled_dt <= now:
            await update.message.reply_text("❌ الوقت المحدد في الماضي! اكتب وقتاً في المستقبل.")
            return
        cloudinary_url = context.user_data.get("cloudinary_url")
        caption = context.user_data.get("caption", "")
        url = context.user_data.get("url", "")
        platform = context.user_data.get("platform", "")
        user_id = update.effective_user.id

        log_post(url, platform, caption, "scheduled", scheduled_at=scheduled_dt.isoformat(), user_id=user_id)

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT last_insert_rowid()")
        post_db_id = c.fetchone()[0]
        conn.close()

        bot_loop = asyncio.get_event_loop()
        scheduler.add_job(
            execute_scheduled_post,
            'date',
            run_date=scheduled_dt,
            args=[cloudinary_url, caption, url, platform, user_id, post_db_id, update.effective_chat.id, bot_loop],
            id=f"post_{post_db_id}",
            replace_existing=True
        )
        context.user_data.pop("waiting_for", None)
        await update.message.reply_text(
            f"✅ تم جدولة المنشور بنجاح!\n\n"
            f"📅 سيُنشر في: {scheduled_dt.strftime('%Y-%m-%d %H:%M')} ({tz_name})\n"
            f"🆔 رقم المنشور: #{post_db_id}\n\n"
            f"لإلغائه اكتب: /cancel_{post_db_id}"
        )
    except ValueError:
        await update.message.reply_text(
            "❌ الصيغة غير صحيحة!\nاكتب الوقت بالصيغة: YYYY-MM-DD HH:MM\n"
            f"مثال: {(datetime.now() + timedelta(hours=2)).strftime('%Y-%m-%d %H:%M')}"
        )

async def handle_custom_caption_input(update, context, text):
    new_title = text.strip().split("\n")[0]
    new_caption = new_title
    context.user_data["video_title"] = new_title
    context.user_data["caption"]     = new_caption
    context.user_data.pop("waiting_for", None)
    voiced = bool(context.user_data.get("voiceover_done"))
    rows, yt_enabled, fb_enabled = build_post_keyboard(voiceover_done=voiced)
    keyboard = rows

    if yt_enabled or fb_enabled:
        extras = []
        if yt_enabled: extras.append("YouTube")
        if fb_enabled: extras.append("Facebook")
        posting_text = "اختر طريقة النشر:\n\n📌 المنصات الإضافية المفعّلة: " + " + ".join(extras)
    else:
        posting_text = "اختر طريقة النشر:"
    preview = new_title[:200] + ("..." if len(new_title) > 200 else "")
    await update.message.reply_text(
        f"✅ تم حفظ العنوان الجديد:\n\n"
        f"{'─' * 28}\n"
        f"{preview}\n"
        f"{'─' * 28}\n\n"
        f"{posting_text}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    role = get_user_role(uid)
    if role == "blocked":
        return
    if role in ("pending", "unknown"):
        await update.message.reply_text("⏳ طلبك قيد المراجعة. أرسل /start للتسجيل.")
        return
    text = update.message.text.strip()
    waiting = context.user_data.get("waiting_for")
    if waiting == "timezone":
        try:
            pytz.timezone(text)
            set_setting("timezone", text)
            context.user_data.pop("waiting_for", None)
            await update.message.reply_text(f"✅ تم تغيير المنطقة الزمنية إلى: {text}")
        except:
            await update.message.reply_text("❌ اسم غير صحيح. جرب مثلاً: Africa/Cairo")
    elif waiting == "max_duration":
        try:
            val = int(text)
            if val < 0: raise ValueError
            set_setting("max_video_duration", str(val))
            context.user_data.pop("waiting_for", None)
            msg = f"✅ تم ضبط الحد الأقصى للمدة: {val} ثانية" if val > 0 else "✅ تم إلغاء حد المدة"
            await update.message.reply_text(msg)
        except:
            await update.message.reply_text("❌ أدخل رقماً صحيحاً (مثل 90)")
    elif waiting == "caption_template":
        set_setting("default_caption_template", text)
        context.user_data.pop("waiting_for", None)
        await update.message.reply_text("✅ تم حفظ قالب الوصف!")
    elif waiting == "schedule_time":
        await handle_schedule_time_input(update, context, text)
    elif waiting == "custom_caption":
        await handle_custom_caption_input(update, context, text)
    elif waiting == "voiceover_text":
        if text.strip().lower() in ("/skip", "skip", "إلغاء", "الغاء"):
            context.user_data.pop("waiting_for", None)
            await update.message.reply_text("✅ تم إلغاء التعليق الصوتي.")
            return
        if len(text.strip()) < 2:
            await update.message.reply_text("❌ النص قصير جداً. اكتب جملة كاملة.")
            return
        voice_id = context.user_data.get("voiceover_voice", "ar-EG-ShakirNeural")
        context.user_data.pop("waiting_for", None)
        await apply_voiceover_to_video(update, context, text.strip(), voice_id)
    elif waiting == "new_ig_token":
        if len(text) < 20:
            await update.message.reply_text("❌ التوكن قصير جداً، تأكد منه وأعد الإرسال.")
            return
        set_setting("ig_access_token_override", text)
        context.user_data.pop("waiting_for", None)
        await update.message.reply_text(
            "✅ تم حفظ Instagram Access Token بنجاح!\n\n"
            "استخدم /test_instagram للتحقق من صحته."
        )
    elif waiting == "new_yt_token":
        if len(text) < 20:
            await update.message.reply_text("❌ التوكن قصير جداً، تأكد منه وأعد الإرسال.")
            return
        set_setting("yt_refresh_token_override", text)
        context.user_data.pop("waiting_for", None)
        await update.message.reply_text(
            "✅ تم حفظ YouTube Refresh Token بنجاح!\n\n"
            "استخدم /test_youtube للتحقق من صحته."
        )
    elif waiting == "new_fb_token":
        if len(text) < 20:
            await update.message.reply_text("❌ التوكن قصير جداً، تأكد منه وأعد الإرسال.")
            return
        set_setting("fb_page_token_override", text)
        context.user_data.pop("waiting_for", None)
        await update.message.reply_text(
            "✅ تم حفظ Facebook Page Access Token بنجاح!"
        )
    elif waiting == "new_fb_page_id":
        new_id = text.strip()
        if not new_id.isdigit() or len(new_id) < 5:
            await update.message.reply_text("❌ Page ID لازم يكون أرقام فقط (8 أرقام أو أكتر). أعد الإرسال.")
            return
        set_setting("fb_page_id_override", new_id)
        context.user_data.pop("waiting_for", None)
        await update.message.reply_text(
            f"✅ تم حفظ Facebook Page ID بنجاح!\n\n"
            f"🆔 ID الجديد: `{new_id}`\n\n"
            f"جرّب تنشر فيديو دلوقتي."
        )
    else:
        # استخراج كل الروابط من الرسالة (دعم روابط متعددة)
        urls = re.findall(r'https?://\S+', text)
        if len(urls) > 1:
            await _handle_bulk_urls(update, context, urls)
        elif len(urls) == 1:
            context.user_data["_single_url"] = urls[0]
            await handle_url(update, context)

async def _handle_bulk_urls(update: Update, context: ContextTypes.DEFAULT_TYPE, urls: list):
    """معالجة روابط متعددة في رسالة واحدة وإضافتها للقائمة"""
    valid_urls = [u for u in urls if detect_platform(u) != "Unknown"]
    if not valid_urls:
        await update.message.reply_text("❌ لم يتم العثور على روابط من منصات مدعومة.")
        return
    status_msg = await update.message.reply_text(
        f"🔄 تم اكتشاف {len(valid_urls)} رابط\n"
        f"سيتم تحميلها وإضافتها للقائمة تلقائياً...\n\n"
        f"{'⏳ ' + chr(10).join(valid_urls[:5])}"
    )
    added = 0
    failed = 0
    for i, url in enumerate(valid_urls):
        try:
            await status_msg.edit_text(
                f"⏳ جاري معالجة {i+1}/{len(valid_urls)}...\n🔗 {url[:60]}"
            )
            platform = detect_platform(url)
            video_path, title, description, uploader, info = download_video(url)
            if not video_path or not os.path.exists(video_path):
                failed += 1
                continue
            # تحويل عمودي إن كان مفعّلاً
            if get_setting("auto_vertical") == "true":
                video_path = convert_to_vertical(video_path)
            cloudinary_url = upload_to_cloudinary(video_path)
            cleanup_file(video_path)
            if not cloudinary_url:
                failed += 1
                continue
            def _c(t):
                t = (t or "").strip()
                return "" if t.startswith("http") else t
            extracted = _c(title) or _c(description) or ""
            caption = strip_hashtags(extracted)
            queue_add(url, platform, caption, cloudinary_url,
                      update.effective_user.id, update.effective_chat.id)
            added += 1
        except Exception as e:
            logger.error(f"Bulk URL error ({url}): {e}")
            failed += 1
    is_running = get_setting("queue_is_running") == "true"
    kb = [[InlineKeyboardButton("📋 إدارة القائمة", callback_data="open_queue")]]
    await status_msg.edit_text(
        f"✅ انتهت المعالجة!\n\n"
        f"📦 تمت إضافة: {added} فيديو\n"
        f"❌ فشل: {failed}\n"
        f"📋 إجمالي القائمة: {queue_count()}\n\n"
        f"{'▶️ النشر التلقائي يعمل.' if is_running else '⏸ اكتب /queue لبدء النشر التلقائي.'}",
        reply_markup=InlineKeyboardMarkup(kb)
    )

def execute_scheduled_post(cloudinary_url, caption, url, platform, user_id, post_db_id, chat_id, bot_loop):
    try:
        ig_id, error = post_to_instagram(cloudinary_url, caption)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        if ig_id:
            c.execute("UPDATE posts SET status='success', ig_post_id=? WHERE id=?", (ig_id, post_db_id))
            c.execute("INSERT OR IGNORE INTO analytics (date, total_posts, successful_posts, failed_posts, scheduled_posts) VALUES (?,0,0,0,0)", (today,))
            c.execute("UPDATE analytics SET successful_posts = successful_posts + 1 WHERE date=?", (today,))
        else:
            c.execute("UPDATE posts SET status='failed' WHERE id=?", (post_db_id,))
            c.execute("INSERT OR IGNORE INTO analytics (date, total_posts, successful_posts, failed_posts, scheduled_posts) VALUES (?,0,0,0,0)", (today,))
            c.execute("UPDATE analytics SET failed_posts = failed_posts + 1 WHERE date=?", (today,))
        conn.commit()
        conn.close()
        if ig_id and get_setting("notify_success") == "true":
            asyncio.run_coroutine_threadsafe(
                app.bot.send_message(chat_id=chat_id, text=f"✅ تم نشر المنشور المجدول #{post_db_id} بنجاح!"),
                bot_loop
            )
        elif not ig_id and get_setting("notify_fail") == "true":
            asyncio.run_coroutine_threadsafe(
                app.bot.send_message(chat_id=chat_id, text=f"❌ فشل نشر المنشور المجدول #{post_db_id}!\nالخطأ: {error}"),
                bot_loop
            )
    except Exception as e:
        logger.error(f"Scheduled post error: {e}")

init_db()

jobstores = {'default': SQLAlchemyJobStore(url='sqlite:///scheduler_jobs.db')}
scheduler = BackgroundScheduler(jobstores=jobstores, timezone=pytz.utc)
scheduler.start()

async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    username = update.effective_user.username or "بدون"
    role     = get_user_role(uid)
    role_ar  = {"admin": "مشرف", "user": "مستخدم", "pending": "منتظر موافقة", "blocked": "محظور"}.get(role, "غير مسجل")
    await update.message.reply_text(
        f"🆔 معرّفك: `{uid}`\n"
        f"👤 اسم المستخدم: @{username}\n"
        f"🔰 صلاحيتك: {role_ar}",
        parse_mode="Markdown"
    )

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 هذا الأمر للمشرف فقط.")
        return
    users = db_get_all_users()
    if not users:
        await update.message.reply_text("📭 لا يوجد مستخدمون مسجلون بعد.")
        return
    role_icon = {"admin": "👑", "user": "✅", "pending": "⏳", "blocked": "🚫"}
    require_approval = get_setting("require_approval") == "true"
    kb = [
        [InlineKeyboardButton(
            f"{'🔒 تعطيل الموافقة' if require_approval else '🔓 تفعيل الموافقة'}",
            callback_data="usr_toggle_approval"
        )]
    ]
    text = f"👥 المستخدمون ({len(users)}):\n\n"
    for u in users:
        tid, uname, fname, role, added_at = u
        icon = role_icon.get(role, "❓")
        date = added_at[:10] if added_at else "؟"
        text += f"{icon} {fname or uname or tid} | @{uname or 'بدون'} | {date}\n"
        # أزرار إدارة لكل مستخدم (مش الـ admin العريق)
        if not (ADMIN_USER_ID and tid == ADMIN_USER_ID):
            row = []
            if role != "admin":    row.append(InlineKeyboardButton("👑 ترقية", callback_data=f"usr_admin_{tid}"))
            if role != "user":     row.append(InlineKeyboardButton("✅ موافقة", callback_data=f"usr_approve_{tid}"))
            if role != "blocked":  row.append(InlineKeyboardButton("🚫 حظر",   callback_data=f"usr_block_{tid}"))
            if row: kb.append(row)
    text += f"\n{'🔒 الموافقة مطلوبة للمستخدمين الجدد' if require_approval else '🔓 مفتوح للجميع تلقائياً'}"
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))

async def users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.answer("🚫 غير مصرح لك.", show_alert=True)
        return
    data = query.data
    if data == "usr_toggle_approval":
        current = get_setting("require_approval") == "true"
        set_setting("require_approval", "false" if current else "true")
        status = "🔒 تم تفعيل الموافقة للمستخدمين الجدد" if not current else "🔓 تم فتح البوت للجميع تلقائياً"
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(status)
        return
    parts = data.split("_")
    if len(parts) < 3:
        return
    action = parts[1]
    try:
        target_id = int(parts[2])
    except ValueError:
        return
    target = db_get_user(target_id)
    target_name = (target[2] or target[1] or str(target_id)) if target else str(target_id)
    if action == "approve":
        db_set_user_role(target_id, "user")
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"✅ تم قبول {target_name} كمستخدم.")
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="✅ تمت الموافقة على طلبك! يمكنك الآن استخدام البوت.\n\nأرسل /start للبدء."
            )
        except Exception:
            pass
    elif action == "block":
        db_set_user_role(target_id, "blocked")
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"🚫 تم حظر {target_name}.")
        try:
            await context.bot.send_message(chat_id=target_id, text="🚫 تم حظرك من استخدام البوت.")
        except Exception:
            pass
    elif action == "admin":
        db_set_user_role(target_id, "admin")
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"👑 تم ترقية {target_name} إلى مشرف.")
        try:
            await context.bot.send_message(chat_id=target_id, text="👑 تم ترقيتك إلى مشرف في البوت!")
        except Exception:
            pass

async def update_token_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📸 تحديث توكن Instagram",  callback_data="tok_ig")],
        [InlineKeyboardButton("▶️ تحديث Refresh Token YouTube", callback_data="tok_yt")],
        [InlineKeyboardButton("📘 تحديث توكن Facebook Page", callback_data="tok_fb")],
        [InlineKeyboardButton("🆔 تحديث Facebook Page ID", callback_data="tok_fb_id")],
        [InlineKeyboardButton("🗑️ حذف التوكنات المخزنة (استخدم الـ Secrets)", callback_data="tok_clear")],
    ]
    ig_stored   = "✅ مخزن في البوت" if get_setting("ig_access_token_override") else "🔸 من Secrets"
    yt_stored   = "✅ مخزن في البوت" if get_setting("yt_refresh_token_override") else "🔸 من Secrets"
    fb_stored   = "✅ مخزن في البوت" if get_setting("fb_page_token_override") else "🔸 من Secrets"
    fb_id_store = "✅ مخزن في البوت" if get_setting("fb_page_id_override") else "🔸 من Secrets"
    fb_id_now   = get_fb_page_id() or "—"
    await update.message.reply_text(
        f"🔐 إدارة التوكنات\n\n"
        f"📸 Instagram Access Token: {ig_stored}\n"
        f"▶️ YouTube Refresh Token: {yt_stored}\n"
        f"📘 Facebook Page Token: {fb_stored}\n"
        f"🆔 Facebook Page ID: {fb_id_store}  (`{fb_id_now}`)\n\n"
        f"اختر التوكن الذي تريد تحديثه:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def test_instagram_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (IG_USER_ID and IG_ACCESS_TOKEN):
        await update.message.reply_text(
            "❌ مفاتيح Instagram غير مضافة!\n\n"
            "أضف في Secrets:\n"
            "• IG_USER_ID\n"
            "• IG_ACCESS_TOKEN"
        )
        return
    msg = await update.message.reply_text("🔄 جاري اختبار الاتصال بـ Instagram...")
    try:
        r = requests.get(
            f"https://graph.facebook.com/v18.0/{IG_USER_ID}",
            params={
                "fields": "id,username,name,followers_count,media_count",
                "access_token": get_ig_token()
            },
            timeout=30
        ).json()
        if "error" in r:
            err = r["error"].get("message", "خطأ غير معروف")
            code = r["error"].get("code", "")
            await msg.edit_text(
                f"❌ فشل الاتصال بـ Instagram\n\n"
                f"الخطأ ({code}): {err}\n\n"
                f"تأكد من صحة IG_ACCESS_TOKEN وأنه لم تنته صلاحيته."
            )
        else:
            username  = r.get("username", "غير متاح")
            name      = r.get("name", "غير متاح")
            followers = r.get("followers_count", "غير متاح")
            media     = r.get("media_count", "غير متاح")
            followers_str = f"{followers:,}" if isinstance(followers, int) else str(followers)
            await msg.edit_text(
                f"✅ الاتصال بـ Instagram يعمل بنجاح!\n\n"
                f"👤 الحساب: @{username}\n"
                f"📛 الاسم: {name}\n"
                f"👥 المتابعون: {followers_str}\n"
                f"🎬 عدد المنشورات: {media}\n\n"
                f"البوت جاهز للنشر على Instagram Reels!"
            )
    except Exception as e:
        await msg.edit_text(f"❌ خطأ في الاتصال: {str(e)[:200]}")

async def test_youtube_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rt = get_yt_refresh_token()
    if not (YT_CLIENT_ID and YT_CLIENT_SECRET and rt):
        await update.message.reply_text(
            "❌ مفاتيح YouTube غير مضافة!\n\n"
            "أضف في Secrets:\n"
            "• YOUTUBE_CLIENT_ID\n"
            "• YOUTUBE_CLIENT_SECRET\n"
            "• YOUTUBE_REFRESH_TOKEN\n\n"
            "أو استخدم /update_token لإدخال التوكن مباشرة."
        )
        return
    msg = await update.message.reply_text("🔄 جاري اختبار الاتصال بـ YouTube...")
    try:
        r = requests.post("https://oauth2.googleapis.com/token", data={
            "client_id":     YT_CLIENT_ID,
            "client_secret": YT_CLIENT_SECRET,
            "refresh_token": rt,
            "grant_type":    "refresh_token",
        }, timeout=30).json()
        if "access_token" in r:
            yt_setting = get_setting("youtube_shorts") == "true"
            status = "✅ مفعّل" if yt_setting else "❌ معطّل (فعّله من /settings)"
            await msg.edit_text(
                f"✅ الاتصال بـ YouTube يعمل بنجاح!\n\n"
                f"🔑 Access Token: تم الحصول عليه\n"
                f"📺 YouTube Shorts في الإعدادات: {status}\n\n"
                f"البوت جاهز للنشر على YouTube Shorts!"
            )
        else:
            err = r.get("error_description") or r.get("error") or "خطأ غير معروف"
            await msg.edit_text(
                f"❌ فشل الاتصال بـ YouTube\n\n"
                f"الخطأ: {err}\n\n"
                f"تأكد من صحة المفاتيح في Secrets."
            )
    except Exception as e:
        await msg.edit_text(f"❌ خطأ في الاتصال: {str(e)[:200]}")

async def test_facebook_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    page_id = get_fb_page_id()
    token   = get_fb_token()
    if not (page_id and token):
        await update.message.reply_text(
            "❌ بيانات Facebook غير مكتملة!\n\n"
            "حدّث من /update_token:\n"
            "• 📘 Facebook Page Token\n"
            "• 🆔 Facebook Page ID"
        )
        return
    msg = await update.message.reply_text("🔄 جاري اختبار الاتصال بـ Facebook...")
    try:
        # 1) فحص التوكن نفسه + الصلاحيات
        me = requests.get(
            "https://graph.facebook.com/v18.0/me",
            params={"fields": "id,name", "access_token": token},
            timeout=30
        ).json()
        if "error" in me:
            err = me["error"].get("message", "خطأ غير معروف")
            await msg.edit_text(
                f"❌ التوكن غير صالح!\n\n"
                f"الخطأ: {err}\n\n"
                f"حدّث التوكن من /update_token"
            )
            return

        token_owner_id   = me.get("id", "—")
        token_owner_name = me.get("name", "—")

        # 2) فحص الـ Page ID المحفوظ
        page = requests.get(
            f"https://graph.facebook.com/v18.0/{page_id}",
            params={"fields": "id,name,category,fan_count", "access_token": token},
            timeout=30
        ).json()

        if "error" in page:
            err = page["error"].get("message", "خطأ غير معروف")
            await msg.edit_text(
                f"❌ Page ID لا يعمل مع هذا التوكن!\n\n"
                f"🆔 Page ID المحفوظ: `{page_id}`\n"
                f"👤 التوكن مرتبط بـ: {token_owner_name} (`{token_owner_id}`)\n\n"
                f"الخطأ: {err}\n\n"
                f"💡 الحلول:\n"
                f"• تأكد إن الـ Page ID رقم صفحة (Page) مش حساب شخصي\n"
                f"• استخدم Page Access Token مش User Token\n"
                f"• حدّث من /update_token"
            )
            return

        # 3) فحص صلاحيات النشر
        perms_check = requests.get(
            "https://graph.facebook.com/v18.0/me/permissions",
            params={"access_token": token},
            timeout=30
        ).json()
        granted = []
        if "data" in perms_check:
            granted = [p["permission"] for p in perms_check["data"] if p.get("status") == "granted"]

        required = ["pages_manage_posts", "pages_read_engagement", "publish_video"]
        missing  = [p for p in required if p not in granted]

        fb_setting = get_setting("facebook_page") == "true"
        status     = "✅ مفعّل" if fb_setting else "❌ معطّل (فعّله من /settings)"

        text = (
            f"✅ الاتصال بـ Facebook يعمل بنجاح!\n\n"
            f"📘 الصفحة: {page.get('name', '—')}\n"
            f"🆔 Page ID: `{page.get('id', '—')}`\n"
            f"📂 الفئة: {page.get('category', '—')}\n"
            f"👥 المتابعون: {page.get('fan_count', 0):,}\n\n"
            f"🔑 التوكن مرتبط بـ: {token_owner_name}\n"
            f"📊 Facebook في الإعدادات: {status}\n"
        )
        if missing:
            text += f"\n⚠️ صلاحيات ناقصة: {', '.join(missing)}\n(جدّد التوكن مع إضافتها)"
        else:
            text += f"\n✅ كل الصلاحيات المطلوبة موجودة!"

        await msg.edit_text(text)
    except Exception as e:
        await msg.edit_text(f"❌ خطأ في الاتصال: {str(e)[:200]}")

async def token_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "tok_ig":
        await query.edit_message_text(
            "📸 أرسل الـ Instagram Access Token الجديد:\n\n"
            "⚠️ التوكن يُحفظ في قاعدة بيانات البوت.\n"
            "للتراجع وحذفه، استخدم /update_token ثم 'حذف التوكنات'."
        )
        context.user_data["waiting_for"] = "new_ig_token"
    elif data == "tok_yt":
        await query.edit_message_text(
            "▶️ أرسل الـ YouTube Refresh Token الجديد:\n\n"
            "⚠️ التوكن يُحفظ في قاعدة بيانات البوت.\n"
            "للتراجع وحذفه، استخدم /update_token ثم 'حذف التوكنات'."
        )
        context.user_data["waiting_for"] = "new_yt_token"
    elif data == "tok_fb":
        await query.edit_message_text(
            "📘 أرسل الـ Facebook Page Access Token الجديد:\n\n"
            "⚠️ التوكن يُحفظ في قاعدة بيانات البوت.\n"
            "للتراجع وحذفه، استخدم /update_token ثم 'حذف التوكنات'."
        )
        context.user_data["waiting_for"] = "new_fb_token"
    elif data == "tok_fb_id":
        await query.edit_message_text(
            "🆔 أرسل Facebook Page ID الجديد:\n\n"
            "🔍 لجلب الـ ID الصحيح:\n"
            "• ادخل صفحتك → About → Page transparency → Page ID\n"
            "• أو استخدم: https://findmyfbid.com\n\n"
            "⚠️ لازم يكون رقم الصفحة (Page) مش الحساب الشخصي."
        )
        context.user_data["waiting_for"] = "new_fb_page_id"
    elif data == "tok_clear":
        set_setting("ig_access_token_override", "")
        set_setting("yt_refresh_token_override", "")
        set_setting("fb_page_token_override", "")
        set_setting("fb_page_id_override", "")
        await query.edit_message_text(
            "🗑️ تم حذف التوكنات المخزنة في البوت.\n\n"
            "البوت سيستخدم التوكنات من Secrets الآن."
        )

async def post_init(application):
    from telegram import BotCommand, BotCommandScopeDefault
    commands = [
        BotCommand("start",     "🏠 الرئيسية - ابدأ من هنا"),
        BotCommand("queue",     "📋 قائمة الانتظار والنشر التلقائي"),
        BotCommand("settings",  "⚙️ الإعدادات - هاشتاقات، AI، إشعارات"),
        BotCommand("analytics", "📊 التحليل اليومي والإحصائيات"),
        BotCommand("scheduled", "⏰ المنشورات المجدولة بتاريخ"),
        BotCommand("help",         "📖 دليل الاستخدام الكامل"),
        BotCommand("test_youtube",   "▶️ اختبار الاتصال بـ YouTube"),
        BotCommand("test_instagram", "📸 اختبار الاتصال بـ Instagram"),
        BotCommand("test_facebook",  "📘 اختبار الاتصال بـ Facebook"),
        BotCommand("update_token",   "🔐 تحديث التوكنات مباشرة من البوت"),
        BotCommand("users",          "👥 إدارة المستخدمين والصلاحيات"),
        BotCommand("myid",           "🆔 عرض معرّفك وصلاحيتك"),
    ]
    await application.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    await application.bot.set_my_description(
        "🤖 بوت نشر الفيديوهات على إنستجرام\n\n"
        "📥 أرسل رابط من: TikTok | YouTube | Instagram | Facebook | Twitter/X | Reddit | Vimeo\n"
        "📤 وسأنشره تلقائياً على إنستجرام كـ Reel مع الوصف والهاشتاقات"
    )
    await application.bot.set_my_short_description(
        "يحوّل روابط الفيديو إلى Reels على إنستجرام تلقائياً 🎬"
    )

app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
app.add_handler(CommandHandler("start",     start))
app.add_handler(CommandHandler("help",      help_command))
app.add_handler(CommandHandler("settings",  settings_menu))
app.add_handler(CommandHandler("analytics", analytics_command))
app.add_handler(CommandHandler("scheduled", scheduled_posts_command))
app.add_handler(CommandHandler("queue",        queue_command))
app.add_handler(CommandHandler("test_youtube",   test_youtube_command))
app.add_handler(CommandHandler("test_instagram", test_instagram_command))
app.add_handler(CommandHandler("test_facebook",  test_facebook_command))
app.add_handler(CommandHandler("update_token",   update_token_command))
app.add_handler(CommandHandler("users",          users_command))
app.add_handler(CommandHandler("myid",           myid_command))
app.add_handler(MessageHandler(filters.Regex(r'^/cancel_\d+$'), cancel_scheduled))
app.add_handler(CallbackQueryHandler(users_callback,       pattern=r'^usr_'))
app.add_handler(CallbackQueryHandler(token_callback,       pattern=r'^tok_'))
app.add_handler(CallbackQueryHandler(settings_callback,    pattern=r'^(toggle_|set_|close_)'))
app.add_handler(CallbackQueryHandler(queue_callback,       pattern=r'^(qi_|q_)'))
app.add_handler(CallbackQueryHandler(video_action_callback,pattern=r'^(caption_ok|post_now|post_youtube|post_facebook|post_both|post_all|post_ig_yt|post_ig_fb|post_yt_fb|schedule_post|edit_caption|ai_caption|cancel_post|add_to_queue|open_queue|force_repost|voiceover_start|voice_pick_male|voice_pick_female|voice_pick_saudi_m|voice_pick_saudi_f)$'))
app.add_handler(MessageHandler(filters.VIDEO | filters.ANIMATION | filters.Document.VIDEO, handle_video_upload))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        c.execute("SELECT total_posts, successful_posts, failed_posts FROM analytics WHERE date=?", (today,))
        row = c.fetchone()
        conn.close()
        total = row[0] if row else 0
        success = row[1] if row else 0
        failed = row[2] if row else 0
        body = (
            f"<html><body style='font-family:sans-serif;text-align:center;padding:40px'>"
            f"<h2>🤖 البوت يعمل!</h2>"
            f"<p>📊 اليوم: {total} منشور | ✅ {success} نجح | ❌ {failed} فشل</p>"
            f"<p style='color:green'>✅ الحالة: نشط</p>"
            f"</body></html>"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, format, *args):
        pass

def run_health_server():
    port = int(os.environ.get("PORT", 3000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"Health server running on port {port}")
    server.serve_forever()

def self_ping_loop():
    """يعمل ping للبوت كل 4 دقايق عشان يفضل صاحي على Replit"""
    import time as _time
    _time.sleep(30)  # استنى 30 ثانية بعد البدء
    repl_url = os.environ.get("REPLIT_DEV_DOMAIN", "")
    if not repl_url:
        logger.info("Self-ping: REPLIT_DEV_DOMAIN not set, skipping.")
        return
    ping_url = f"https://{repl_url}:3000/"
    while True:
        try:
            r = requests.get(ping_url, timeout=10)
            logger.info(f"Self-ping OK: {r.status_code}")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")
        _time.sleep(240)  # كل 4 دقايق

health_thread = threading.Thread(target=run_health_server, daemon=True)
health_thread.start()

ping_thread = threading.Thread(target=self_ping_loop, daemon=True)
ping_thread.start()

print("البوت يعمل الآن بكل الميزات المتقدمة..")
logger.info("Bot started successfully")
app.run_polling(drop_pending_updates=True)
