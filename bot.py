import asyncio
import os
import re
import secrets
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InlineQueryResultCachedPhoto,
    InlineQueryResultPhoto,
    InputTextMessageContent,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ChatMemberStatus
from telegram.error import Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)
from yt_dlp import YoutubeDL

load_dotenv()

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
OG_TITLE_RE = re.compile(r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']', re.IGNORECASE)
OG_DESC_RE = re.compile(r'<meta\s+property=["\']og:description["\']\s+content=["\']([^"\']+)["\']', re.IGNORECASE)

MAX_FILE_SIZE_MB = 300
DEFAULT_CONCURRENT_JOBS = 8
INLINE_TOKEN_TTL_HOURS = 6

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".m4v"}
AUDIO_EXTENSIONS = {".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac"}

TEXTS = {
    "ru": {
        "welcome": (
            "Привет! Отправьте ссылку сразу (YouTube Shorts, TikTok, Instagram Reels, Pinterest, Spotify, VK, Я.Музыка, Likee).\n"
            "Также работает inline-режим: @username_бота <ссылка>."
        ),
        "choose_lang": "Выберите язык:",
        "lang_saved": "Язык сохранён.",
        "menu": "Меню открыто. Выберите действие:",
        "help": (
            "• Параллельная обработка запросов\n"
            "• Лимит файла: 300MB\n"
            "• Форматы: auto/mp4/mkv/mp3\n"
            "• В auto видео отправляется как video\n"
            "• Требуется подписка на канал"
        ),
        "need_sub": "Для использования бота подпишитесь на канал {channel} и повторите запрос.",
        "send_link": "Отправьте ссылку (http/https).",
        "bad_link": "Это не похоже на ссылку. Отправьте корректный URL.",
        "unsupported": "Ссылка не поддерживается текущим whitelist.",
        "queued": "Заявка принята в обработку ✅",
        "downloading": "Скачиваю медиа, подождите...",
        "sending": "Отправляю файл ({size:.1f}MB)...",
        "too_big": "Файл слишком большой: {size:.1f}MB. Лимит 300MB.",
        "done": "Готово.",
        "error": "Ошибка: {err}",
        "format_now": "Текущий формат: {fmt}",
        "format_saved": "Формат сохранён: {fmt}",
        "owner_only": "Эта функция только для владельца.",
        "broadcast_done": "Рассылка завершена. Успешно: {sent}, ошибок: {failed}",
        "ask_caption": "Отправьте новый текст подписи (поддерживаются emoji/premium emoji).",
        "caption_saved": "Подпись обновлена.",
        "ask_broadcast": "Отправьте сообщение для рассылки (текст/фото/видео и т.д.).",
        "cancelled": "Действие отменено.",
        "group_hint": "В чате напишите: @{username} <ссылка>",
        "ask_inline_text": "Отправьте текст для inline-карточки.",
        "inline_text_saved": "Текст inline-карточки обновлён.",
        "ask_inline_image": "Отправьте фото или ссылку на картинку для inline-карточки.",
        "inline_image_saved": "Изображение inline-карточки обновлено.",
        "inline_pick": "Выберите действие:",
        "inline_auto": "⬇️ Скачать авто",
        "inline_audio": "🎵 Скачать аудио",
        "inline_sent_pm": "Готово: отправил в ЛС.",
        "inline_need_start": "Откройте бота в ЛС и нажмите /start, затем повторите.",
    },
    "en": {
        "welcome": (
            "Hi! Send a link directly (YouTube Shorts, TikTok, Instagram Reels, Pinterest, Spotify, VK, Yandex Music, Likee).\n"
            "Inline mode also works: @bot_username <url>."
        ),
        "choose_lang": "Choose your language:",
        "lang_saved": "Language saved.",
        "menu": "Menu opened. Choose an action:",
        "help": (
            "• Parallel processing\n"
            "• File limit: 300MB\n"
            "• Formats: auto/mp4/mkv/mp3\n"
            "• In auto mode video is sent as Telegram video\n"
            "• Required channel subscription"
        ),
        "need_sub": "Please subscribe to {channel} and try again.",
        "send_link": "Send a link (http/https).",
        "bad_link": "This does not look like a valid URL.",
        "unsupported": "Unsupported URL by current whitelist.",
        "queued": "Request queued ✅",
        "downloading": "Downloading media...",
        "sending": "Uploading file ({size:.1f}MB)...",
        "too_big": "File too big: {size:.1f}MB. Limit is 300MB.",
        "done": "Done.",
        "error": "Error: {err}",
        "format_now": "Current format: {fmt}",
        "format_saved": "Format saved: {fmt}",
        "owner_only": "Owner-only function.",
        "broadcast_done": "Broadcast finished. Sent: {sent}, failed: {failed}",
        "ask_caption": "Send new caption text (emoji/premium emoji supported).",
        "caption_saved": "Caption updated.",
        "ask_broadcast": "Send broadcast message (text/photo/video/etc).",
        "cancelled": "Cancelled.",
        "group_hint": "In group send: @{username} <link>",
        "ask_inline_text": "Send inline card text.",
        "inline_text_saved": "Inline card text updated.",
        "ask_inline_image": "Send photo or image URL for inline card.",
        "inline_image_saved": "Inline card image updated.",
        "inline_pick": "Choose an action:",
        "inline_auto": "⬇️ Download auto",
        "inline_audio": "🎵 Download audio",
        "inline_sent_pm": "Done: sent to PM.",
        "inline_need_start": "Open bot in PM and press /start, then try again.",
    },
}


@dataclass
class Config:
    bot_token: str
    owner_id: int
    required_channel: str
    ytdlp_cookies_file: Optional[str]
    concurrent_jobs: int


class Storage:
    def __init__(self, db_path: str = "bot.db") -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                language TEXT DEFAULT 'ru',
                language_selected INTEGER DEFAULT 0,
                preferred_format TEXT DEFAULT 'auto',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_states (
                user_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inline_tokens (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._ensure_setting("media_caption", "")
        self._ensure_setting("inline_card_text", "Нажмите кнопку ниже для скачивания")
        self._ensure_setting("inline_card_photo_url", "")
        self._ensure_setting("inline_card_photo_file_id", "")
        self.conn.commit()

    def _ensure_setting(self, key: str, value: str) -> None:
        self.conn.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))

    def set_setting(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO bot_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_setting(self, key: str, default: str = "") -> str:
        row = self.conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    def upsert_user(self, user_id: int) -> None:
        self.conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        self.conn.commit()

    def has_language(self, user_id: int) -> bool:
        row = self.conn.execute("SELECT language_selected FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return bool(row and row[0] == 1)

    def get_language(self, user_id: int) -> str:
        self.upsert_user(user_id)
        row = self.conn.execute("SELECT language FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row[0] if row and row[0] in {"ru", "en"} else "ru"

    def set_language(self, user_id: int, lang: str) -> None:
        self.upsert_user(user_id)
        self.conn.execute("UPDATE users SET language = ?, language_selected = 1 WHERE user_id = ?", (lang, user_id))
        self.conn.commit()

    def set_format(self, user_id: int, preferred_format: str) -> None:
        self.upsert_user(user_id)
        self.conn.execute("UPDATE users SET preferred_format = ? WHERE user_id = ?", (preferred_format, user_id))
        self.conn.commit()

    def get_format(self, user_id: int) -> str:
        self.upsert_user(user_id)
        row = self.conn.execute("SELECT preferred_format FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row[0] if row else "auto"

    def all_users(self) -> list[int]:
        return [r[0] for r in self.conn.execute("SELECT user_id FROM users").fetchall()]

    def set_state(self, user_id: int, state: str) -> None:
        self.conn.execute(
            "INSERT INTO user_states (user_id, state) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET state = excluded.state",
            (user_id, state),
        )
        self.conn.commit()

    def get_state(self, user_id: int) -> Optional[str]:
        row = self.conn.execute("SELECT state FROM user_states WHERE user_id = ?", (user_id,)).fetchone()
        return row[0] if row else None

    def clear_state(self, user_id: int) -> None:
        self.conn.execute("DELETE FROM user_states WHERE user_id = ?", (user_id,))
        self.conn.commit()

    def create_inline_token(self, user_id: int, url: str) -> str:
        self.cleanup_expired_inline_tokens(INLINE_TOKEN_TTL_HOURS)
        token = secrets.token_urlsafe(8)
        self.conn.execute(
            "INSERT OR REPLACE INTO inline_tokens (token, user_id, url) VALUES (?, ?, ?)",
            (token, user_id, url),
        )
        self.conn.commit()
        return token

    def get_inline_token_url(self, token: str, ttl_hours: int = INLINE_TOKEN_TTL_HOURS) -> Optional[str]:
        row = self.conn.execute(
            """
            SELECT url
            FROM inline_tokens
            WHERE token = ?
              AND created_at >= DATETIME('now', ?)
            """,
            (token, f"-{ttl_hours} hours"),
        ).fetchone()
        return row[0] if row else None

    def cleanup_expired_inline_tokens(self, ttl_hours: int = INLINE_TOKEN_TTL_HOURS) -> None:
        self.conn.execute(
            "DELETE FROM inline_tokens WHERE created_at < DATETIME('now', ?)",
            (f"-{ttl_hours} hours",),
        )
        self.conn.commit()


def t(lang: str, key: str, **kwargs: object) -> str:
    return TEXTS.get(lang, TEXTS["en"])[key].format(**kwargs)


def load_config() -> Config:
    required = ["BOT_TOKEN", "OWNER_ID", "REQUIRED_CHANNEL"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    cj = max(1, min(32, int(os.getenv("CONCURRENT_JOBS", str(DEFAULT_CONCURRENT_JOBS)))))
    return Config(
        bot_token=os.environ["BOT_TOKEN"],
        owner_id=int(os.environ["OWNER_ID"]),
        required_channel=os.environ["REQUIRED_CHANNEL"],
        ytdlp_cookies_file=os.getenv("YTDLP_COOKIES_FILE") or None,
        concurrent_jobs=cj,
    )


def extract_url(text: str) -> Optional[str]:
    match = URL_RE.search(text or "")
    return match.group(0) if match else None


def resolve_url(url: str) -> str:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=15) as resp:
            return resp.geturl() or url
    except Exception:
        return url


def run_command(cmd: list[str], cwd: Optional[Path] = None) -> tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=str(cwd) if cwd else None)
    return p.returncode, ((p.stdout or "") + "\n" + (p.stderr or "")).strip()


def ffmpeg_convert(source: Path, preset: str) -> Path:
    if preset == "auto":
        return source
    out = source.with_suffix(f".{preset}")
    if preset in {"mp4", "mkv"}:
        cmd = ["ffmpeg", "-y", "-i", str(source), "-c:v", "libx264", "-c:a", "aac", str(out)]
    elif preset == "mp3":
        out = source.with_suffix(".mp3")
        cmd = ["ffmpeg", "-y", "-i", str(source), "-vn", "-c:a", "libmp3lame", "-q:a", "2", str(out)]
    else:
        raise RuntimeError("Unsupported format preset")
    code, output = run_command(cmd)
    if code != 0:
        raise RuntimeError(f"ffmpeg failed: {output[:600]}")
    return out


def find_latest_file(folder: Path) -> Optional[Path]:
    files = [p for p in folder.rglob("*") if p.is_file()]
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def spotify_search_query(url: str) -> Optional[str]:
    if "open.spotify.com/track/" not in url:
        return None
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        title_m = OG_TITLE_RE.search(html)
        desc_m = OG_DESC_RE.search(html)
        title = unescape(title_m.group(1)).strip() if title_m else ""
        desc = unescape(desc_m.group(1)).strip() if desc_m else ""
        query = f"{title} {desc}".strip()
        return f"ytsearch1:{query}" if query else None
    except Exception:
        return None


def should_gallery_first(url: str) -> bool:
    u = url.lower()
    return ("pinterest" in u or "pin.it" in u or "/photo/" in u and "tiktok" in u)


def ytdlp_download(target: str, work_dir: Path, cookies_file: Optional[str], audio_only: bool = False) -> tuple[bool, str]:
    outtmpl = str(work_dir / "%(title).100B-%(id)s.%(ext)s")
    opts = {
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": 15,
        "fragment_retries": 15,
        "socket_timeout": 30,
        "concurrent_fragment_downloads": 8,
        "max_filesize": MAX_FILE_SIZE_MB * 1024 * 1024,
        "http_headers": {"User-Agent": "Mozilla/5.0"},
    }
    if audio_only:
        opts["format"] = "bestaudio/best"
    else:
        opts["format"] = "bv*[height<=1080]+ba/b[height<=1080]/b"
        opts["merge_output_format"] = "mp4"
    if cookies_file:
        opts["cookiefile"] = cookies_file
    try:
        with YoutubeDL(opts) as ydl:
            ydl.extract_info(target, download=True)
        return True, ""
    except Exception as exc:
        return False, str(exc)


def gallery_download(url: str, work_dir: Path) -> tuple[bool, str]:
    code, out = run_command(["gallery-dl", "--directory", str(work_dir), "--write-metadata", url])
    return code == 0, out


def download_media_with_config(url: str, work_dir: Path, cookies_file: Optional[str], force_audio: bool = False) -> Path:
    normalized = resolve_url(url)
    target = normalized

    # Spotify DRM fallback -> search on YouTube by track metadata
    sp_query = spotify_search_query(normalized)
    if sp_query:
        target = sp_query
        force_audio = True

    yt_error = ""
    gd_error = ""

    if should_gallery_first(normalized):
        ok, gd_error = gallery_download(normalized, work_dir)
        if ok:
            f = find_latest_file(work_dir)
            if f:
                return f

    ok, yt_error = ytdlp_download(target, work_dir, cookies_file, audio_only=force_audio)
    if ok:
        f = find_latest_file(work_dir)
        if f:
            return f

    ok, gd_error = gallery_download(normalized, work_dir)
    if ok:
        f = find_latest_file(work_dir)
        if f:
            return f

    hints = []
    if "tiktok" in normalized and not cookies_file:
        hints.append("TikTok may require YTDLP_COOKIES_FILE")
    if "spotify" in normalized:
        hints.append("Spotify direct media is DRM; bot tries YouTube search fallback")
    raise RuntimeError(
        f"Download failed. yt-dlp: {yt_error[:450]} | gallery-dl: {gd_error[:450]}"
        + (" | Hint: " + "; ".join(hints) if hints else "")
    )


def is_supported_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False

    if host in {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}:
        return "/shorts/" in path or host == "youtu.be"
    if host in {"tiktok.com", "www.tiktok.com", "vm.tiktok.com", "vt.tiktok.com"}:
        return True
    if host in {"instagram.com", "www.instagram.com"}:
        return "/reel/" in path or "/reels/" in path
    if host in {"pinterest.com", "www.pinterest.com", "pin.it"}:
        return True
    if host in {"spotify.com", "open.spotify.com"}:
        return True
    if host in {"vk.com", "www.vk.com", "m.vk.com", "vkvideo.ru", "www.vkvideo.ru"}:
        return True
    if host in {"music.yandex.ru", "yandex.ru", "www.yandex.ru"}:
        return "music" in host or "/music" in path
    if host in {"likee.video", "www.likee.video", "l.likee.video", "like-video.com"}:
        return True
    return False


def make_menu(lang: str, is_owner: bool) -> ReplyKeyboardMarkup:
    if lang == "ru":
        rows = [
            [KeyboardButton("📥 Скачать видео/медиа")],
            [KeyboardButton("🎬 Формат"), KeyboardButton("🌐 Язык")],
            [KeyboardButton("ℹ️ Помощь")],
        ]
        if is_owner:
            rows.append([KeyboardButton("📝 Текст под видео"), KeyboardButton("📣 Рассылка")])
            rows.append([KeyboardButton("💬 Текст инлайн"), KeyboardButton("🖼 Фото инлайн")])
    else:
        rows = [
            [KeyboardButton("📥 Download video/media")],
            [KeyboardButton("🎬 Format"), KeyboardButton("🌐 Language")],
            [KeyboardButton("ℹ️ Help")],
        ]
        if is_owner:
            rows.append([KeyboardButton("📝 Media caption"), KeyboardButton("📣 Broadcast")])
            rows.append([KeyboardButton("💬 Inline text"), KeyboardButton("🖼 Inline image")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def make_format_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("AUTO", callback_data="fmt:auto"), InlineKeyboardButton("MP4", callback_data="fmt:mp4")],
            [InlineKeyboardButton("MKV", callback_data="fmt:mkv"), InlineKeyboardButton("MP3", callback_data="fmt:mp3")],
        ]
    )


def make_language_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🇷🇺 Русский", callback_data="lang:ru"), InlineKeyboardButton("🇬🇧 English", callback_data="lang:en")]])


def make_inline_download_buttons(token: str, lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(t(lang, "inline_auto"), callback_data=f"idl:{token}:auto"),
                InlineKeyboardButton(t(lang, "inline_audio"), callback_data=f"idl:{token}:audio"),
            ]
        ]
    )


async def check_subscription(context: ContextTypes.DEFAULT_TYPE, channel: str, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(channel, user_id)
        return member.status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER, ChatMemberStatus.RESTRICTED}
    except Exception:
        return False


async def ensure_subscription_or_notify(context: ContextTypes.DEFAULT_TYPE, cfg: Config, chat_id: int, user_id: int, lang: str) -> bool:
    if not await check_subscription(context, cfg.required_channel, user_id):
        await context.bot.send_message(chat_id, t(lang, "need_sub", channel=cfg.required_channel))
        return False
    return True


async def send_processed_file(context: ContextTypes.DEFAULT_TYPE, chat_id: int, result: Path, caption: str, fmt: str) -> None:
    suffix = result.suffix.lower()
    with result.open("rb") as f:
        if fmt == "auto" and suffix in VIDEO_EXTENSIONS:
            await context.bot.send_video(chat_id=chat_id, video=f, caption=caption, supports_streaming=True)
        elif fmt == "mp3" or suffix in AUDIO_EXTENSIONS:
            await context.bot.send_audio(chat_id=chat_id, audio=f, caption=caption)
        else:
            await context.bot.send_document(chat_id=chat_id, document=f, caption=caption)


def schedule_download(context: ContextTypes.DEFAULT_TYPE, storage: Storage, cfg: Config, chat_id: int, sender_id: int, url: str, lang: str, force_audio: bool = False) -> None:
    tasks: set[asyncio.Task] = context.bot_data["tasks"]
    semaphore: asyncio.Semaphore = context.bot_data["download_semaphore"]

    async def runner() -> None:
        async with semaphore:
            await process_download(context, storage, cfg, chat_id, sender_id, url, lang, force_audio)

    task = asyncio.create_task(runner())
    tasks.add(task)
    task.add_done_callback(lambda tsk: tasks.discard(tsk))


async def process_download(context: ContextTypes.DEFAULT_TYPE, storage: Storage, cfg: Config, chat_id: int, sender_id: int, url: str, lang: str, force_audio: bool = False) -> None:
    status = await context.bot.send_message(chat_id, t(lang, "downloading"))
    try:
        with tempfile.TemporaryDirectory(prefix="tg_dl_") as tmp:
            src = await asyncio.to_thread(download_media_with_config, url, Path(tmp), cfg.ytdlp_cookies_file, force_audio)
            fmt = "mp3" if force_audio else storage.get_format(sender_id)
            out = await asyncio.to_thread(ffmpeg_convert, src, fmt)

            size_mb = out.stat().st_size / (1024 * 1024)
            if size_mb > MAX_FILE_SIZE_MB:
                await status.edit_text(t(lang, "too_big", size=size_mb))
                return

            await status.edit_text(t(lang, "sending", size=size_mb))
            caption = storage.get_setting("media_caption", "").strip() or t(lang, "done")
            await send_processed_file(context, chat_id, out, caption, fmt)
            await status.delete()
    except Exception as exc:
        await status.edit_text(t(lang, "error", err=str(exc)[:700]))


async def process_inline_callback_download(query, context: ContextTypes.DEFAULT_TYPE, storage: Storage, cfg: Config, token: str, mode: str, lang: str) -> None:
    uid = query.from_user.id
    url = storage.get_inline_token_url(token, INLINE_TOKEN_TTL_HOURS)
    if not url:
        await query.answer("Expired", show_alert=True)
        return

    if not await check_subscription(context, cfg.required_channel, uid):
        await query.answer(t(lang, "need_sub", channel=cfg.required_channel), show_alert=True)
        return

    try:
        await query.answer(t(lang, "downloading"), show_alert=False)
    except Exception:
        pass

    force_audio = mode == "audio"
    try:
        with tempfile.TemporaryDirectory(prefix="tg_inline_") as tmp:
            src = await asyncio.to_thread(download_media_with_config, url, Path(tmp), cfg.ytdlp_cookies_file, force_audio)
            fmt = "mp3" if force_audio else "auto"
            out = await asyncio.to_thread(ffmpeg_convert, src, fmt)
            size_mb = out.stat().st_size / (1024 * 1024)
            if size_mb > MAX_FILE_SIZE_MB:
                await query.answer(t(lang, "too_big", size=size_mb), show_alert=True)
                return

            caption = storage.get_setting("media_caption", "").strip() or t(lang, "done")
            try:
                await send_processed_file(context, uid, out, caption, fmt)
                await query.answer(t(lang, "inline_sent_pm"), show_alert=True)
            except Forbidden:
                await query.answer(t(lang, "inline_need_start"), show_alert=True)
                return

            try:
                if query.inline_message_id:
                    # Keep same inline message, only update caption/text status
                    await context.bot.edit_message_text(
                        inline_message_id=query.inline_message_id,
                        text=f"✅ {t(lang, 'inline_sent_pm')}\n🔗 {url}",
                        reply_markup=make_inline_download_buttons(token, lang),
                    )
            except Exception:
                pass
    except Exception as exc:
        await query.answer(t(lang, "error", err=str(exc)[:120]), show_alert=True)


def message_text_or_caption(message) -> str:
    return (message.text or message.caption or "").strip()


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    storage: Storage = context.bot_data["storage"]
    cfg: Config = context.bot_data["cfg"]
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        me = await context.bot.get_me()
        await update.message.reply_text(t("ru", "group_hint", username=me.username or "bot"))
        return

    uid = update.effective_user.id
    storage.upsert_user(uid)
    storage.clear_state(uid)
    if not storage.has_language(uid):
        await update.message.reply_text(TEXTS["ru"]["choose_lang"], reply_markup=make_language_buttons())
        return
    lang = storage.get_language(uid)
    await update.message.reply_text(t(lang, "welcome"), reply_markup=make_menu(lang, uid == cfg.owner_id))


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    storage: Storage = context.bot_data["storage"]
    cfg: Config = context.bot_data["cfg"]
    query = update.callback_query
    if not query or not query.from_user:
        return

    uid = query.from_user.id
    storage.upsert_user(uid)
    lang = storage.get_language(uid)
    payload = query.data or ""

    if payload.startswith("lang:"):
        chosen = payload.split(":", 1)[1]
        if chosen not in {"ru", "en"}:
            await query.answer("Invalid", show_alert=True)
            return
        storage.set_language(uid, chosen)
        await query.edit_message_text(t(chosen, "lang_saved"))
        await context.bot.send_message(uid, t(chosen, "menu"), reply_markup=make_menu(chosen, uid == cfg.owner_id))
        await query.answer()
        return

    if payload.startswith("fmt:"):
        fmt = payload.split(":", 1)[1]
        if fmt not in {"auto", "mp4", "mkv", "mp3"}:
            await query.answer("Invalid", show_alert=True)
            return
        storage.set_format(uid, fmt)
        await query.answer(t(lang, "format_saved", fmt=fmt), show_alert=True)
        return

    if payload.startswith("idl:"):
        parts = payload.split(":")
        if len(parts) != 3:
            await query.answer("Bad data", show_alert=True)
            return
        _, token, mode = parts
        await process_inline_callback_download(query, context, storage, cfg, token, mode, lang)
        return


async def inline_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    storage: Storage = context.bot_data["storage"]
    cfg: Config = context.bot_data["cfg"]
    query = update.inline_query
    if not query or not query.from_user:
        return

    uid = query.from_user.id
    storage.upsert_user(uid)
    lang = storage.get_language(uid)
    q = (query.query or "").strip()

    if not q:
        txt = "Введите ссылку после @бота" if lang == "ru" else "Type URL after @bot"
        await query.answer([
            InlineQueryResultArticle(
                id="empty",
                title=txt,
                input_message_content=InputTextMessageContent(txt),
            )
        ], cache_time=1)
        return

    url = extract_url(q)
    if not url or not is_supported_url(url):
        await query.answer([], cache_time=1, switch_pm_text=t(lang, "unsupported"), switch_pm_parameter="unsupported")
        return

    token = storage.create_inline_token(uid, url)
    inline_text = storage.get_setting("inline_card_text", t(lang, "inline_pick"))
    photo_url = storage.get_setting("inline_card_photo_url", "").strip()
    photo_file_id = storage.get_setting("inline_card_photo_file_id", "").strip()
    kb = make_inline_download_buttons(token, lang)

    if photo_file_id:
        result = InlineQueryResultCachedPhoto(
            id=token,
            photo_file_id=photo_file_id,
            caption=inline_text,
            reply_markup=kb,
        )
    elif photo_url:
        result = InlineQueryResultPhoto(
            id=token,
            photo_url=photo_url,
            thumbnail_url=photo_url,
            caption=inline_text,
            reply_markup=kb,
        )
    else:
        result = InlineQueryResultArticle(
            id=token,
            title=t(lang, "inline_pick"),
            description=url,
            input_message_content=InputTextMessageContent(f"{inline_text}\n🔗 {url}"),
            reply_markup=kb,
        )

    await query.answer([result], cache_time=1)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    storage: Storage = context.bot_data["storage"]
    cfg: Config = context.bot_data["cfg"]

    if not update.effective_chat or not update.effective_user or not update.message:
        return

    msg = update.message
    uid = update.effective_user.id
    storage.upsert_user(uid)
    lang = storage.get_language(uid)
    is_owner = uid == cfg.owner_id

    text = message_text_or_caption(msg)
    state = storage.get_state(uid)

    if text.lower() in {"/start", "start"}:
        storage.clear_state(uid)

    if text.lower() in {"cancel", "отмена"}:
        storage.clear_state(uid)
        await msg.reply_text(t(lang, "cancelled"), reply_markup=make_menu(lang, is_owner))
        return

    # owner states accept text/media
    if state == "awaiting_caption" and is_owner:
        value = message_text_or_caption(msg)
        if not value:
            await msg.reply_text(t(lang, "ask_caption"))
            return
        storage.set_setting("media_caption", value)
        storage.clear_state(uid)
        await msg.reply_text(t(lang, "caption_saved"), reply_markup=make_menu(lang, True))
        return

    if state == "awaiting_inline_text" and is_owner:
        value = message_text_or_caption(msg)
        if not value:
            await msg.reply_text(t(lang, "ask_inline_text"))
            return
        storage.set_setting("inline_card_text", value)
        storage.clear_state(uid)
        await msg.reply_text(t(lang, "inline_text_saved"), reply_markup=make_menu(lang, True))
        return

    if state == "awaiting_inline_image" and is_owner:
        if msg.photo:
            storage.set_setting("inline_card_photo_file_id", msg.photo[-1].file_id)
            storage.set_setting("inline_card_photo_url", "")
        else:
            image_url = extract_url(text)
            if not image_url:
                await msg.reply_text(t(lang, "ask_inline_image"))
                return
            storage.set_setting("inline_card_photo_url", image_url)
            storage.set_setting("inline_card_photo_file_id", "")
        storage.clear_state(uid)
        await msg.reply_text(t(lang, "inline_image_saved"), reply_markup=make_menu(lang, True))
        return

    if state == "awaiting_broadcast" and is_owner:
        storage.clear_state(uid)
        sent = 0
        failed = 0
        for user_id in storage.all_users():
            try:
                await context.bot.copy_message(chat_id=user_id, from_chat_id=msg.chat_id, message_id=msg.message_id)
                sent += 1
            except Exception:
                failed += 1
        await msg.reply_text(t(lang, "broadcast_done", sent=sent, failed=failed), reply_markup=make_menu(lang, True))
        return

    # Chat type specific
    chat_type = update.effective_chat.type
    if chat_type == "private":
        if text == "/start":
            return

        # direct URL always works
        direct_url = extract_url(text)
        if direct_url:
            if not is_supported_url(direct_url):
                await msg.reply_text(t(lang, "unsupported"), reply_markup=make_menu(lang, is_owner))
                return
            if not await ensure_subscription_or_notify(context, cfg, update.effective_chat.id, uid, lang):
                return
            storage.clear_state(uid)
            schedule_download(context, storage, cfg, update.effective_chat.id, uid, direct_url, lang)
            await msg.reply_text(t(lang, "queued"), reply_markup=make_menu(lang, is_owner))
            return

        if text in {"📥 Скачать видео/медиа", "📥 Download video/media"}:
            storage.set_state(uid, "awaiting_url")
            await msg.reply_text(t(lang, "send_link"), reply_markup=ReplyKeyboardRemove())
            return

        if text in {"🎬 Формат", "🎬 Format"}:
            storage.clear_state(uid)
            await msg.reply_text(t(lang, "format_now", fmt=storage.get_format(uid)), reply_markup=make_format_buttons())
            return

        if text in {"🌐 Язык", "🌐 Language"}:
            storage.clear_state(uid)
            await msg.reply_text(t(lang, "choose_lang"), reply_markup=make_language_buttons())
            return

        if text in {"ℹ️ Помощь", "ℹ️ Help"}:
            storage.clear_state(uid)
            await msg.reply_text(t(lang, "help"), reply_markup=make_menu(lang, is_owner))
            return

        if text in {"📝 Текст под видео", "📝 Media caption"}:
            if not is_owner:
                await msg.reply_text(t(lang, "owner_only"), reply_markup=make_menu(lang, is_owner))
            else:
                storage.set_state(uid, "awaiting_caption")
                await msg.reply_text(t(lang, "ask_caption"), reply_markup=ReplyKeyboardRemove())
            return

        if text in {"📣 Рассылка", "📣 Broadcast"}:
            if not is_owner:
                await msg.reply_text(t(lang, "owner_only"), reply_markup=make_menu(lang, is_owner))
            else:
                storage.set_state(uid, "awaiting_broadcast")
                await msg.reply_text(t(lang, "ask_broadcast"), reply_markup=ReplyKeyboardRemove())
            return

        if text in {"💬 Текст инлайн", "💬 Inline text"}:
            if not is_owner:
                await msg.reply_text(t(lang, "owner_only"), reply_markup=make_menu(lang, is_owner))
            else:
                storage.set_state(uid, "awaiting_inline_text")
                await msg.reply_text(t(lang, "ask_inline_text"), reply_markup=ReplyKeyboardRemove())
            return

        if text in {"🖼 Фото инлайн", "🖼 Inline image"}:
            if not is_owner:
                await msg.reply_text(t(lang, "owner_only"), reply_markup=make_menu(lang, is_owner))
            else:
                storage.set_state(uid, "awaiting_inline_image")
                await msg.reply_text(t(lang, "ask_inline_image"), reply_markup=ReplyKeyboardRemove())
            return

        if state == "awaiting_url":
            await msg.reply_text(t(lang, "bad_link"))
            return

        await msg.reply_text(t(lang, "menu"), reply_markup=make_menu(lang, is_owner))
        return

    if chat_type in {"group", "supergroup"}:
        text_only = (msg.text or msg.caption or "").lower()
        me = await context.bot.get_me()
        mention = f"@{(me.username or '').lower()}"
        if mention not in text_only:
            return

        url = extract_url(message_text_or_caption(msg))
        if not url:
            return
        if not is_supported_url(url):
            await msg.reply_text(t(lang, "unsupported"))
            return
        if not await ensure_subscription_or_notify(context, cfg, update.effective_chat.id, uid, lang):
            return
        schedule_download(context, storage, cfg, update.effective_chat.id, uid, url, lang)
        await msg.reply_text(t(lang, "queued"))


def main() -> None:
    cfg = load_config()
    storage = Storage()

    app = Application.builder().token(cfg.bot_token).concurrent_updates(True).build()
    app.bot_data["cfg"] = cfg
    app.bot_data["storage"] = storage
    app.bot_data["download_semaphore"] = asyncio.Semaphore(cfg.concurrent_jobs)
    app.bot_data["tasks"] = set()

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(InlineQueryHandler(inline_query_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.ALL & ~filters.UpdateType.EDITED, message_handler))

    print(f"Bot running. Concurrent jobs: {cfg.concurrent_jobs}. Limit: {MAX_FILE_SIZE_MB}MB")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
