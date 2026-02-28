import asyncio
import os
import re
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ChatMemberStatus
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

TEXTS = {
    "ru": {
        "welcome": (
            "Привет! Я помогу скачать видео/медиа из YouTube, Instagram, TikTok и других платформ.\n\n"
            "Как пользоваться:\n"
            "1) Нажми кнопку «Скачать видео/медиа»\n"
            "2) Отправь ссылку\n"
            "3) Получи файл в выбранном формате\n\n"
            "Для работы нужна подписка на обязательный канал."
        ),
        "choose_lang": "Выберите язык:",
        "lang_saved": "Язык сохранён.",
        "menu": "Меню открыто. Выберите действие:",
        "help": (
            "Возможности:\n"
            "• Скачивание через yt-dlp с fallback на gallery-dl\n"
            "• Отправка файлов до лимитов Telegram Bot API\n"
            "• Выбор формата: auto/mp4/mkv/mp3\n"
            "• Обязательная подписка на канал"
        ),
        "need_sub": "Для использования бота подпишитесь на канал {channel} и повторите запрос.",
        "send_link": "Отправьте ссылку (http/https).",
        "bad_link": "Это не похоже на ссылку. Отправьте корректный URL.",
        "downloading": "Скачиваю медиа, подождите...",
        "sending": "Отправляю файл ({size:.1f}MB)...",
        "too_big": "Файл слишком большой: {size:.1f}MB. Лимит {limit}MB.",
        "done": "Готово.",
        "error": "Ошибка: {err}",
        "format_now": "Текущий формат: {fmt}",
        "format_saved": "Формат сохранён: {fmt}",
        "owner_only": "Эта функция только для владельца.",
        "broadcast_done": "Рассылка завершена. Успешно: {sent}, ошибок: {failed}",
        "ask_caption": "Отправьте новый текст под видео (caption).",
        "caption_saved": "Текст под видео обновлён.",
        "ask_broadcast": "Отправьте текст рассылки одним сообщением.",
        "cancelled": "Действие отменено.",
        "group_hint": "В чате напишите: @{username} <ссылка>",
    },
    "en": {
        "welcome": (
            "Hi! I can download video/media from YouTube, Instagram, TikTok, and many other platforms.\n\n"
            "How to use:\n"
            "1) Tap “Download video/media”\n"
            "2) Send a link\n"
            "3) Receive file in your chosen format\n\n"
            "A required channel subscription is needed before using the bot."
        ),
        "choose_lang": "Choose your language:",
        "lang_saved": "Language saved.",
        "menu": "Menu opened. Choose an action:",
        "help": (
            "Features:\n"
            "• Download via yt-dlp with gallery-dl fallback\n"
            "• File sending up to Telegram Bot API limits\n"
            "• Output formats: auto/mp4/mkv/mp3\n"
            "• Required channel subscription gate"
        ),
        "need_sub": "Please subscribe to {channel} and try again.",
        "send_link": "Send a link (http/https).",
        "bad_link": "This does not look like a valid URL. Send a correct link.",
        "downloading": "Downloading media, please wait...",
        "sending": "Uploading file ({size:.1f}MB)...",
        "too_big": "File is too large: {size:.1f}MB. Limit: {limit}MB.",
        "done": "Done.",
        "error": "Error: {err}",
        "format_now": "Current format: {fmt}",
        "format_saved": "Format saved: {fmt}",
        "owner_only": "This function is owner-only.",
        "broadcast_done": "Broadcast finished. Sent: {sent}, failed: {failed}",
        "ask_caption": "Send new media caption text.",
        "caption_saved": "Media caption updated.",
        "ask_broadcast": "Send broadcast text in one message.",
        "cancelled": "Cancelled.",
        "group_hint": "In group chat send: @{username} <link>",
    },
}


@dataclass
class Config:
    bot_token: str
    owner_id: int
    required_channel: str
    max_file_size_mb: int


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
            "INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('media_caption', '')"
        )
        self.conn.commit()

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
        rows = self.conn.execute("SELECT user_id FROM users").fetchall()
        return [row[0] for row in rows]

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

    def get_media_caption(self) -> str:
        row = self.conn.execute("SELECT value FROM bot_settings WHERE key = 'media_caption'").fetchone()
        return row[0] if row else ""

    def set_media_caption(self, value: str) -> None:
        self.conn.execute(
            "INSERT INTO bot_settings (key, value) VALUES ('media_caption', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (value,),
        )
        self.conn.commit()


def t(lang: str, key: str, **kwargs: object) -> str:
    return TEXTS.get(lang, TEXTS["en"])[key].format(**kwargs)


def load_config() -> Config:
    required = ["BOT_TOKEN", "OWNER_ID", "REQUIRED_CHANNEL"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing env variables: {', '.join(missing)}")

    return Config(
        bot_token=os.environ["BOT_TOKEN"],
        owner_id=int(os.environ["OWNER_ID"]),
        required_channel=os.environ["REQUIRED_CHANNEL"],
        max_file_size_mb=int(os.getenv("MAX_FILE_SIZE_MB", "2048")),
    )


def run_command(cmd: list[str], cwd: Optional[Path] = None) -> tuple[int, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(cwd) if cwd else None)
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    return proc.returncode, output.strip()


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
        raise RuntimeError(f"ffmpeg failed: {output[:700]}")
    return out


def find_latest_file(folder: Path) -> Optional[Path]:
    files = [p for p in folder.rglob("*") if p.is_file()]
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def download_media(url: str, work_dir: Path) -> Path:
    output_template = str(work_dir / "%(title).100B-%(id)s.%(ext)s")
    yt_cmd = [
        "yt-dlp",
        "--no-playlist",
        "-f",
        "bv*+ba/b",
        "--merge-output-format",
        "mp4",
        "--restrict-filenames",
        "--no-warnings",
        "-o",
        output_template,
        url,
    ]
    code, yt_log = run_command(yt_cmd)
    if code == 0:
        file_path = find_latest_file(work_dir)
        if file_path:
            return file_path

    gd_cmd = ["gallery-dl", "--directory", str(work_dir), "--write-metadata", url]
    gcode, gd_log = run_command(gd_cmd)
    if gcode == 0:
        file_path = find_latest_file(work_dir)
        if file_path:
            return file_path

    raise RuntimeError(f"Download failed. yt-dlp: {yt_log[:350]} | gallery-dl: {gd_log[:350]}")


def extract_url(text: str) -> Optional[str]:
    match = URL_RE.search(text)
    return match.group(0) if match else None


def make_menu(lang: str, is_owner: bool) -> ReplyKeyboardMarkup:
    if lang == "ru":
        rows = [
            [KeyboardButton("📥 Скачать видео/медиа")],
            [KeyboardButton("🎬 Формат"), KeyboardButton("🌐 Язык")],
            [KeyboardButton("ℹ️ Помощь")],
        ]
        if is_owner:
            rows.append([KeyboardButton("📝 Текст под видео"), KeyboardButton("📣 Рассылка")])
    else:
        rows = [
            [KeyboardButton("📥 Download video/media")],
            [KeyboardButton("🎬 Format"), KeyboardButton("🌐 Language")],
            [KeyboardButton("ℹ️ Help")],
        ]
        if is_owner:
            rows.append([KeyboardButton("📝 Media caption"), KeyboardButton("📣 Broadcast")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def make_format_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("AUTO", callback_data="fmt:auto"), InlineKeyboardButton("MP4", callback_data="fmt:mp4")],
            [InlineKeyboardButton("MKV", callback_data="fmt:mkv"), InlineKeyboardButton("MP3", callback_data="fmt:mp3")],
        ]
    )


def make_language_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🇷🇺 Русский", callback_data="lang:ru"), InlineKeyboardButton("🇬🇧 English", callback_data="lang:en")]]
    )


async def check_subscription(context: ContextTypes.DEFAULT_TYPE, channel: str, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(channel, user_id)
        return member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
            ChatMemberStatus.RESTRICTED,
        }
    except Exception:
        return False


async def handle_download(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    storage: Storage,
    cfg: Config,
    chat_id: int,
    sender_id: int,
    url: str,
    lang: str,
) -> None:
    subscribed = await check_subscription(context, cfg.required_channel, sender_id)
    if not subscribed:
        await context.bot.send_message(chat_id, t(lang, "need_sub", channel=cfg.required_channel))
        return

    status = await context.bot.send_message(chat_id, t(lang, "downloading"))
    try:
        with tempfile.TemporaryDirectory(prefix="tg_dl_") as tmp:
            tmp_dir = Path(tmp)
            source = await asyncio.to_thread(download_media, url, tmp_dir)
            fmt = storage.get_format(sender_id)
            result = await asyncio.to_thread(ffmpeg_convert, source, fmt)
            size_mb = result.stat().st_size / (1024 * 1024)
            if size_mb > cfg.max_file_size_mb:
                await status.edit_text(t(lang, "too_big", size=size_mb, limit=cfg.max_file_size_mb))
                return

            await status.edit_text(t(lang, "sending", size=size_mb))
            extra_caption = storage.get_media_caption().strip() or t(lang, "done")
            with result.open("rb") as file_obj:
                await context.bot.send_document(chat_id=chat_id, document=file_obj, caption=extra_caption)
            await status.delete()
    except Exception as exc:
        await status.edit_text(t(lang, "error", err=str(exc)[:700]))


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    storage: Storage = context.bot_data["storage"]
    cfg: Config = context.bot_data["cfg"]

    if not update.effective_chat or not update.effective_user:
        return

    if update.effective_chat.type != "private":
        me = await context.bot.get_me()
        await update.message.reply_text(t("ru", "group_hint", username=me.username or "bot"))
        return

    uid = update.effective_user.id
    storage.upsert_user(uid)
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
    payload = query.data or ""

    if payload.startswith("lang:"):
        lang = payload.split(":", 1)[1]
        if lang not in {"ru", "en"}:
            await query.answer("Invalid language", show_alert=True)
            return
        storage.set_language(uid, lang)
        await query.edit_message_text(t(lang, "lang_saved"))
        await context.bot.send_message(uid, t(lang, "menu"), reply_markup=make_menu(lang, uid == cfg.owner_id))
        await query.answer()
        return

    if payload.startswith("fmt:"):
        fmt = payload.split(":", 1)[1]
        if fmt not in {"auto", "mp4", "mkv", "mp3"}:
            await query.answer("Invalid format", show_alert=True)
            return
        storage.set_format(uid, fmt)
        lang = storage.get_language(uid)
        await query.answer(t(lang, "format_saved", fmt=fmt), show_alert=True)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    storage: Storage = context.bot_data["storage"]
    cfg: Config = context.bot_data["cfg"]

    if not update.effective_chat or not update.effective_user or not update.message:
        return

    text = (update.message.text or "").strip()
    if not text:
        return

    uid = update.effective_user.id
    storage.upsert_user(uid)
    lang = storage.get_language(uid)
    is_owner = uid == cfg.owner_id

    if update.effective_chat.type == "private":
        if text == "/start":
            return

        state = storage.get_state(uid)
        if text.lower() in {"cancel", "отмена"}:
            storage.clear_state(uid)
            await update.message.reply_text(t(lang, "cancelled"), reply_markup=make_menu(lang, is_owner))
            return

        if state == "awaiting_url":
            url = extract_url(text)
            if not url:
                await update.message.reply_text(t(lang, "bad_link"))
                return
            storage.clear_state(uid)
            await handle_download(update, context, storage, cfg, update.effective_chat.id, uid, url, lang)
            await update.message.reply_text(t(lang, "menu"), reply_markup=make_menu(lang, is_owner))
            return

        if state == "awaiting_caption" and is_owner:
            storage.set_media_caption(text)
            storage.clear_state(uid)
            await update.message.reply_text(t(lang, "caption_saved"), reply_markup=make_menu(lang, True))
            return

        if state == "awaiting_broadcast" and is_owner:
            storage.clear_state(uid)
            users = storage.all_users()
            sent, failed = 0, 0
            for user in users:
                try:
                    await context.bot.send_message(user, text)
                    sent += 1
                except Exception:
                    failed += 1
            await update.message.reply_text(
                t(lang, "broadcast_done", sent=sent, failed=failed), reply_markup=make_menu(lang, True)
            )
            return

        if text in {"📥 Скачать видео/медиа", "📥 Download video/media"}:
            storage.set_state(uid, "awaiting_url")
            await update.message.reply_text(t(lang, "send_link"), reply_markup=ReplyKeyboardRemove())
        elif text in {"🎬 Формат", "🎬 Format"}:
            await update.message.reply_text(t(lang, "format_now", fmt=storage.get_format(uid)), reply_markup=make_format_buttons())
        elif text in {"🌐 Язык", "🌐 Language"}:
            await update.message.reply_text(t(lang, "choose_lang"), reply_markup=make_language_buttons())
        elif text in {"ℹ️ Помощь", "ℹ️ Help"}:
            await update.message.reply_text(t(lang, "help"), reply_markup=make_menu(lang, is_owner))
        elif text in {"📝 Текст под видео", "📝 Media caption"}:
            if not is_owner:
                await update.message.reply_text(t(lang, "owner_only"), reply_markup=make_menu(lang, is_owner))
            else:
                storage.set_state(uid, "awaiting_caption")
                await update.message.reply_text(t(lang, "ask_caption"), reply_markup=ReplyKeyboardRemove())
        elif text in {"📣 Рассылка", "📣 Broadcast"}:
            if not is_owner:
                await update.message.reply_text(t(lang, "owner_only"), reply_markup=make_menu(lang, is_owner))
            else:
                storage.set_state(uid, "awaiting_broadcast")
                await update.message.reply_text(t(lang, "ask_broadcast"), reply_markup=ReplyKeyboardRemove())
        else:
            await update.message.reply_text(t(lang, "menu"), reply_markup=make_menu(lang, is_owner))
        return

    if update.effective_chat.type in {"group", "supergroup"}:
        me = await context.bot.get_me()
        mention = f"@{(me.username or '').lower()}"
        lower = text.lower()
        if not mention or mention not in lower:
            return
        url = extract_url(text)
        if not url:
            return
        await handle_download(update, context, storage, cfg, update.effective_chat.id, uid, url, lang)


def main() -> None:
    cfg = load_config()
    storage = Storage()

    app = Application.builder().token(cfg.bot_token).build()
    app.bot_data["cfg"] = cfg
    app.bot_data["storage"] = storage

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    print("Bot is running with Telegram Bot API...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
