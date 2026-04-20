from ffmpeg_seek import ffmpeg_cut
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from thumbnails import Thumbnail
import re
import config
import asyncio
import copy
import html
import random
import logging
import mimetypes
import os
import signal
import time
import uuid
from collections.abc import Awaitable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


# --- Импортируем только типы и вспомогательные функции, инициализацию перенесём внутрь main() ---
from aiogram import types
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.enums import ChatType, ParseMode
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    FSInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from motor.motor_asyncio import AsyncIOMotorClient
from pytgcalls.types import AudioQuality, GroupCallConfig, MediaStream, StreamEnded, VideoQuality
from pytgcalls.exceptions import NoActiveGroupCall, NotInCallError, YtDlpError
from pytgcalls import filters as tg_filters
from telethon.sessions import StringSession
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

# --- MongoDB Cache ---

from mongo_cache_migration.mongo_cache import MongoCache

# --- Serialization helpers ---
def serialize_media_info(media_info):
    # Handles aiogram types and dicts, recursively, avoiding circular refs
    seen = set()
    def _serialize(obj):
        if id(obj) in seen:
            return None
        seen.add(id(obj))
        if isinstance(obj, dict):
            return {k: _serialize(v) for k, v in obj.items() if isinstance(k, (str, int, float, bool, type(None)))}
        elif isinstance(obj, list):
            return [_serialize(v) for v in obj]
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        elif hasattr(obj, "file_id"):
            return {
                "file_id": getattr(obj, "file_id", None),
                "file_unique_id": getattr(obj, "file_unique_id", None),
                "duration": getattr(obj, "duration", None),
                "performer": getattr(obj, "performer", None),
                "title": getattr(obj, "title", None),
                "file_name": getattr(obj, "file_name", None),
                "mime_type": getattr(obj, "mime_type", None),
                "file_size": getattr(obj, "file_size", None),
            }
        else:
            return str(obj)
    return _serialize(media_info)

# For yt-dlp info dicts, remove unserializable objects
def serialize_info(info):
    if not isinstance(info, dict):
        return str(info)
    result = {}
    for k, v in info.items():
        if isinstance(v, (str, int, float, bool, type(None), list, dict)):
            result[k] = v
        # skip objects like FFmpegMergerPP
    return result


def getenv(key, default=None):
    return os.getenv(key, default)

# === CONFIG ===
TG_AUDIO_FILESIZE_LIMIT = int(getenv("TG_AUDIO_FILESIZE_LIMIT", 2 * 1024 ** 3))
TG_VIDEO_FILESIZE_LIMIT = int(getenv("TG_VIDEO_FILESIZE_LIMIT", 2 * 1024 ** 3))

START_IMG_URL = [
    "https://image2url.com/r2/default/images/1769269338835-d5ce1f25-55d6-45fc-b9ad-c04ae647827e.jpg",
    "https://image2url.com/r2/default/images/1769269355185-77c5d002-ce9a-47ce-aba1-d1b033e60472.jpg",
    "https://image2url.com/r2/default/images/1769269377267-3084111d-b3fe-4e5e-be58-418b26f25c4d.jpg",
    "https://image2url.com/r2/default/images/1769269399286-a06b9ba6-3f29-47a5-9a32-9f0c3e0a905c.jpg",
    "https://image2url.com/r2/default/images/1769269443873-5d739aec-a837-45be-aa83-409ae4259c5e.jpg",
    "https://image2url.com/r2/default/images/1769269553883-e7fa9182-2d84-4961-a2bf-4ae63e810b1e.jpg"
]

PING_IMG_URL = getenv(
    "PING_IMG_URL",
    "https://image2url.com/r2/default/images/1768792821746-ad62ab76-1fdc-45d7-8b5e-a5343577d6bb.jpg"
)
API_TOKEN = os.getenv("API_TOKEN", "8275714086:AAFsZnOE2e2oXtykXtM5Xfewy8rwuUNMPy8")
API_ID = int(os.getenv("API_ID", "27638882"))
API_HASH = os.getenv("API_HASH", "f745cdd5ddb46cf841d6990048f52935")
TELETHON_STRING_SESSION = os.getenv(
    "TELETHON_STRING_SESSION",
    "1ApWapzMBu4bvMCVrj5iIKCL3MqsJ_08h39fffJvXScUVacmHaD4Q5Cl9Z7alqsgXHvWh0p1p1QhP2B5OXsQS8MTO2Wr6G4YJOAtyhkwqJTWE8gLkJTv7_k-edQDPsz_B94Qk-8DAFbsVIZ6Z1MvEEv5Kw34X9DEitcVV_rUR5TYmQ0Pod30uj_TZoCFzyqClbnSKQ5GB8Rin5euJF4IhJvzqLZ7gFGVRNVMvQFgsOVHgTfv6Mz2HyNi0VEsOhKtQJRPB8LELgzxHDWZQs2iL9_QAtFjPZfQBLR5J5RAgeBBWTWucCoynazkSbm27LwEf9yoPFr61GfTdRtML4zTGHhWcd_9TMjI="
)
MONGO_URL = os.getenv(
    "MONGO_URL",
    "mongodb+srv://sarkis05082008_db_user:JhF9FZ4OllNEpWVz@cluster0.mo7yftz.mongodb.net/?appName=Cluster0",
)

POLLING_RESTART_DELAY = 2
SIGINT_CONFIRM_WINDOW = 3
LOCK_FILE_PATH = Path(__file__).with_name("bot.instance.lock")
BASE_DIR = Path(__file__).resolve().parent
MEDIA_DOWNLOAD_TIMEOUT = 900
START_GRACE_SECONDS = 1.0
STREAM_END_DEBOUNCE_SECONDS = 0.75
MONGO_CONNECT_TIMEOUT_MS = 5000

# --- Инициализация MongoCache ---

# --- Только MongoDB, без локального кэша и путей ---
MONGO_URL = os.getenv(
    "MONGO_URL",
    "mongodb+srv://sarkis05082008_db_user:JhF9FZ4OllNEpWVz@cluster0.mo7yftz.mongodb.net/?appName=Cluster0",
)
mongo_cache: MongoCache | None = None

AUDIO_EXTENSIONS = {
    ".aac",
    ".aiff",
    ".alac",
    ".amr",
    ".flac",
    ".m4a",
    ".mid",
    ".midi",
    ".mp3",
    ".oga",
    ".ogg",
    ".opus",
    ".wav",
    ".wma",
}
VIDEO_EXTENSIONS = {
    ".3gp",
    ".avi",
    ".flv",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ts",
    ".webm",
    ".wmv",
}



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
start_time = time.time()

# === State Storage Classes ===
def default_chat_state(chat_id: int) -> dict[str, Any]:
    return {
        "chat_id": chat_id,
        "queue": [],
        "current": None,
        "paused": False,
    }

class BaseStateStorage:
    async def load(self, chat_id: int) -> dict[str, Any]:
        raise NotImplementedError

    async def save(self, chat_id: int, state: dict[str, Any]) -> None:
        raise NotImplementedError

    async def reset_all(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None

class MemoryStateStorage(BaseStateStorage):
    def __init__(self) -> None:
        self._data: dict[int, dict[str, Any]] = {}

    async def load(self, chat_id: int) -> dict[str, Any]:
        return copy.deepcopy(self._data.get(chat_id, default_chat_state(chat_id)))

    async def save(self, chat_id: int, state: dict[str, Any]) -> None:
        self._data[chat_id] = copy.deepcopy(state)

    async def reset_all(self) -> None:
        self._data.clear()

class MongoStateStorage(BaseStateStorage):
    def __init__(self, client: AsyncIOMotorClient, collection) -> None:
        self._client = client
        self._collection = collection

    async def load(self, chat_id: int) -> dict[str, Any]:
        document = await self._collection.find_one({"chat_id": chat_id}, {"_id": 0})
        if not document:
            return default_chat_state(chat_id)
        state = default_chat_state(chat_id)
        state.update(document)
        state["queue"] = document.get("queue", [])
        return state

    async def save(self, chat_id: int, state: dict[str, Any]) -> None:
        # Очистка элементов очереди и current перед сохранением
        def clean_item(d):
            allowed = {
                "id", "kind", "title", "stream_source", "cleanup_path", "duration", "requested_by", "requested_by_id", "original_query", "source_label", "thumb_path", "views", "channel", "uploader"
            }
            return {k: v for k, v in d.items() if k in allowed and not isinstance(v, (bytes, bytearray))}

        payload = copy.deepcopy(state)
        # Очистить очередь
        if "queue" in payload and isinstance(payload["queue"], list):
            payload["queue"] = [clean_item(item) for item in payload["queue"] if isinstance(item, dict)]
        # Очистить current
        if "current" in payload and isinstance(payload["current"], dict):
            payload["current"] = clean_item(payload["current"])
        payload["chat_id"] = chat_id
        await self._collection.replace_one({"chat_id": chat_id}, payload, upsert=True)

    async def reset_all(self) -> None:
        await self._collection.delete_many({})

    async def close(self) -> None:
        self._client.close()


queue_storage: BaseStateStorage = MemoryStateStorage()


@dataclass
class ChatRuntime:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    just_started_until: float = 0.0
    stream_end_task: asyncio.Task | None = None


chat_runtimes: dict[int, ChatRuntime] = {}


def get_runtime(chat_id: int) -> ChatRuntime:
    runtime = chat_runtimes.get(chat_id)
    if runtime is None:
        runtime = ChatRuntime()
        chat_runtimes[chat_id] = runtime
    return runtime


class SingleInstanceLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def acquire(self):
        self.handle = self.path.open("a+", encoding="utf-8")
        self.handle.seek(0)
        self.handle.write("0")
        self.handle.flush()
        self.handle.seek(0)

        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            existing_pid = self.read_pid()
            self.handle.close()
            self.handle = None
            return existing_pid

        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"{os.getpid()}\n")
        self.handle.flush()
        return None

    def read_pid(self):
        try:
            return int(self.path.read_text(encoding="utf-8").strip())
        except (FileNotFoundError, ValueError):
            return None

    def release(self):
        if not self.handle:
            return

        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def acquire_single_instance_lock():
    lock = SingleInstanceLock(LOCK_FILE_PATH)
    existing_pid = lock.acquire()
    if existing_pid is not None:
        pid_hint = f" PID {existing_pid}" if existing_pid else ""
        raise SystemExit(
            f"[ERROR] Бот уже запущен в другом процессе.{pid_hint} "
            "Остановите предыдущий экземпляр и оставьте только один polling."
        )
    return lock


def build_start_text() -> str:
    return (
        "<b>Voice Chat Music Bot</b>\n"
        "\n"
        "Я умею:\n"
        "• играть музыку и видео в голосовых чатах\n"
        "• принимать reply на Telegram-файлы\n"
        "• брать ссылки и названия треков с YouTube\n"
        "• скачивать песни командой /song\n"
        "• держать очередь и переключать треки автоматически\n"
        "\n"
        "Команды в группах: /play, /vplay, /song, /ping, /pause, /resume, /skip, /stop, /end\n"
        "\n"
        "Чтобы всё работало, добавь бота в чат, а аккаунт из String Session тоже должен быть в этом чате и иметь доступ к голосовому чату."
    )


async def get_bot_username() -> str:
    if bot is None:
        raise RuntimeError("Bot is not initialized")
    if getattr(bot, "me", None) is not None and getattr(bot.me, "username", None):
        return bot.me.username
    me = await bot.get_me()
    if getattr(me, "username", None):
        return me.username
    raise RuntimeError("Unable to determine bot username")


async def get_group_invite_url() -> str:
    username = await get_bot_username()
    return f"https://t.me/{username}?startgroup=true"


async def build_start_markup():
    kb = InlineKeyboardBuilder()
    kb.button(text="Добавить бота в чат", url=await get_group_invite_url())
    return kb.as_markup()


async def send_start_message(message: types.Message):
    text = build_start_text()
    markup = await build_start_markup()
    img_url = random.choice(START_IMG_URL)

    if message.chat.type == ChatType.PRIVATE:
        await message.answer_photo(img_url, caption=text, reply_markup=markup)
        return

    await message.reply_photo(
        img_url,
        caption="<b>Напиши мне в личные сообщения, чтобы увидеть описание и кнопку добавления.</b>",
        reply_markup=markup,
    )






def html_escape(value: Any) -> str:
    return html.escape(str(value))


def format_duration(duration: int | None) -> str:
    if not duration:
        return "?"
    minutes, seconds = divmod(int(duration), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def safe_user_name(message: types.Message) -> str:
    if message.from_user:
        return message.from_user.full_name
    return "Unknown"


def is_group_chat(message: types.Message) -> bool:
    return message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}


def command_args(command: CommandObject | None) -> str:
    return (command.args or "").strip() if command else ""


def looks_like_url(value: str) -> bool:
    parsed = urlparse(value)
    return bool(parsed.scheme and parsed.netloc)




def cleanup_item(item: dict[str, Any] | None) -> None:
    if not item:
        return


def extract_extension(name: str | None, mime_type: str | None, fallback: str) -> str:
    if name:
        suffix = Path(name).suffix.lower()
        if suffix:
            return suffix
    if mime_type:
        guessed = mimetypes.guess_extension(mime_type)
        if guessed:
            return guessed
    return fallback


def describe_item(item: dict[str, Any]) -> str:
    title = html_escape(item.get("title", "Без названия"))
    kind = "видео" if item.get("kind") == "video" else "аудио"
    duration = format_duration(item.get("duration"))
    return f"{title} [{kind}, {duration}]"


def build_track_item(
    *,
    kind: str,
    title: str,
    stream_source: str,
    cleanup_path: str | None,
    requested_by: str,
    requested_by_id: int | None,
    duration: int | None = None,
    original_query: str | None = None,
    source_label: str | None = None,
) -> dict[str, Any]:
    return {
        "id": uuid.uuid4().hex,
        "kind": kind,
        "title": title,
        "stream_source": stream_source,
        "cleanup_path": cleanup_path,
        "duration": duration,
        "requested_by": requested_by,
        "requested_by_id": requested_by_id,
        "original_query": original_query,
        "source_label": source_label,
    }


def audio_stream_from_source(source: str) -> MediaStream:
    return MediaStream(
        source,
        audio_parameters=AudioQuality.HIGH,
        video_flags=MediaStream.Flags.IGNORE,
        audio_flags=MediaStream.Flags.REQUIRED,
    )


def video_stream_from_source(source: str) -> MediaStream:
    return MediaStream(
        source,
        audio_parameters=AudioQuality.HIGH,
        video_parameters=VideoQuality.HD_720p,
        audio_flags=MediaStream.Flags.AUTO_DETECT,
        video_flags=MediaStream.Flags.REQUIRED,
    )


def build_stream(item: dict[str, Any]) -> MediaStream:
    if item["kind"] == "video":
        return video_stream_from_source(item["stream_source"])
    return audio_stream_from_source(item["stream_source"])


async def create_storage() -> BaseStateStorage:
    if not MONGO_URL:
        logger.warning("MONGO_URL не задан. Очередь будет храниться только в памяти.")
        return MemoryStateStorage()

    try:
        client = AsyncIOMotorClient(MONGO_URL, serverSelectionTimeoutMS=MONGO_CONNECT_TIMEOUT_MS)
        await client.admin.command("ping")
        logger.info("MongoDB подключён. Очередь хранится в базе.")
        return MongoStateStorage(client, client["muzza"]["queue"])
    except Exception:
        logger.exception("MongoDB недоступен. Переключаю очередь на память.")
        return MemoryStateStorage()


async def set_state(chat_id: int, state: dict[str, Any]) -> None:
    await queue_storage.save(chat_id, state)


async def get_state(chat_id: int) -> dict[str, Any]:
    return await queue_storage.load(chat_id)


async def resolve_query_metadata(query: str) -> dict[str, Any]:
    target = query if looks_like_url(query) else f"ytsearch1:{query}"

    def worker() -> dict[str, Any]:
        options = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "skip_download": True,
            "default_search": "ytsearch",
        }
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(target, download=False)
            if info and "entries" in info:
                entries = [entry for entry in info.get("entries", []) if entry]
                if not entries:
                    raise DownloadError("Ничего не найдено")
                info = entries[0]
            if not info:
                raise DownloadError("Ничего не найдено")
            return info

    return await asyncio.to_thread(worker)


async def build_query_track(query: str, force_video: bool, message: types.Message) -> dict[str, Any]:
    requested_by = safe_user_name(message)
    requested_by_id = message.from_user.id if message.from_user else None

    metadata: dict[str, Any] | None = None
    source = query
    title = query
    duration = None
    source_label = "URL" if looks_like_url(query) else "YouTube search"

    try:
        metadata = await resolve_query_metadata(query)
    except DownloadError:
        if not looks_like_url(query):
            raise ValueError("Ничего не найдено по запросу.")
    except Exception as exc:
        if not looks_like_url(query):
            raise ValueError(f"Не удалось найти трек: {exc}") from exc


    if metadata:
        # --- Выбрать лучший поток для PyTgCalls ---
        best_url = None
        best_height = -1
        best_abr = -1
        if "formats" in metadata:
            formats = metadata["formats"]
            if force_video:
                for fmt in formats:
                    if (
                        fmt.get("vcodec") != "none"
                        and fmt.get("acodec") != "none"
                        and fmt.get("height")
                        and fmt.get("url")
                    ):
                        height = fmt.get("height", 0)
                        if height > best_height:
                            best_height = height
                            best_url = fmt["url"]
                if not best_url:
                    for fmt in formats:
                        if fmt.get("vcodec") != "none" and fmt.get("url"):
                            best_url = fmt["url"]
                            break
            else:
                for fmt in formats:
                    if (
                        fmt.get("acodec") != "none"
                        and fmt.get("vcodec") == "none"
                        and fmt.get("abr")
                        and fmt.get("url")
                    ):
                        abr = fmt.get("abr", 0)
                        if abr > best_abr:
                            best_abr = abr
                            best_url = fmt["url"]
                if not best_url:
                    for fmt in formats:
                        if fmt.get("acodec") != "none" and fmt.get("url"):
                            best_url = fmt["url"]
                            break
        source = best_url or metadata.get("url") or metadata.get("webpage_url") or metadata.get("original_url") or query
        title = metadata.get("title") or query
        duration = metadata.get("duration")
        video_id = metadata.get("id") if (metadata.get("extractor") and metadata["extractor"].lower() == "youtube") else None
        source_label = metadata.get("extractor") if metadata.get("extractor") else source_label
        views = metadata.get("view_count")

    kind = "video" if force_video else "audio"
    item = build_track_item(
        kind=kind,
        title=title,
        stream_source=source,
        cleanup_path=None,
        duration=duration,
        requested_by=requested_by,
        requested_by_id=requested_by_id,
        original_query=query,
        source_label=source_label,
    )
    if video_id:
        item["id"] = video_id
    if metadata and metadata.get("view_count") is not None:
        item["views"] = metadata["view_count"]
    return item


def detect_message_media(message: types.Message) -> dict[str, Any] | None:
    def get_file_id(obj):
        # aiogram v3: all file-like objects have .file_id
        return getattr(obj, "file_id", None)

    if message.audio:
        audio = message.audio
        performer = audio.performer or ""
        title = audio.title or audio.file_name or "Audio"
        joined = " - ".join(part for part in [performer, title] if part).strip()
        return {
            "kind": "audio",
            "file": audio,
            "file_id": get_file_id(audio),
            "title": joined or "Audio",
            "duration": audio.duration,
            "extension": extract_extension(audio.file_name, audio.mime_type, ".mp3"),
        }

    if message.voice:
        voice = message.voice
        return {
            "kind": "audio",
            "file": voice,
            "file_id": get_file_id(voice),
            "title": "Voice message",
            "duration": voice.duration,
            "extension": ".ogg",
        }

    if message.video:
        video = message.video
        return {
            "kind": "video",
            "file": video,
            "file_id": get_file_id(video),
            "title": video.file_name or "Video",
            "duration": video.duration,
            "extension": extract_extension(video.file_name, video.mime_type, ".mp4"),
        }

    if message.video_note:
        note = message.video_note
        return {
            "kind": "video",
            "file": note,
            "file_id": get_file_id(note),
            "title": "Video note",
            "duration": note.duration,
            "extension": ".mp4",
        }

    if message.animation:
        animation = message.animation
        return {
            "kind": "video",
            "file": animation,
            "file_id": get_file_id(animation),
            "title": animation.file_name or "Animation",
            "duration": animation.duration,
            "extension": extract_extension(animation.file_name, animation.mime_type, ".mp4"),
        }

    if message.document:
        document = message.document
        mime = (document.mime_type or "").lower()
        filename = (document.file_name or "").lower()
        ext = Path(filename).suffix.lower()
        if mime.startswith("audio/") or ext in AUDIO_EXTENSIONS:
            return {
                "kind": "audio",
                "file": document,
                "file_id": get_file_id(document),
                "title": document.file_name or "Audio file",
                "duration": None,
                "extension": extract_extension(document.file_name, document.mime_type, ".mp3"),
            }
        if mime.startswith("video/") or ext in VIDEO_EXTENSIONS:
            return {
                "kind": "video",
                "file": document,
                "file_id": get_file_id(document),
                "title": document.file_name or "Video file",
                "duration": None,
                "extension": extract_extension(document.file_name, document.mime_type, ".mp4"),
            }

    return None


async def download_telegram_media(media_info: dict[str, Any]) -> Path:
    extension = media_info.get("extension") or (".mp4" if media_info["kind"] == "video" else ".mp3")
    cache_key = f"tgmedia:{media_info['file_id']}:{extension}"
    if mongo_cache is None:
        raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
    cached = await mongo_cache.get_file(cache_key)
    if cached:
        data, meta = cached
        return data

    # Скачиваем файл через ассистент-бота (userbot/Telethon)
    from telethon.tl.types import InputDocument
    import tempfile
    file_id = media_info.get("file_id")
    if not file_id:
        raise ValueError("media_info missing file_id for Telethon download")
    # Получаем сообщение с файлом через userbot
    # Для этого нужен chat_id и message_id, которые должны быть в media_info
    chat_id = media_info.get("chat_id")
    message_id = media_info.get("message_id")
    if not chat_id or not message_id:
        raise ValueError("media_info missing chat_id or message_id for Telethon download")
    msg = await userbot.get_messages(chat_id, ids=message_id)
    with tempfile.NamedTemporaryFile(suffix=extension, delete=False) as tmp_file:
        await userbot.download_media(msg, file=tmp_file.name)
        tmp_file.flush()
        tmp_path = tmp_file.name
    with open(tmp_path, "rb") as f:
        data = f.read()
    os.remove(tmp_path)
    await mongo_cache.save_file(cache_key, data, {"media_info": serialize_media_info(media_info)})
    return data


async def build_reply_track(message: types.Message, media_message: types.Message, force_video: bool) -> dict[str, Any]:
    media_info = detect_message_media(media_message)
    if not media_info:
        raise ValueError("В reply нет поддерживаемого аудио/видео файла.")

    # Patch: Add chat_id and message_id for Telethon download
    if hasattr(media_message, "chat") and hasattr(media_message.chat, "id"):
        media_info["chat_id"] = media_message.chat.id
    elif hasattr(media_message, "chat_id"):
        media_info["chat_id"] = media_message.chat_id
    if hasattr(media_message, "message_id"):
        media_info["message_id"] = media_message.message_id

    if force_video and media_info["kind"] != "video":
        raise ValueError("Для /vplay нужен видеофайл или ссылка на видео.")

    local_path = await download_telegram_media(media_info)
    requested_by = safe_user_name(message)
    requested_by_id = message.from_user.id if message.from_user else None

    # Сохраняем файл в кеш по ключу song:{file_id} для поддержки seek
    file_id = media_info.get("file_id")
    if file_id and os.path.exists(str(local_path)):
        with open(str(local_path), "rb") as f:
            data = f.read()
        cache_key = f"song:{file_id}"
        if mongo_cache is not None:
            await mongo_cache.save_file(cache_key, data, {"media_info": serialize_media_info(media_info)})

    return build_track_item(
        kind=media_info["kind"],
        title=media_info["title"],
        stream_source=str(local_path),
        cleanup_path=str(local_path),
        duration=media_info.get("duration"),
        requested_by=requested_by,
        requested_by_id=requested_by_id,
        original_query=file_id,  # теперь original_query = file_id для Telegram
        source_label="telegram",
    )


async def resolve_track_request(
    message: types.Message,
    command: CommandObject | None,
    *,
    force_video: bool,
) -> dict[str, Any]:
    if message.reply_to_message:
        reply_media = detect_message_media(message.reply_to_message)
        if reply_media:
            return await build_reply_track(message, message.reply_to_message, force_video)

    current_media = detect_message_media(message)
    if current_media:
        return await build_reply_track(message, message, force_video)

    query = command_args(command)
    if not query:
        if force_video:
            raise ValueError(
                "Использование команды:\n/vplay [ссылка или название]\nили reply на видео."
            )
        raise ValueError(
            "Использование команды:\n/play [ссылка или название]\nили reply на аудио или видео."
        )

    return await build_query_track(query, force_video, message)


async def leave_voice_chat(chat_id: int) -> None:
    with suppress(NoActiveGroupCall, NotInCallError):
        await voice_client.leave_call(chat_id)


async def start_playback_locked(
    chat_id: int,
    state: dict[str, Any],
    runtime: ChatRuntime,
    item: dict[str, Any],
) -> dict[str, Any]:
    if mongo_cache is None:
        raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")

    stream = build_stream(item)
    print(f"[DEBUG] stream for play: {stream}")
    await voice_client.play(chat_id, stream=stream, config=GroupCallConfig(auto_start=True))
    state["current"] = item
    state["paused"] = False
    state["current_pos"] = 0  # позиция в секундах
    state["current_start_time"] = time.time()  # время старта
    runtime.just_started_until = time.monotonic() + START_GRACE_SECONDS
    if runtime.stream_end_task and not runtime.stream_end_task.done():
        runtime.stream_end_task.cancel()
    runtime.stream_end_task = None
    await set_state(chat_id, state)


    # --- Всегда отправлять info-карточку: YouTube — кастомная, файл — DEFAULT_THUMB ---
    from aiogram.types import FSInputFile
    title = item.get("title", "Без названия")
    performer = item.get("requested_by", "Unknown")
    duration = item.get("duration")
    duration_str = format_duration(duration)
    caption = (
        f"<b>{html_escape(title)}</b>\n"
        f"<b>Длительность:</b> {duration_str}\n"
        f"<b>Заказал:</b> {html_escape(performer)}"
    )
    thumb_file = None
    thumb_url = None
    extractor = (item.get("source_label") or "").lower()
    is_youtube = extractor == "youtube"
    video_id = None
    if is_youtube:
        if item.get("id") and isinstance(item["id"], str) and len(item["id"]) == 11:
            video_id = item["id"]
        elif item.get("original_query"):
            m = re.search(r"(?:v=|youtu\\.be/)([\w-]{11})", item["original_query"])
            if m:
                video_id = m.group(1)
            elif item.get("original_query").startswith("http") and "youtube.com" in item["original_query"]:
                parts = item["original_query"].split("v=")
                if len(parts) > 1:
                    video_id = parts[1][:11]
        if video_id and isinstance(video_id, str) and len(video_id) == 11:
            cache_key = f"thumb:{video_id}"
            cached = await mongo_cache.get_file(cache_key)
            if cached:
                data, meta = cached
                from aiogram.types import BufferedInputFile
                thumb_file = BufferedInputFile(data, filename=f"{video_id}.jpg")
            else:
                try:
                    # Передаём максимум информации для генерации обложки
                    song_obj = type("Song", (), {
                        "id": video_id,
                        "title": item.get("title", "Unknown Song"),
                        "views": item.get("views", 0),
                        "channel": item.get("channel") or item.get("uploader"),
                        "uploader": item.get("uploader"),
                        "duration": item.get("duration") or item.get("duration_string"),
                        "thumbnail": item.get("thumbnail")
                    })()
                    thumb_result = await Thumbnail().generate(song_obj)
                    from aiogram.types import BufferedInputFile
                    import os
                    if thumb_result:
                        if isinstance(thumb_result, bytes):
                            data = thumb_result
                            await mongo_cache.save_file(cache_key, data, {"video_id": video_id})
                            thumb_file = BufferedInputFile(data, filename=f"{video_id}.jpg")
                        elif isinstance(thumb_result, str) and os.path.exists(thumb_result):
                            with open(thumb_result, "rb") as fp:
                                data = fp.read()
                            await mongo_cache.save_file(cache_key, data, {"video_id": video_id})
                            thumb_file = BufferedInputFile(data, filename=f"{video_id}.jpg")
                        else:
                            thumb_file = None
                    else:
                        thumb_file = None
                except Exception:
                    thumb_file = None
        elif video_id:
            logger.warning(f"Некорректный YouTube video_id: {video_id!r}. Использую DEFAULT_THUMB.")
    if not thumb_file:
        logger.warning(f"[Thumb Debug] Не удалось получить кастомную обложку для item={item.get('title', 'no-title')}, video_id={video_id}, использую DEFAULT_THUMB")
        thumb_url = getattr(config, "DEFAULT_THUMB", None)

    def build_progress_bar(current, total, width=12):
        if not current or not total:
            return "--:-- ━{}━ --:--".format("━"*width)
        filled = int(width * current / total)
        bar = "━" * filled + "⬤" + "━" * (width - filled - 1)
        return f"{format_duration(current)} {bar} {format_duration(total)}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=build_progress_bar(0, duration or 1), callback_data="progress")],
        [
            InlineKeyboardButton(text="ᐖ", callback_data="resume"),
            InlineKeyboardButton(text="II", callback_data="pause"),
            InlineKeyboardButton(text="⟳", callback_data="restart"),
            InlineKeyboardButton(text="▷▷", callback_data="skip"),
            InlineKeyboardButton(text="▢", callback_data="end"),
        ],
        [InlineKeyboardButton(text="add me in your group", url=await get_group_invite_url())],
        [InlineKeyboardButton(text="close", callback_data="close")],
    ])

    sent_msg = None
    if thumb_file:
        sent_msg = await bot.send_photo(chat_id, thumb_file, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    elif thumb_url:
        sent_msg = await bot.send_photo(chat_id, thumb_url, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)

    async def update_progress_bar(msg, chat_id, duration):
        try:
            while True:
                await asyncio.sleep(5)
                chat_state = await get_state(chat_id)
                if chat_state.get("paused") or not chat_state.get("current"):
                    continue
                start_time = chat_state.get("current_start_time", time.time())
                current_pos = int(time.time() - start_time)
                if current_pos >= duration:
                    break
                progress_text = build_progress_bar(current_pos, duration)
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=progress_text, callback_data="progress")],
                    [
                        InlineKeyboardButton(text="ᐖ", callback_data="resume"),
                        InlineKeyboardButton(text="II", callback_data="pause"),
                        InlineKeyboardButton(text="⟳", callback_data="restart"),
                        InlineKeyboardButton(text="▷▷", callback_data="skip"),
                        InlineKeyboardButton(text="▢", callback_data="end"),
                    ],
                    [InlineKeyboardButton(text="add me in your group", url=await get_group_invite_url())],
                    [InlineKeyboardButton(text="close", callback_data="close")],
                ])
                try:
                    await msg.edit_reply_markup(reply_markup=keyboard)
                except Exception:
                    pass
        except Exception:
            pass

    if sent_msg and duration:
        asyncio.create_task(update_progress_bar(sent_msg, chat_id, duration))
    return item

# === CALLBACK HANDLERS ===
async def progress_callback(query: CallbackQuery, state=None):
    chat_id = query.message.chat.id
    chat_state = await get_state(chat_id)
    duration = 1
    if chat_state.get("current"):
        duration = chat_state["current"].get("duration") or 1
    if chat_state.get("paused"):
        current_pos = chat_state.get("current_pos", 0)
    else:
        start_time = chat_state.get("current_start_time", time.time())
        current_pos = int(time.time() - start_time)
    # Ограничить current_pos и percent
    if current_pos > duration:
        current_pos = duration
    percent = int(100 * current_pos / duration) if duration else 0
    def build_progress_bar(current, total, width=12):
        if not current or not total:
            return "--:-- ━{}━ --:--".format("━"*width)
        filled = int(width * current / total)
        if filled > width - 1:
            filled = width - 1
        bar = "━" * filled + "⬤" + "━" * (width - filled - 1)
        return f"{format_duration(current)} {bar} {format_duration(total)}"
    progress_text = build_progress_bar(current_pos, duration)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=progress_text, callback_data="progress")],
        [
            InlineKeyboardButton(text="ᐖ", callback_data="resume"),
            InlineKeyboardButton(text="II", callback_data="pause"),
            InlineKeyboardButton(text="⟳", callback_data="restart"),
            InlineKeyboardButton(text="▷▷", callback_data="skip"),
            InlineKeyboardButton(text="▢", callback_data="end"),
        ],
        [InlineKeyboardButton(text="add me in your group", url=await get_group_invite_url())],
        [InlineKeyboardButton(text="close", callback_data="close")],
    ])
    try:
        await query.message.edit_reply_markup(reply_markup=keyboard)
    except Exception:
        pass
    await query.answer(f"Воспроизведено: {percent}%", show_alert=False)

async def resume_callback(query: CallbackQuery):
    chat_id = query.message.chat.id
    chat_state = await get_state(chat_id)
    if chat_state.get("paused"):
        chat_state["paused"] = False
        paused_pos = chat_state.get("current_pos", 0)
        chat_state["current_start_time"] = time.time() - paused_pos
        await set_state(chat_id, chat_state)
        try:
            await voice_client.resume(chat_id)
        except Exception:
            pass
        await query.answer("Воспроизведение возобновлено")
    else:
        await query.answer("Уже играет")

async def pause_callback(query: CallbackQuery):
    chat_id = query.message.chat.id
    chat_state = await get_state(chat_id)
    if not chat_state.get("paused"):
        start_time = chat_state.get("current_start_time", time.time())
        chat_state["current_pos"] = int(time.time() - start_time)
        chat_state["paused"] = True
        await set_state(chat_id, chat_state)
        try:
            await voice_client.pause(chat_id)
        except Exception:
            pass
        await query.answer("Пауза")
    else:
        await query.answer("Уже на паузе")

async def restart_callback(query: CallbackQuery):
    chat_id = query.message.chat.id
    chat_state = await get_state(chat_id)
    if not chat_state.get("current"):
        await query.answer("Нет трека для рестарта")
        return
    # Сбросить только таймер и состояние
    chat_state["current_pos"] = 0
    chat_state["current_start_time"] = time.time()
    chat_state["paused"] = False
    await set_state(chat_id, chat_state)
    # Обновить только клавиатуру (прогресс-бар)
    duration = chat_state["current"].get("duration") or 1
    def build_progress_bar(current, total, width=12):
        if not current or not total:
            return "--:-- ━{}━ --:--".format("━"*width)
        filled = int(width * current / total)
        if filled > width - 1:
            filled = width - 1
        bar = "━" * filled + "⬤" + "━" * (width - filled - 1)
        return f"{format_duration(current)} {bar} {format_duration(total)}"
    progress_text = build_progress_bar(0, duration)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=progress_text, callback_data="progress")],
        [
            InlineKeyboardButton(text="ᐖ", callback_data="resume"),
            InlineKeyboardButton(text="II", callback_data="pause"),
            InlineKeyboardButton(text="⟳", callback_data="restart"),
            InlineKeyboardButton(text="▷▷", callback_data="skip"),
            InlineKeyboardButton(text="▢", callback_data="end"),
        ],
        [InlineKeyboardButton(text="add me in your group", url=await get_group_invite_url())],
        [InlineKeyboardButton(text="close", callback_data="close")],
    ])
    try:
        await query.message.edit_reply_markup(reply_markup=keyboard)
    except Exception:
        pass
    await query.answer("Воспроизведение сначала")

async def skip_callback(query: CallbackQuery):
    chat_id = query.message.chat.id
    await handle_skip(chat_id)
    await query.answer("Следующий трек")

async def end_callback(query: CallbackQuery):
    chat_id = query.message.chat.id
    await handle_end(chat_id)
    await query.answer("Воспроизведение завершено")

async def close_callback(query: CallbackQuery):
    await query.message.delete()
    await query.answer()


async def skip_failed_items_locked(
    chat_id: int,
    state: dict[str, Any],
    runtime: ChatRuntime,
    *,
    announcer: Awaitable[None] | None = None,
) -> dict[str, Any] | None:
    if announcer is not None:
        await announcer

    current = state.get("current")
    if current:
        cleanup_item(current)
    state["current"] = None
    state["paused"] = False

    while state["queue"]:
        candidate = state["queue"].pop(0)
        try:
            started = await start_playback_locked(chat_id, state, runtime, candidate)
            return started
        except Exception as exc:
            cleanup_item(candidate)
            logger.exception("Не удалось запустить элемент очереди в %s", chat_id)
            await bot.send_message(
                chat_id,
                "<b>Пропускаю элемент очереди из-за ошибки.</b>\n"
                f"{html_escape(exc)}",
            )

    await set_state(chat_id, state)
    await leave_voice_chat(chat_id)
    return None


async def enqueue_or_start(chat_id: int, item: dict[str, Any]) -> tuple[str, int | None]:
    runtime = get_runtime(chat_id)
    async with runtime.lock:
        state = await get_state(chat_id)

        # Очищаем item от больших полей перед сохранением в очередь/state
        def clean_item(d):
            allowed = {
                "id", "kind", "title", "stream_source", "cleanup_path", "duration", "requested_by", "requested_by_id", "original_query", "source_label", "thumb_path", "views", "channel", "uploader"
            }
            return {k: v for k, v in d.items() if k in allowed and not isinstance(v, (bytes, bytearray))}

        safe_item = clean_item(item)

        if state.get("current"):
            state["queue"].append(safe_item)
            await set_state(chat_id, state)
            return "queued", len(state["queue"])

        try:
            await start_playback_locked(chat_id, state, runtime, safe_item)
        except Exception:
            cleanup_item(safe_item)
            raise
        return "started", None


async def handle_skip(chat_id: int) -> dict[str, Any] | None:
    runtime = get_runtime(chat_id)
    async with runtime.lock:
        state = await get_state(chat_id)
        current = state.get("current")

        if not current and not state["queue"]:
            return None

        if state["queue"]:
            previous = current
            candidate = state["queue"].pop(0)
            state["current"] = None
            state["paused"] = False
            try:
                started = await start_playback_locked(chat_id, state, runtime, candidate)
                cleanup_item(previous)
                return started
            except Exception:
                cleanup_item(candidate)
                if previous:
                    cleanup_item(previous)
                logger.exception("Не удалось переключить трек в чате %s", chat_id)
                state["current"] = None
                state["paused"] = False
                await set_state(chat_id, state)
                await leave_voice_chat(chat_id)
                raise

        current_item = current
        state["current"] = None
        state["paused"] = False
        await set_state(chat_id, state)
        await leave_voice_chat(chat_id)
        cleanup_item(current_item)
        return None


async def handle_end(chat_id: int) -> None:
    runtime = get_runtime(chat_id)
    async with runtime.lock:
        state = await get_state(chat_id)
        current_item = state.get("current")
        for item in state.get("queue", []):
            cleanup_item(item)
        state["queue"] = []
        state["current"] = None
        state["paused"] = False
        runtime.just_started_until = time.monotonic() + START_GRACE_SECONDS
        if runtime.stream_end_task and not runtime.stream_end_task.done():
            runtime.stream_end_task.cancel()
        runtime.stream_end_task = None
        await set_state(chat_id, state)
        await leave_voice_chat(chat_id)
        cleanup_item(current_item)


async def download_song(query: str) -> tuple[Path, dict[str, Any]]:
    # Ключ кеша — строка запроса
    cache_key = f"song:{query.strip().lower()}"
    if mongo_cache is None:
        raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
    cached = await mongo_cache.get_file(cache_key)
    if cached:
        data, meta = cached
        return data, meta.get("info", {})

    def worker() -> tuple[bytes, dict[str, Any]]:
        options = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "default_search": "ytsearch",
            "format": "bestaudio/best",
            "outtmpl": "%(__id__)s.%(ext)s",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ],
        }
        with YoutubeDL(options) as ydl:
            target = query if looks_like_url(query) else f"ytsearch1:{query}"
            info = ydl.extract_info(target, download=True)
            if info and "entries" in info:
                entries = [entry for entry in info.get("entries", []) if entry]
                if not entries:
                    raise DownloadError("Ничего не найдено")
                info = entries[0]
            if not info:
                raise DownloadError("Ничего не найдено")
            # Найти mp3-файл
            mp3_path = None
            for f in os.listdir('.'):
                if f.endswith('.mp3'):
                    mp3_path = f
                    break
            if not mp3_path:
                raise FileNotFoundError("yt-dlp завершился без MP3 файла")
            with open(mp3_path, 'rb') as fp:
                data = fp.read()
            os.remove(mp3_path)
            return data, info

    data, info = await asyncio.to_thread(worker)
    if mongo_cache is None:
        raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
    await mongo_cache.save_file(cache_key, data, {"info": info})
    return data, info


async def register_bot_commands() -> None:
    private_commands = [
        BotCommand(command="start", description="Описание бота"),
        BotCommand(command="song", description="Скачать песню из YouTube"),
        BotCommand(command="ping", description="Uptime бота"),
    ]
    group_commands = [
        BotCommand(command="play", description="Играть аудио или reply на файл"),
        BotCommand(command="vplay", description="Играть видео или reply на файл"),
        BotCommand(command="song", description="Скачать песню из YouTube"),
        BotCommand(command="pause", description="Пауза"),
        BotCommand(command="resume", description="Продолжить"),
        BotCommand(command="skip", description="Следующий элемент очереди"),
        BotCommand(command="stop", description="Остановить и выйти"),
        BotCommand(command="end", description="Остановить и выйти"),
        BotCommand(command="ping", description="Uptime бота"),
    ]
    await bot.set_my_commands(private_commands, scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands(group_commands, scope=BotCommandScopeAllGroupChats())


def playback_error_text(exc: Exception) -> str:
    return html_escape(str(exc) or exc.__class__.__name__)


def require_group(message: types.Message) -> bool:
    return is_group_chat(message)



# --- Обработчик stream_end будет зарегистрирован после инициализации voice_client ---
async def on_stream_end(_, update: StreamEnded):
    chat_id = update.chat_id
    runtime = get_runtime(chat_id)
    if time.monotonic() < runtime.just_started_until:
        return
    if runtime.stream_end_task and not runtime.stream_end_task.done():
        return

    async def worker() -> None:
        try:
            await asyncio.sleep(STREAM_END_DEBOUNCE_SECONDS)
            async with runtime.lock:
                if time.monotonic() < runtime.just_started_until:
                    return
                state = await get_state(chat_id)
                if not state.get("current"):
                    return
                await skip_failed_items_locked(chat_id, state, runtime)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка при автоматическом переходе к следующему элементу")
        finally:
            runtime.stream_end_task = None

    runtime.stream_end_task = asyncio.create_task(worker())



async def start_cmd(message: types.Message):
    await send_start_message(message)



async def ping_cmd(message: types.Message):
    uptime = int(time.time() - start_time)
    hours, remainder = divmod(uptime, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Количество чатов (по состоянию на память)
    chat_count = len(chat_runtimes)

    # Количество активных проигрываний
    active_playbacks = 0
    for chat_id, runtime in chat_runtimes.items():
        state = await get_state(chat_id)
        if state.get("current"):
            active_playbacks += 1



    # Размер кеша (GridFS) — сумма всех файлов
    if mongo_cache is None:
        raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
    bucket_name = mongo_cache.fs._bucket_name if hasattr(mongo_cache.fs, '_bucket_name') else 'cache'
    files_collection = mongo_cache.db[f"{bucket_name}.files"]
    total_size = 0
    async for doc in files_collection.find({}, {"length": 1}):
        total_size += doc.get("length", 0)
    cache_size_mb = total_size / (1024 * 1024)

    text = (
        f"<b>Бот работает:</b> {hours}ч {minutes}м {seconds}с\n"
        f"<b>Чатов:</b> {chat_count}\n"
        f"<b>Активных проигрываний:</b> {active_playbacks}\n"
        f"<b>Размер кеша:</b> {cache_size_mb:.2f} MB"
    )

    await message.answer_photo(
        PING_IMG_URL,
        caption=text,
        parse_mode=ParseMode.HTML
    )



async def play_cmd(message: types.Message, command: CommandObject):
    if not require_group(message):
        await message.reply("<b>/play работает только в группах и супергруппах.</b>")
        return

    try:
        item = await resolve_track_request(message, command, force_video=False)
        # Если это YouTube URL, получаем info через yt-dlp
        info = None
        file_path = None
        if looks_like_url(item.get("original_query", "")):
            def worker():
                with YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
                    # Скачиваем файл (download=True) и получаем info
                    return ydl.extract_info(item["original_query"], download=True)
            info = await asyncio.to_thread(worker)
            # Обновляем item
            if info:
                item["title"] = info.get("title")
                item["uploader"] = info.get("uploader")
                item["duration"] = info.get("duration")
                item["thumbnail"] = info.get("thumbnail")
                item["views"] = info.get("view_count")
                # Сохраняем исходный файл в mongo_cache для seek (YouTube)
                if info.get("requested_downloads"):
                    # yt-dlp >=2023.07.06: requested_downloads[0]["filepath"]
                    file_path = info["requested_downloads"][0]["filepath"]
                else:
                    # yt-dlp <2023: _filename
                    file_path = info.get("_filename")
                if file_path and os.path.exists(file_path):
                    with open(file_path, "rb") as f:
                        data = f.read()
                    cache_key = f"song:{item.get('original_query', '').strip().lower()}"
                    if mongo_cache is not None:
                        await mongo_cache.save_file(
                            cache_key, data,
                            {"info": serialize_info(info), "original_query": item.get('original_query', '')}
                        )

        # Сохраняем исходный файл в mongo_cache для seek для других источников (например, Telegram-файлы)
        # file_path может быть в item["file_path"] или item["audio_file_path"]
        generic_file_path = item.get("file_path") or item.get("audio_file_path")
        if generic_file_path and os.path.exists(generic_file_path):
            with open(generic_file_path, "rb") as f:
                data = f.read()
            cache_key = f"song:{item.get('original_query', '').strip().lower()}"
            if mongo_cache is not None:
                await mongo_cache.save_file(
                    cache_key, data,
                    {"info": serialize_info(info) if info else {}, "original_query": item.get('original_query', '')}
                )
        # Передаём все поля item в Thumbnail.generate
        if item.get("id"):
            try:
                thumb_path = await Thumbnail().generate(type("Song", (), item)())
            except Exception:
                thumb_path = None
            if thumb_path and os.path.exists(thumb_path):
                item["thumb_path"] = thumb_path
        status, position = await enqueue_or_start(message.chat.id, item)
    except ValueError as exc:
        from config import DEFAULT_THUMB
        await message.answer_photo(
            DEFAULT_THUMB,
            caption=f"{exc}",
            parse_mode=ParseMode.HTML
        )
        return
    except Exception as exc:
        logger.exception("Ошибка в /play")
        await message.reply(
            "<b>Не удалось запустить воспроизведение.</b>\n"
            f"{playback_error_text(exc)}"
        )
        return

    if status == "queued":
        await message.reply(
            "<b>Добавлено в очередь.</b>\n"
            f"Позиция: {position}"
        )
        return



async def vplay_cmd(message: types.Message, command: CommandObject):
    if not require_group(message):
        await message.reply("<b>/vplay работает только в группах и супергруппах.</b>")
        return

    try:
        item = await resolve_track_request(message, command, force_video=True)
        # Если это YouTube URL, получаем info через yt-dlp
        info = None
        if looks_like_url(item.get("original_query", "")):
            def worker():
                with YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
                    return ydl.extract_info(item["original_query"], download=False)
            info = await asyncio.to_thread(worker)
            # Обновляем item
            if info:
                item["title"] = info.get("title")
                item["uploader"] = info.get("uploader")
                item["duration"] = info.get("duration")
                item["thumbnail"] = info.get("thumbnail")
                item["views"] = info.get("view_count")
        # Передаём все поля item в Thumbnail.generate
        if item.get("id"):
            try:
                thumb_path = await Thumbnail().generate(type("Song", (), item)())
            except Exception:
                thumb_path = None
            if thumb_path and os.path.exists(thumb_path):
                item["thumb_path"] = thumb_path
        status, position = await enqueue_or_start(message.chat.id, item)
    except ValueError as exc:
        from config import DEFAULT_THUMB
        await message.answer_photo(
            DEFAULT_THUMB,
            caption=f"{exc}",
            parse_mode=ParseMode.HTML
        )
        return
    except Exception as exc:
        logger.exception("Ошибка в /vplay")
        await message.reply(
            "<b>Не удалось запустить видео.</b>\n"
            f"{playback_error_text(exc)}"
        )
        return

    if status == "queued":
        await message.reply(
            "<b>Видео добавлено в очередь.</b>\n"
            f"Позиция: {position}"
        )
        return



async def song_cmd(message: types.Message, command: CommandObject):
    query = command_args(command)
    if not query:
        await message.reply(
            "Please provide a song name or YouTube URL.\n\n"
            "Example: `/song Believer` or `/song https://www.youtube.com/watch?v=7wtfhZwyrcc`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    await message.reply("<b>Скачиваю трек, подожди...</b>")
    file_path: Path | None = None
    try:
        file_path, info = await download_song(query)
        # Проверяем, что file_path — это путь к файлу, а не байты
        audio = None
        if isinstance(file_path, (str, Path)) and os.path.exists(str(file_path)):
            # Явно указываем filename для FSInputFile
            audio = FSInputFile(str(file_path), filename="song.mp3")
        elif isinstance(file_path, bytes):
            from aiogram.types import BufferedInputFile
            audio = BufferedInputFile(file_path, filename="song.mp3")
        else:
            raise ValueError("Некорректный тип file_path для аудиофайла")

        title = info.get("title") or (file_path.stem if hasattr(file_path, "stem") else "Song")
        performer = info.get("uploader") or info.get("artist")
        duration = info.get("duration")
        # Генерируем кастомную обложку через thumbnails.py
        thumb_path = None
        video_id = None
        # Пытаемся извлечь video_id из info
        if info.get("id"):
            video_id = info["id"]
        elif info.get("original_url"):
            import re
            m = re.search(r"(?:v=|youtu\.be/)([\w-]{11})", info["original_url"])
            if m:
                video_id = m.group(1)
        if video_id:
            try:
                thumb_path = await Thumbnail().generate(type("Song", (), {"id": video_id})())
            except Exception:
                thumb_path = None
        if thumb_path and os.path.exists(thumb_path):
            thumb_file = FSInputFile(thumb_path, filename="thumb.jpg")
        else:
            thumb_file = None
        await message.reply_audio(
            audio=audio,
            title=title,
            performer=performer,
            duration=duration,
            caption=f"<b>Готово:</b> {html_escape(title)}",
            thumbnail=thumb_file if thumb_file else None,
        )
    except Exception as exc:
        logger.exception("Ошибка в /song")
        # Ограничиваем длину сообщения об ошибке, чтобы избежать TelegramBadRequest
        error_text = playback_error_text(exc)
        if len(error_text) > 3500:
            error_text = error_text[:3500] + "..."
        await message.reply(
            "<b>Не удалось скачать трек.</b>\n"
            f"{error_text}"
        )



async def pause_cmd(message: types.Message):
    if not require_group(message):
        await message.reply("<b>/pause работает только в группах и супергруппах.</b>")
        return

    runtime = get_runtime(message.chat.id)
    async with runtime.lock:
        state = await get_state(message.chat.id)
        if not state.get("current"):
            await message.reply("<b>Сейчас ничего не играет.</b>")
            return
        try:
            await voice_client.pause(message.chat.id)
            state["paused"] = True
            await set_state(message.chat.id, state)
        except Exception as exc:
            logger.exception("Ошибка в /pause")
            await message.reply(
                "<b>Не удалось поставить на паузу.</b>\n"
                f"{playback_error_text(exc)}"
            )
            return

    await message.reply("<b>Пауза.</b>")



async def resume_cmd(message: types.Message):
    if not require_group(message):
        await message.reply("<b>/resume работает только в группах и супергруппах.</b>")
        return

    runtime = get_runtime(message.chat.id)
    async with runtime.lock:
        state = await get_state(message.chat.id)
        if not state.get("current"):
            await message.reply("<b>Сейчас ничего не играет.</b>")
            return
        try:
            await voice_client.resume(message.chat.id)
            state["paused"] = False
            await set_state(message.chat.id, state)
        except Exception as exc:
            logger.exception("Ошибка в /resume")
            await message.reply(
                "<b>Не удалось снять паузу.</b>\n"
                f"{playback_error_text(exc)}"
            )
            return

    await message.reply("<b>Воспроизведение продолжено.</b>")



async def skip_cmd(message: types.Message):
    if not require_group(message):
        await message.reply("<b>/skip работает только в группах и супергруппах.</b>")
        return

    try:
        next_item = await handle_skip(message.chat.id)
    except Exception as exc:
        await message.reply(
            "<b>Не удалось переключить очередь.</b>\n"
            f"{playback_error_text(exc)}"
        )
        return

    if next_item:
        await message.reply(f"<b>Следующий элемент:</b> {describe_item(next_item)}")
        return

    await message.reply("<b>Очередь закончилась, голосовой чат остановлен.</b>")



async def end_cmd(message: types.Message):
    if not require_group(message):
        await message.reply("<b>/end и /stop работают только в группах и супергруппах.</b>")
        return

    await handle_end(message.chat.id)
    await message.reply("<b>Воспроизведение остановлено, очередь очищена.</b>")



async def unknown_command_cmd(message: types.Message):
    command_token = (message.text or message.caption or "").split(maxsplit=1)[0].lower()
    command_name = command_token[1:].split("@", maxsplit=1)[0]

    if command_name == "start":
        logger.warning("Fallback matched /start for update that missed CommandStart: %s", command_token)
        await send_start_message(message)
        return

    await message.reply(
        "<b>Команда не распознана.</b>\n"
        "Доступно: /start, /play, /vplay, /song, /ping, /pause, /resume, /skip, /stop, /end"
    )


def install_signal_handlers(loop, shutdown_event):
    last_sigint_at = 0.0

    def request_shutdown(signum, _frame):
        nonlocal last_sigint_at
        signal_name = signal.Signals(signum).name

        if signal_name == "SIGINT":
            now = time.monotonic()
            if now - last_sigint_at > SIGINT_CONFIRM_WINDOW:
                last_sigint_at = now
                print(
                    f"\n[WARN] Получен сигнал SIGINT. "
                    f"Нажмите Ctrl+C ещё раз в течение {SIGINT_CONFIRM_WINDOW} сек, чтобы остановить бота."
                )
                return

        print(f"\n[INFO] Получен сигнал {signal_name}. Останавливаю бота...")
        loop.call_soon_threadsafe(shutdown_event.set)

    for signal_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, signal_name, None)
        if sig is not None:
            signal.signal(sig, request_shutdown)


async def stop_polling_on_shutdown(shutdown_event: asyncio.Event):
    await shutdown_event.wait()
    with suppress(RuntimeError):
        await dp.stop_polling()


async def run_polling_forever(shutdown_event: asyncio.Event):
    while not shutdown_event.is_set():
        try:
            await dp.start_polling(
                bot,
                handle_signals=False,
                close_bot_session=False,
            )
        except Exception:
            logger.exception("Polling завершился с ошибкой.")
        else:
            if not shutdown_event.is_set():
                logger.warning("Polling остановился без запроса на завершение.")

        if shutdown_event.is_set():
            break

        print(f"[WARN] Polling остановился. Перезапуск через {POLLING_RESTART_DELAY} сек...")
        await asyncio.sleep(POLLING_RESTART_DELAY)


async def shutdown():
    for runtime in chat_runtimes.values():
        if runtime.stream_end_task and not runtime.stream_end_task.done():
            runtime.stream_end_task.cancel()
            with suppress(asyncio.CancelledError):
                await runtime.stream_end_task

    for chat_id in list(chat_runtimes):
        with suppress(Exception):
            await leave_voice_chat(chat_id)

    with suppress(Exception):
        await queue_storage.reset_all()
    with suppress(Exception):
        await userbot.disconnect()
    with suppress(Exception):
        await bot.session.close()
    with suppress(Exception):
        await queue_storage.close()
    # instance_lock больше не используется


async def prepare_environment():
    global queue_storage

    queue_storage = await create_storage()
    await queue_storage.reset_all()
    await register_bot_commands()

async def seek_cmd(message: types.Message, command: CommandObject):
    seconds = 0
    try:
        seconds = int(command_args(command))
    except Exception:
        await message.reply("<b>Использование: /seek &lt;секунды&gt;\nНапример: /seek 120</b>")
        return
    if seconds <= 0:
        await message.reply("<b>Укажите положительное число секунд.</b>")
        return
    chat_id = message.chat.id
    runtime = get_runtime(chat_id)
    async with runtime.lock:
        state = await get_state(chat_id)
        current = state.get("current")
        if not current:
            await message.reply("<b>Сейчас ничего не играет.</b>")
            return
        # Получаем исходный аудиофайл из кеша
        cache_key = f"song:{current.get('original_query', '').strip().lower()}"
        if mongo_cache is None:
            raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
        cached = await mongo_cache.get_file(cache_key)
        # Если не найдено по original_query, пробуем по file_id (для Telegram)
        if not cached and current.get('original_query') and len(str(current.get('original_query'))) > 0:
            alt_cache_key = f"song:{current.get('original_query')}"
            cached = await mongo_cache.get_file(alt_cache_key)
        if not cached and current.get('file_id'):
            alt_cache_key = f"song:{current.get('file_id')}"
            cached = await mongo_cache.get_file(alt_cache_key)
        if not cached:
            await message.reply(
                "<b>Исходный файл не найден в кеше.\n\nПеремотка доступна только для треков, которые были добавлены через /play, /vplay или reply на файл, либо уже были закешированы. Попробуйте добавить трек заново.</b>"
            )
            return
        data, meta = cached
        # Обрезаем через ffmpeg
        new_data = ffmpeg_cut(data, seconds)
        if not new_data:
            await message.reply("<b>Ошибка ffmpeg при перемотке.</b>")
            return
        # Сохраняем новый файл во временный кеш
        seek_key = f"seek:{cache_key}:{seconds}"
        if mongo_cache is None:
            raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
        await mongo_cache.save_file(seek_key, new_data, {"seek": seconds, "original": cache_key})
        # Обновляем state и перезапускаем поток
        current["seek_offset"] = seconds
        state["current"] = current
        await set_state(chat_id, state)
        await leave_voice_chat(chat_id)
        # Запускаем воспроизведение с seek
        await start_playback_locked(chat_id, state, runtime, current)
        await message.reply(f"<b>Перемотка вперёд на {seconds} секунд.</b>")

async def seekback_cmd(message: types.Message, command: CommandObject):
    seconds = 0
    try:
        seconds = int(command_args(command))
    except Exception:
        await message.reply("<b>Использование: /seekback &lt;секунды&gt;\nНапример: /seekback 10</b>")
        return
    if seconds <= 0:
        await message.reply("<b>Укажите положительное число секунд.</b>")
        return
    chat_id = message.chat.id
    runtime = get_runtime(chat_id)
    async with runtime.lock:
        state = await get_state(chat_id)
        current = state.get("current")
        if not current:
            await message.reply("<b>Сейчас ничего не играет.</b>")
            return
        # Получаем текущий offset
        cur_offset = current.get("seek_offset", 0)
        new_offset = max(0, cur_offset - seconds)
        # Получаем исходный аудиофайл из кеша
        cache_key = f"song:{current.get('original_query', '').strip().lower()}"
        if mongo_cache is None:
            raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
        cached = await mongo_cache.get_file(cache_key)
        # Если не найдено по original_query, пробуем по file_id (для Telegram)
        if not cached and current.get('original_query') and len(str(current.get('original_query'))) > 0:
            alt_cache_key = f"song:{current.get('original_query')}"
            cached = await mongo_cache.get_file(alt_cache_key)
        if not cached and current.get('file_id'):
            alt_cache_key = f"song:{current.get('file_id')}"
            cached = await mongo_cache.get_file(alt_cache_key)
        if not cached:
            await message.reply(
                "<b>Исходный файл не найден в кеше.\n\nПеремотка доступна только для треков, которые были добавлены через /play, /vplay или reply на файл, либо уже были закешированы. Попробуйте добавить трек заново.</b>"
            )
            return
        data, meta = cached
        # Обрезаем через ffmpeg
        new_data = ffmpeg_cut(data, new_offset)
        if not new_data:
            await message.reply("<b>Ошибка ffmpeg при перемотке.</b>")
            return
        # Сохраняем новый файл во временный кеш
        seek_key = f"seek:{cache_key}:{new_offset}"
        if mongo_cache is None:
            raise RuntimeError("MongoDB cache is not initialized. Please check bot startup sequence.")
        await mongo_cache.save_file(seek_key, new_data, {"seek": new_offset, "original": cache_key})
        # Обновляем state и перезапускаем поток
        current["seek_offset"] = new_offset
        state["current"] = current
        await set_state(chat_id, state)
        await leave_voice_chat(chat_id)
        # Запускаем воспроизведение с seek
        await start_playback_locked(chat_id, state, runtime, current)
        await message.reply(f"<b>Перемотка назад на {seconds} секунд.</b>")



async def main():

    global bot, dp, userbot, voice_client, mongo_cache

    # Инициализация асинхронных клиентов внутри event loop
    from aiogram import Bot, Dispatcher
    from aiogram.client.bot import DefaultBotProperties
    from telethon import TelegramClient
    from pytgcalls import PyTgCalls


    # --- Инициализация MongoCache ---
    mongo_cache = MongoCache(MONGO_URL)

    bot = Bot(
        token=API_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    userbot = TelegramClient(StringSession(TELETHON_STRING_SESSION), API_ID, API_HASH)
    voice_client = PyTgCalls(userbot)


    # Регистрируем обработчик stream_end после инициализации voice_client
    voice_client.on_update(tg_filters.stream_end())(on_stream_end)

    # Регистрируем message-хендлеры
    dp.message(CommandStart())(start_cmd)
    dp.message(Command("ping"))(ping_cmd)
    dp.message(Command("play"))(play_cmd)
    dp.message(Command("vplay"))(vplay_cmd)
    dp.message(Command("song"))(song_cmd)
    dp.message(Command("pause"))(pause_cmd)
    dp.message(Command("resume"))(resume_cmd)
    dp.message(Command("skip"))(skip_cmd)
    dp.message(Command("end", "stop"))(end_cmd)
    dp.message(Command("seek"))(seek_cmd)
    dp.message(Command("seekback"))(seekback_cmd)
    # dp.message(lambda message: (message.text or message.caption or "").startswith("/"))(unknown_command_cmd)

    # === Регистрация callback-хендлеров для inline-кнопок ===
    dp.callback_query(lambda c: c.data == "progress")(progress_callback)
    dp.callback_query(lambda c: c.data == "resume")(resume_callback)
    dp.callback_query(lambda c: c.data == "pause")(pause_callback)
    dp.callback_query(lambda c: c.data == "restart")(restart_callback)
    dp.callback_query(lambda c: c.data == "skip")(skip_callback)
    dp.callback_query(lambda c: c.data == "end")(end_callback)
    dp.callback_query(lambda c: c.data == "close")(close_callback)

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    install_signal_handlers(loop, shutdown_event)

    await prepare_environment()
    await userbot.start()
    await voice_client.start()

    print("[INFO] Userbot запущен.")
    print("[INFO] PyTgCalls запущен.")
    print("[INFO] Aiogram polling запущен. Бот ждёт событий...")

    shutdown_task = asyncio.create_task(stop_polling_on_shutdown(shutdown_event))
    try:
        await run_polling_forever(shutdown_event)
    finally:
        shutdown_event.set()
        shutdown_task.cancel()
        with suppress(asyncio.CancelledError):
            await shutdown_task
        await shutdown()
        print("[INFO] Бот завершён корректно.")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
