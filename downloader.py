import asyncio
import os
import re
from pathlib import Path

import yt_dlp

DOWNLOAD_PATH = Path("downloads")
DOWNLOAD_PATH.mkdir(exist_ok=True)

MAX_FILE_MB = 48  # Telegram лимит с запасом

FORMAT_OPTS = {
    "video": {
        "format": (
            "bestvideo[ext=mp4][filesize<48M]+bestaudio[ext=m4a]/"
            "bestvideo[ext=mp4]+bestaudio/"
            "best[ext=mp4]/best"
        ),
        "merge_output_format": "mp4",
    },
    "720p": {
        "format": (
            "bestvideo[height<=720][ext=mp4][filesize<48M]+bestaudio[ext=m4a]/"
            "bestvideo[height<=720][ext=mp4]+bestaudio/"
            "best[height<=720][ext=mp4]/best[height<=720]"
        ),
        "merge_output_format": "mp4",
    },
    "1080p": {
        "format": (
            "bestvideo[height<=1080][ext=mp4][filesize<48M]+bestaudio[ext=m4a]/"
            "bestvideo[height<=1080][ext=mp4]+bestaudio/"
            "best[height<=1080][ext=mp4]/best[height<=1080]"
        ),
        "merge_output_format": "mp4",
    },
    "audio": {
        "format": "bestaudio/best",
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
    },
    "photo": {
        "format": "best",
        "skip_download": False,
    },
}


class DownloadResult:
    def __init__(self, path: str, title: str, fmt: str, is_photo: bool = False):
        self.path = path
        self.title = title
        self.fmt = fmt
        self.is_photo = is_photo


def _sync_get_info(url: str) -> dict:
    """Получает метаданные без скачивания."""
    opts = {"quiet": True, "no_warnings": True, "skip_download": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


def _sync_download(url: str, fmt: str) -> DownloadResult:
    out_tpl = str(DOWNLOAD_PATH / "%(id)s.%(ext)s")

    opts = {
        "outtmpl": out_tpl,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 30,
        "max_filesize": MAX_FILE_MB * 1024 * 1024,
        **FORMAT_OPTS.get(fmt, FORMAT_OPTS["video"]),
    }

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        title = info.get("title", "")

        if fmt == "audio":
            # После постпроцессора расширение меняется на mp3
            base = str(DOWNLOAD_PATH / info["id"])
            path = base + ".mp3"
            if not os.path.exists(path):
                # Fallback: ищем любой файл с этим ID
                for f in DOWNLOAD_PATH.iterdir():
                    if f.stem == info["id"]:
                        path = str(f)
                        break
        else:
            path = ydl.prepare_filename(info)
            if not os.path.exists(path):
                # Файл мог получить другое расширение после merge
                for f in DOWNLOAD_PATH.iterdir():
                    if f.stem == info["id"]:
                        path = str(f)
                        break

        if not os.path.exists(path):
            raise FileNotFoundError(f"Файл не найден после скачивания: {path}")

        size_mb = os.path.getsize(path) / (1024 * 1024)
        if size_mb > MAX_FILE_MB:
            os.remove(path)
            raise ValueError(f"Файл слишком большой ({size_mb:.1f} МБ). Попробуй 720p.")

        return DownloadResult(path=path, title=title, fmt=fmt)


def _sync_download_photos(url: str) -> list[str]:
    """Скачивает фото (Instagram карусели и т.п.)."""
    out_tpl = str(DOWNLOAD_PATH / "%(id)s_%(autonumber)s.%(ext)s")
    opts = {
        "outtmpl": out_tpl,
        "quiet": True,
        "no_warnings": True,
        "format": "best",
        "socket_timeout": 30,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        # Собираем все скачанные файлы
        video_id = info.get("id", "")
        photos = sorted([
            str(f) for f in DOWNLOAD_PATH.iterdir()
            if f.stem.startswith(video_id) and f.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp")
        ])
        return photos


async def get_info(url: str) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_get_info, url)


async def download(url: str, fmt: str = "video") -> DownloadResult:
    loop = asyncio.get_event_loop()
    if fmt == "photo":
        photos = await loop.run_in_executor(None, _sync_download_photos, url)
        if not photos:
            raise FileNotFoundError("Фото не найдены")
        return DownloadResult(path=photos[0], title="", fmt="photo", is_photo=True)
    return await loop.run_in_executor(None, _sync_download, url, fmt)


def cleanup(*paths: str):
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass


def is_valid_url(text: str) -> bool:
    return bool(re.match(r"https?://", text.strip()))


def detect_type(url: str) -> str:
    """Угадывает тип контента по URL."""
    u = url.lower()
    if any(x in u for x in ["instagram.com/p/", "instagram.com/reel"]):
        return "video"
    if "instagram.com" in u:
        return "photo"
    return "video"
