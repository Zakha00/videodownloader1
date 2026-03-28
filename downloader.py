import asyncio
import os
import re
from pathlib import Path

import yt_dlp

DOWNLOAD_PATH = Path("downloads")
DOWNLOAD_PATH.mkdir(exist_ok=True)

MAX_MB = 48   # Telegram limit with margin

# Instagram / Meta часто режут «ботов» и датацентры; без cookies Reels могут падать с "unavailable".
# Опционально: путь к Netscape cookies.txt (экспорт из браузера) — переменная на Render.
_COOKIEFILE = (os.getenv("YTDLP_COOKIEFILE") or os.getenv("COOKIEFILE") or "").strip()

_YDL_COMMON = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "retries": 3,
    "fragment_retries": 5,
    "socket_timeout": 60,
    "http_headers": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    },
}


def _merge_opts(extra: dict) -> dict:
    o = {**_YDL_COMMON, **extra}
    if _COOKIEFILE and Path(_COOKIEFILE).is_file():
        o["cookiefile"] = _COOKIEFILE
    return o


def normalize_url(url: str) -> str:
    """Убирает хвост ?igsh=… и # — иногда мешают экстрактору."""
    u = url.strip().split("#")[0].split("?")[0]
    return u.rstrip("/") or url.strip()

_FORMAT_OPTS = {
    "video": {
        "format": (
            "bestvideo[ext=mp4][filesize<48M]+bestaudio[ext=m4a]/"
            "bestvideo[ext=mp4]+bestaudio/best[ext=mp4]/best"
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
}


class DownloadResult:
    __slots__ = ("path", "title", "fmt", "extra_photos")

    def __init__(self, path: str, title: str, fmt: str,
                 extra_photos: list[str] | None = None):
        self.path         = path
        self.title        = title
        self.fmt          = fmt
        self.extra_photos = extra_photos or []   # для каруселей


def _find_file(video_id: str, suffix: str | None = None) -> str | None:
    """Ищет файл по ID в папке загрузок."""
    for f in DOWNLOAD_PATH.iterdir():
        if f.stem == video_id or f.stem.startswith(video_id + "."):
            if suffix is None or f.suffix.lower() == suffix:
                return str(f)
    return None


def _sync_download(url: str, fmt: str) -> DownloadResult:
    url = normalize_url(url)
    out_tpl = str(DOWNLOAD_PATH / "%(id)s.%(ext)s")
    opts = _merge_opts(
        {
            "outtmpl": out_tpl,
            "max_filesize": MAX_MB * 1024 * 1024,
            **_FORMAT_OPTS[fmt],
        }
    )

    with yt_dlp.YoutubeDL(opts) as ydl:
        info  = ydl.extract_info(url, download=True)
        title = info.get("title", "")
        vid   = info.get("id", "")

        if fmt == "audio":
            path = str(DOWNLOAD_PATH / (vid + ".mp3"))
            if not os.path.exists(path):
                path = _find_file(vid) or path
        else:
            path = ydl.prepare_filename(info)
            if not os.path.exists(path):
                path = _find_file(vid) or path

        if not path or not os.path.exists(path):
            raise FileNotFoundError("Файл не создан после скачивания.")

        mb = os.path.getsize(path) / (1024 * 1024)
        if mb > MAX_MB:
            cleanup(path)
            raise ValueError(
                f"Видео весит {mb:.0f} МБ — слишком большое для Telegram.\n"
                "Попробуй формат 720p ↗️"
            )

        return DownloadResult(path=path, title=title, fmt=fmt)


def _sync_download_photos(url: str) -> DownloadResult:
    """Скачивает фото/карусель (Instagram, VK и т.п.)."""
    url = normalize_url(url)
    out_tpl = str(DOWNLOAD_PATH / "%(id)s_%(autonumber)03d.%(ext)s")
    opts = _merge_opts(
        {
            "outtmpl": out_tpl,
            "format": "best",
        }
    )

    with yt_dlp.YoutubeDL(opts) as ydl:
        info  = ydl.extract_info(url, download=True)
        title = info.get("title", "")
        vid   = info.get("id", "")

    photo_exts = {".jpg", ".jpeg", ".png", ".webp"}
    files = sorted(
        str(f) for f in DOWNLOAD_PATH.iterdir()
        if f.stem.startswith(vid) and f.suffix.lower() in photo_exts
    )

    # Если фотки не нашли — попробуем как видео
    if not files:
        video_exts = {".mp4", ".mov", ".webm"}
        files = sorted(
            str(f) for f in DOWNLOAD_PATH.iterdir()
            if f.stem.startswith(vid) and f.suffix.lower() in video_exts
        )
        if files:
            return DownloadResult(path=files[0], title=title, fmt="video",
                                  extra_photos=files[1:])
        raise FileNotFoundError("Фото не найдены. Попробуй формат 📹 Видео.")

    return DownloadResult(path=files[0], title=title, fmt="photo",
                          extra_photos=files[1:])


async def download(url: str, fmt: str = "video") -> DownloadResult:
    loop = asyncio.get_event_loop()
    if fmt == "photo":
        return await loop.run_in_executor(None, _sync_download_photos, url)
    return await loop.run_in_executor(None, _sync_download, url, fmt)


def cleanup(*paths: str):
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass


def is_valid_url(text: str) -> bool:
    return bool(re.match(r"https?://\S+", text.strip()))
