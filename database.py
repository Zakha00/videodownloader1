import os
from datetime import datetime

try:
    import libsql_experimental as libsql  # pyright: ignore[reportMissingImports]
except ModuleNotFoundError:
    import libsql  # pyright: ignore[reportMissingImports]

TURSO_URL   = os.getenv("TURSO_DB_URL", "")
TURSO_TOKEN = os.getenv("TURSO_DB_TOKEN", "")

FREE_LIMIT     = 3   # бесплатных скачиваний до первой подписки
BATCH_SIZE     = 5   # скачиваний за каждую подписку
REFERRAL_BONUS = 3   # бонус за каждого приглашённого

# Индексы колонок таблицы users
COL_USER_ID    = 0
COL_USERNAME   = 1
COL_FIRST_NAME = 2
COL_DOWNLOADS  = 3
COL_GRANTS     = 4
COL_REF_BONUS  = 5
COL_REFERRER   = 6
COL_JOINED     = 7
COL_ACTIVE     = 8


def _c():
    return libsql.connect(database=TURSO_URL, auth_token=TURSO_TOKEN)


def init_db():
    c = _c()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id             INTEGER PRIMARY KEY,
            username            TEXT    DEFAULT '',
            first_name          TEXT    DEFAULT '',
            downloads           INTEGER DEFAULT 0,
            subscription_grants INTEGER DEFAULT 0,
            referral_bonus      INTEGER DEFAULT 0,
            referrer_id         INTEGER DEFAULT NULL,
            joined_at           TEXT    DEFAULT (datetime('now')),
            last_active         TEXT    DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS downloads_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER,
            url        TEXT,
            title      TEXT    DEFAULT '',
            fmt        TEXT    DEFAULT 'video',
            status     TEXT    DEFAULT 'ok',
            created_at TEXT    DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS ad_channels (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            url    TEXT    NOT NULL,
            name   TEXT    NOT NULL,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS referrals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER UNIQUE,
            created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS stats (
            date            TEXT PRIMARY KEY,
            total_downloads INTEGER DEFAULT 0,
            new_users       INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS group_settings (
            chat_id      INTEGER PRIMARY KEY,
            delete_links INTEGER DEFAULT 0,
            added_at     TEXT DEFAULT (datetime('now'))
        );
    """)
    c.commit()
    _migrate_users_columns()


def _migrate_users_columns() -> None:
    """
    Старые базы Turso могли создать `users` без новых колонок.
    CREATE TABLE IF NOT EXISTS не добавляет поля — тогда row[COL_*] даёт IndexError/TypeError.
    """
    c = _c()
    rows = c.execute("PRAGMA table_info(users)").fetchall()
    if not rows:
        return
    have = {str(r[1]) for r in rows}
    alters = []
    if "subscription_grants" not in have:
        alters.append("ALTER TABLE users ADD COLUMN subscription_grants INTEGER DEFAULT 0")
    if "referral_bonus" not in have:
        alters.append("ALTER TABLE users ADD COLUMN referral_bonus INTEGER DEFAULT 0")
    if "referrer_id" not in have:
        alters.append("ALTER TABLE users ADD COLUMN referrer_id INTEGER DEFAULT NULL")
    if "joined_at" not in have:
        alters.append(
            "ALTER TABLE users ADD COLUMN joined_at TEXT DEFAULT (datetime('now'))"
        )
    if "last_active" not in have:
        alters.append(
            "ALTER TABLE users ADD COLUMN last_active TEXT DEFAULT (datetime('now'))"
        )
    if "downloads" not in have:
        alters.append("ALTER TABLE users ADD COLUMN downloads INTEGER DEFAULT 0")
    for sql in alters:
        c.execute(sql)
    if alters:
        c.commit()


# ─── Users ────────────────────────────────────────────────────────────────────

def upsert_user(user_id: int, username: str, first_name: str):
    c = _c()
    is_new = c.execute(
        "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
    ).fetchone() is None

    c.execute("""
        INSERT INTO users (user_id, username, first_name)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username    = excluded.username,
            first_name  = excluded.first_name,
            last_active = datetime('now')
    """, (user_id, username or '', first_name or ''))

    if is_new:
        today = datetime.now().strftime("%Y-%m-%d")
        c.execute("""
            INSERT INTO stats (date, new_users) VALUES (?, 1)
            ON CONFLICT(date) DO UPDATE SET new_users = new_users + 1
        """, (today,))
    c.commit()


def get_user(user_id: int):
    # Явный порядок колонок совпадает с COL_* (не зависит от порядка в старой таблице).
    return _c().execute(
        """
        SELECT user_id, username, first_name, downloads,
               subscription_grants, referral_bonus, referrer_id,
               joined_at, last_active
        FROM users WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()


def _allowed(row) -> int:
    if not row:
        return FREE_LIMIT
    return FREE_LIMIT + row[COL_GRANTS] * BATCH_SIZE + row[COL_REF_BONUS]


def needs_subscription(user_id: int) -> bool:
    row = get_user(user_id)
    dl  = row[COL_DOWNLOADS] if row else 0
    return dl >= _allowed(row)


def remaining_downloads(user_id: int) -> int:
    row = get_user(user_id)
    dl  = row[COL_DOWNLOADS] if row else 0
    return max(0, _allowed(row) - dl)


def downloads_allowed(user_id: int) -> int:
    return _allowed(get_user(user_id))


def grant_subscription(user_id: int):
    c = _c()
    c.execute(
        "UPDATE users SET subscription_grants = subscription_grants + 1 WHERE user_id = ?",
        (user_id,)
    )
    c.commit()


def increment_downloads(user_id: int):
    c = _c()
    c.execute(
        "UPDATE users SET downloads = downloads + 1, last_active = datetime('now') WHERE user_id = ?",
        (user_id,)
    )
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("""
        INSERT INTO stats (date, total_downloads) VALUES (?, 1)
        ON CONFLICT(date) DO UPDATE SET total_downloads = total_downloads + 1
    """, (today,))
    c.commit()


# ─── Referrals ────────────────────────────────────────────────────────────────

def register_referral(referrer_id: int, referred_id: int) -> bool:
    if referrer_id == referred_id:
        return False
    c = _c()
    if c.execute("SELECT 1 FROM referrals WHERE referred_id = ?", (referred_id,)).fetchone():
        return False
    c.execute(
        "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
        (referrer_id, referred_id)
    )
    c.execute(
        "UPDATE users SET referral_bonus = referral_bonus + ? WHERE user_id = ?",
        (REFERRAL_BONUS, referrer_id)
    )
    c.commit()
    return True


def get_referral_count(user_id: int) -> int:
    row = _c().execute(
        "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)
    ).fetchone()
    return row[0] if row else 0


# ─── History ──────────────────────────────────────────────────────────────────

def log_download(user_id: int, url: str, title: str, fmt: str, status: str):
    c = _c()
    c.execute(
        "INSERT INTO downloads_log (user_id, url, title, fmt, status) VALUES (?, ?, ?, ?, ?)",
        (user_id, url, (title or url)[:120], fmt, status)
    )
    c.commit()


def get_history(user_id: int, limit: int = 8):
    return _c().execute("""
        SELECT title, fmt, created_at FROM downloads_log
        WHERE user_id = ? AND status = 'ok'
        ORDER BY created_at DESC LIMIT ?
    """, (user_id, limit)).fetchall()


# ─── Ad channels (rotation) ───────────────────────────────────────────────────

_ad_idx = 0


def get_ad_channels():
    return _c().execute(
        "SELECT id, url, name, active FROM ad_channels ORDER BY id"
    ).fetchall()


def get_next_ad_channel():
    global _ad_idx
    active = [ch for ch in get_ad_channels() if ch[3] == 1]
    if not active:
        return None, None
    ch = active[_ad_idx % len(active)]
    _ad_idx += 1
    return ch[1], ch[2]   # url, name


def add_ad_channel(url: str, name: str):
    c = _c()
    c.execute("INSERT INTO ad_channels (url, name) VALUES (?, ?)", (url, name))
    c.commit()


def remove_ad_channel(channel_id: int):
    c = _c()
    c.execute("DELETE FROM ad_channels WHERE id = ?", (channel_id,))
    c.commit()


def toggle_ad_channel(channel_id: int, active: bool):
    c = _c()
    c.execute("UPDATE ad_channels SET active = ? WHERE id = ?",
              (1 if active else 0, channel_id))
    c.commit()


# ─── Group settings ───────────────────────────────────────────────────────────

def register_group(chat_id: int):
    _c().execute(
        "INSERT OR IGNORE INTO group_settings (chat_id) VALUES (?)", (chat_id,)
    )
    _c().commit()


def get_group_delete_links(chat_id: int) -> bool:
    row = _c().execute(
        "SELECT delete_links FROM group_settings WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    return bool(row[0]) if row else False


# ─── Admin stats ──────────────────────────────────────────────────────────────

def get_total_users() -> int:
    row = _c().execute("SELECT COUNT(*) FROM users").fetchone()
    return row[0] if row else 0


def get_today_stats():
    today = datetime.now().strftime("%Y-%m-%d")
    return _c().execute("SELECT * FROM stats WHERE date = ?", (today,)).fetchone()


def get_total_downloads_all() -> int:
    row = _c().execute("SELECT SUM(total_downloads) FROM stats").fetchone()
    return row[0] or 0


def get_all_user_ids():
    return [r[0] for r in _c().execute("SELECT user_id FROM users").fetchall()]


def get_top_users(limit: int = 5):
    return _c().execute("""
        SELECT user_id, first_name, username, downloads
        FROM users ORDER BY downloads DESC LIMIT ?
    """, (limit,)).fetchall()
