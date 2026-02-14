import json
import logging
import os
import random
import sqlite3
import asyncio
import calendar
import ssl
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv
import certifi
from telegram import InputFile, LabeledPrice, Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    TypeHandler,
    filters,
)

load_dotenv()


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# Fixed weights for regular gift tiers.
REGULAR_PRIZE_WEIGHTS = {
    50: 3,
    25: 6,
    15: 22,
}
DEFAULT_VALENTINE_50_GIFT_IDS = (
    "5800655655995968830",  # 🎁
    "5801108895304779062",  # 🎁
)

DEFAULT_DAILY_LIMIT_TEXT = "Сьогоднішній ліміт вичерпано: призи на сьогодні вже розіграно."
DEFAULT_DAILY_PRIZE_LIMIT = 1
DEFAULT_AUTO_TOPUP_THRESHOLD = 100
DEFAULT_AUTO_TOPUP_AMOUNT = 615
# Telegram 🎰 dice.value encodes 3 reels x 4 symbols (total 64 outcomes).
# We treat "win" as: any triple OR any two 7s.
SLOT_SYMBOLS = 4
SLOT_REELS = 3
SLOT_SEVEN_SYMBOL = 3
DB_PATH = Path(os.getenv("DB_PATH", "gift_state.sqlite3"))
API_BASE = "https://api.telegram.org"
CLAIM_LOCK = asyncio.Lock()
TZ_NAME = os.getenv("TZ", "Europe/Kyiv")
try:
    APP_TZ = ZoneInfo(TZ_NAME)
except ZoneInfoNotFoundError:
    logger.warning("Unknown TZ=%s, fallback to UTC", TZ_NAME)
    APP_TZ = timezone.utc


@dataclass
class GroupMonthState:
    sent_50: int
    sent_25: int
    sent_15: int


@dataclass
class DailyClaim:
    user_id: int
    winner_name: str
    attempt_no: int
    claimed_stars: int


@dataclass(frozen=True)
class NftPoolItem:
    gift_id: str
    slug: str | None
    title: str | None


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_claims (
                chat_id INTEGER NOT NULL,
                day_key TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                winner_name TEXT NOT NULL DEFAULT '',
                attempt_no INTEGER NOT NULL DEFAULT 0,
                claimed_stars INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, day_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_attempts (
                chat_id INTEGER NOT NULL,
                day_key TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, day_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_claims (
                chat_id INTEGER NOT NULL,
                month_key TEXT NOT NULL,
                sent_50 INTEGER NOT NULL DEFAULT 0,
                sent_25 INTEGER NOT NULL DEFAULT 0,
                sent_15 INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, month_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_nft_claims (
                month_key TEXT NOT NULL,
                gift_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                day_key TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                attempt_no INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                PRIMARY KEY (month_key, gift_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_limit_notices (
                chat_id INTEGER NOT NULL,
                day_key TEXT NOT NULL,
                last_notice_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, day_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_claim_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                day_key TEXT NOT NULL,
                month_key TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                winner_name TEXT NOT NULL DEFAULT '',
                attempt_no INTEGER NOT NULL DEFAULT 0,
                claimed_stars INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def current_keys() -> tuple[str, str]:
    now = datetime.now(APP_TZ)
    return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m")


def has_daily_claim(chat_id: int, day_key: str) -> bool:
    return count_daily_claims(chat_id, day_key) > 0


def get_daily_claim(chat_id: int, day_key: str) -> DailyClaim | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT user_id, winner_name, attempt_no, claimed_stars
            FROM daily_claim_events
            WHERE chat_id = ? AND day_key = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (chat_id, day_key),
        ).fetchone()
    if row:
        return DailyClaim(user_id=row[0], winner_name=row[1], attempt_no=row[2], claimed_stars=row[3])

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT user_id, winner_name, attempt_no, claimed_stars
            FROM daily_claims
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
    if not row:
        return None
    return DailyClaim(user_id=row[0], winner_name=row[1], attempt_no=row[2], claimed_stars=row[3])


def reset_daily_claim(chat_id: int, day_key: str, month_key: str) -> tuple[bool, int, int, int]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT claimed_stars
            FROM daily_claim_events
            WHERE chat_id = ? AND day_key = ? AND month_key = ?
            """,
            (chat_id, day_key, month_key),
        ).fetchall()
        stars_rows = [int(r[0]) for r in rows if r and isinstance(r[0], int)]
        if not stars_rows:
            row = conn.execute(
                """
                SELECT claimed_stars
                FROM daily_claims
                WHERE chat_id = ? AND day_key = ?
                """,
                (chat_id, day_key),
            ).fetchone()
            if row and isinstance(row[0], int):
                stars_rows = [int(row[0])]
        if not stars_rows:
            return False, 0, 0, 0

        rollback_50 = sum(1 for stars in stars_rows if stars == 50)
        rollback_25 = sum(1 for stars in stars_rows if stars == 25)
        rollback_15 = sum(1 for stars in stars_rows if stars == 15)

        conn.execute(
            "DELETE FROM daily_claim_events WHERE chat_id = ? AND day_key = ? AND month_key = ?",
            (chat_id, day_key, month_key),
        )
        conn.execute(
            "DELETE FROM daily_claims WHERE chat_id = ? AND day_key = ?",
            (chat_id, day_key),
        )
        conn.execute(
            "DELETE FROM daily_attempts WHERE chat_id = ? AND day_key = ?",
            (chat_id, day_key),
        )
        conn.execute(
            "DELETE FROM daily_limit_notices WHERE chat_id = ? AND day_key = ?",
            (chat_id, day_key),
        )
        conn.execute(
            """
            DELETE FROM monthly_nft_claims
            WHERE chat_id = ? AND day_key = ? AND month_key = ?
            """,
            (chat_id, day_key, month_key),
        )

        if rollback_50:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_50 = CASE WHEN sent_50 >= ? THEN sent_50 - ? ELSE 0 END
                WHERE chat_id = ? AND month_key = ?
                """,
                (rollback_50, rollback_50, chat_id, month_key),
            )
        if rollback_25:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_25 = CASE WHEN sent_25 >= ? THEN sent_25 - ? ELSE 0 END
                WHERE chat_id = ? AND month_key = ?
                """,
                (rollback_25, rollback_25, chat_id, month_key),
            )
        if rollback_15:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_15 = CASE WHEN sent_15 >= ? THEN sent_15 - ? ELSE 0 END
                WHERE chat_id = ? AND month_key = ?
                """,
                (rollback_15, rollback_15, chat_id, month_key),
            )

        conn.commit()
    return True, rollback_50, rollback_25, rollback_15


def increment_daily_attempt(chat_id: int, day_key: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO daily_attempts (chat_id, day_key, attempts)
            VALUES (?, ?, 0)
            ON CONFLICT(chat_id, day_key) DO NOTHING
            """,
            (chat_id, day_key),
        )
        conn.execute(
            """
            UPDATE daily_attempts
            SET attempts = attempts + 1
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        )
        row = conn.execute(
            """
            SELECT attempts
            FROM daily_attempts
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
        conn.commit()
    if not row:
        return 0
    return row[0]


def get_daily_attempt(chat_id: int, day_key: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT attempts
            FROM daily_attempts
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
    if not row:
        return 0
    return row[0]


def reset_daily_attempt(chat_id: int, day_key: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            DELETE FROM daily_attempts
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        )
        conn.commit()


def count_daily_claims(chat_id: int, day_key: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        events_row = conn.execute(
            """
            SELECT COUNT(*)
            FROM daily_claim_events
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
        legacy_row = conn.execute(
            """
            SELECT COUNT(*)
            FROM daily_claims
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
    events_count = int(events_row[0]) if events_row and events_row[0] is not None else 0
    legacy_count = int(legacy_row[0]) if legacy_row and legacy_row[0] is not None else 0
    if events_count > 0:
        return events_count
    return legacy_count


def get_month_state(chat_id: int, month_key: str) -> GroupMonthState:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT sent_50, sent_25, sent_15
            FROM monthly_claims
            WHERE chat_id = ? AND month_key = ?
            """,
            (chat_id, month_key),
        ).fetchone()
    if not row:
        return GroupMonthState(sent_50=0, sent_25=0, sent_15=0)
    return GroupMonthState(sent_50=row[0], sent_25=row[1], sent_15=row[2])


def get_setting(key: str) -> str | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT value FROM bot_settings WHERE key = ?",
            (key,),
        ).fetchone()
    if not row:
        return None
    return row[0]


def set_setting(key: str, value: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO bot_settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        conn.commit()


def set_topup_status(status: str, details: str = "") -> None:
    set_setting("topup_status", status)
    set_setting("topup_details", details[:1000])


def get_daily_prize_limit() -> int:
    raw_setting = get_setting("daily_prize_limit")
    if raw_setting is not None:
        try:
            return max(0, min(100, int(raw_setting)))
        except ValueError:
            logger.warning("Invalid daily_prize_limit setting=%s. Fallback to env/default.", raw_setting)

    raw_env = os.getenv("DAILY_PRIZE_LIMIT", str(DEFAULT_DAILY_PRIZE_LIMIT)).strip()
    try:
        return max(0, min(100, int(raw_env)))
    except ValueError:
        logger.warning("Invalid DAILY_PRIZE_LIMIT=%s. Fallback to default=%s", raw_env, DEFAULT_DAILY_PRIZE_LIMIT)
        return DEFAULT_DAILY_PRIZE_LIMIT


def set_daily_prize_limit(limit: int) -> None:
    set_setting("daily_prize_limit", str(max(0, min(100, limit))))


def average_regular_prize_stars() -> float:
    total_units = sum(REGULAR_PRIZE_WEIGHTS.values())
    if total_units <= 0:
        return 20.0
    weighted_sum = sum(stars * count for stars, count in REGULAR_PRIZE_WEIGHTS.items())
    return weighted_sum / total_units


def estimate_monthly_stars_spend(daily_limit: int) -> tuple[int, float]:
    now = datetime.now(APP_TZ)
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    avg_stars = average_regular_prize_stars()
    estimate = daily_limit * days_in_month * avg_stars
    return days_in_month, estimate


def should_send_daily_limit_notice(chat_id: int, day_key: str, cooldown_min: int) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT last_notice_at
            FROM daily_limit_notices
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
    if not row:
        return True

    last_notice_at = datetime.fromisoformat(row[0])
    now_utc = datetime.now(timezone.utc)
    return now_utc - last_notice_at >= timedelta(minutes=cooldown_min)


def mark_daily_limit_notice(chat_id: int, day_key: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO daily_limit_notices (chat_id, day_key, last_notice_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, day_key)
            DO UPDATE SET last_notice_at = excluded.last_notice_at
            """,
            (chat_id, day_key, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def is_dry_run() -> bool:
    # DRY_RUN=1 disables side effects like notifying the payer/owner about winners.
    return parse_bool_env("DRY_RUN", False)


def decode_slot_value(value: int) -> tuple[int, int, int] | None:
    # Mapping assumption:
    # value in 1..64 corresponds to base-4 digits of (value - 1), one digit per reel.
    if not isinstance(value, int) or value < 1 or value > SLOT_SYMBOLS**SLOT_REELS:
        return None
    x = value - 1
    a = x // 16
    b = (x // 4) % 4
    c = x % 4
    return a, b, c


def is_winning_slot_value(value: int) -> bool:
    symbols = decode_slot_value(value)
    if not symbols:
        return False
    a, b, c = symbols
    if a == b == c:
        return True
    seven_count = int(a == SLOT_SEVEN_SYMBOL) + int(b == SLOT_SEVEN_SYMBOL) + int(c == SLOT_SEVEN_SYMBOL)
    return seven_count >= 2


def parse_id_list_env(name: str, default: tuple[str, ...]) -> list[str]:
    raw = os.getenv(name, "")
    result: list[str] = []
    seen: set[str] = set()
    chunks = raw.split(",") if raw else list(default)
    for chunk in chunks:
        item = chunk.strip()
        if not item or not item.isdigit() or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def is_valentine_day(now: datetime | None = None) -> bool:
    current = now or datetime.now(APP_TZ)
    return current.month == 2 and current.day == 14


def valentine_50_gift_ids() -> list[str]:
    return parse_id_list_env("VALENTINE_50_GIFT_IDS", DEFAULT_VALENTINE_50_GIFT_IDS)


def pick_preferred_50_gift_id(gifts: list[dict[str, Any]], preferred_ids: list[str]) -> str | None:
    available_50_ids: set[str] = set()
    for gift in gifts:
        gift_id = gift.get("id")
        if gift.get("star_count") == 50 and gift_id is not None:
            available_50_ids.add(str(gift_id))

    candidates = [gift_id for gift_id in preferred_ids if gift_id in available_50_ids]
    if not candidates:
        return None
    return random.choice(candidates)


def parse_monthly_nft_pool() -> list[NftPoolItem]:
    raw_pool = os.getenv("MONTHLY_NFT_POOL", "")
    result: list[NftPoolItem] = []
    seen: set[str] = set()

    for chunk in raw_pool.split(","):
        item = chunk.strip()
        if not item:
            continue
        parts = item.split(":", 2)
        gift_id = parts[0].strip() if parts else ""
        slug: str | None = None
        title: str | None = None
        if len(parts) >= 2:
            slug = parts[1].strip() or None
        if len(parts) >= 3:
            title = parts[2].strip() or None
        if not gift_id.isdigit():
            logger.warning("Invalid gift id in MONTHLY_NFT_POOL: %s", item)
            continue
        if gift_id in seen:
            continue
        seen.add(gift_id)
        result.append(NftPoolItem(gift_id=gift_id, slug=slug, title=title))

    return result


def resolve_nft_slug(gift_id: str) -> str | None:
    for item in parse_monthly_nft_pool():
        if item.gift_id == gift_id:
            return item.slug
    return None


def resolve_nft_title(gift_id: str) -> str | None:
    for item in parse_monthly_nft_pool():
        if item.gift_id == gift_id:
            return item.title
    return None


def get_claimed_monthly_nft_gift_ids(month_key: str) -> set[str]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT gift_id
            FROM monthly_nft_claims
            WHERE month_key = ?
            """,
            (month_key,),
        ).fetchall()
    return {str(row[0]) for row in rows if row and isinstance(row[0], str)}


def save_monthly_nft_claim(
    month_key: str,
    gift_id: str,
    chat_id: int,
    day_key: str,
    user_id: int,
    attempt_no: int,
) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        inserted = conn.execute(
            """
            INSERT OR IGNORE INTO monthly_nft_claims (
                month_key, gift_id, chat_id, day_key, user_id, attempt_no, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                month_key,
                gift_id,
                chat_id,
                day_key,
                user_id,
                attempt_no,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    return inserted.rowcount > 0


def month_draws_left(now: datetime) -> int:
    _, days_in_month = calendar.monthrange(now.year, now.month)
    days_left = max(1, days_in_month - now.day + 1)
    daily_limit = max(1, get_daily_prize_limit())
    return days_left * daily_limit


def pick_monthly_nft_gift(month_key: str) -> tuple[str | None, int, int, float]:
    pool = [item.gift_id for item in parse_monthly_nft_pool()]
    if not pool:
        return None, 0, 0, 0.0
    used = get_claimed_monthly_nft_gift_ids(month_key)
    remaining = [gift_id for gift_id in pool if gift_id not in used]
    nft_left = len(remaining)
    if nft_left <= 0:
        return None, 0, 0, 0.0

    now = datetime.now(APP_TZ)
    draws_left = month_draws_left(now)
    if parse_bool_env("NFT_TEST_FORCE", False):
        return random.choice(remaining), nft_left, draws_left, 1.0
    probability = min(1.0, nft_left / draws_left)
    if random.random() < probability:
        return random.choice(remaining), nft_left, draws_left, probability
    return None, nft_left, draws_left, probability


def allowed_chat_ids() -> set[int]:
    raw = os.getenv("ALLOWED_CHAT_IDS", "")
    result: set[int] = set()
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        try:
            result.add(int(item))
        except ValueError:
            logger.warning("Invalid chat id in ALLOWED_CHAT_IDS: %s", item)
    return result


def is_chat_allowed(chat_id: int) -> bool:
    allowed = allowed_chat_ids()
    # Security-first: if whitelist is empty, bot ignores all chats.
    if not allowed:
        return False
    return chat_id in allowed


def is_slot_allowed_context(chat_type: str, chat_id: int, user_id: int) -> bool:
    if chat_type == "private":
        return is_stats_owner(user_id)
    return is_chat_allowed(chat_id)


def resolve_business_connection_id() -> str | None:
    env_value = os.getenv("BUSINESS_CONNECTION_ID")
    if env_value:
        return env_value
    return get_setting("business_connection_id")


def resolve_stats_owner_user_id() -> int | None:
    owner_id_raw = os.getenv("STATS_OWNER_USER_ID", "").strip()
    if not owner_id_raw:
        return None
    try:
        return int(owner_id_raw)
    except ValueError:
        return None


def is_stats_owner(user_id: int) -> bool:
    owner_id = resolve_stats_owner_user_id()
    if owner_id is None:
        return True
    return user_id == owner_id


def get_gift_convert_value(gift: dict[str, Any]) -> int:
    for key in ("convert_star_count", "convert_stars", "resale_star_count"):
        value = gift.get(key)
        if isinstance(value, int):
            return value
    return 0


def get_gift_sticker_file_id(gift: dict[str, Any]) -> str | None:
    sticker = gift.get("sticker")
    if not isinstance(sticker, dict):
        return None
    file_id = sticker.get("file_id")
    if isinstance(file_id, str) and file_id:
        return file_id
    return None


def is_sellable_gift(gift: dict[str, Any]) -> bool:
    if get_gift_convert_value(gift) > 0:
        return True
    for key in ("can_be_converted_to_stars", "can_be_sold", "sellable"):
        value = gift.get(key)
        if isinstance(value, bool) and value:
            return True
    return False


def save_successful_claim(
    chat_id: int,
    user_id: int,
    winner_name: str,
    attempt_no: int,
    day_key: str,
    month_key: str,
    stars: int,
) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO daily_claims (
                chat_id, day_key, user_id, winner_name, attempt_no, claimed_stars, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (chat_id, day_key, user_id, winner_name, attempt_no, stars, datetime.now(timezone.utc).isoformat()),
        )
        conn.execute(
            """
            INSERT INTO daily_claim_events (
                chat_id, day_key, month_key, user_id, winner_name, attempt_no, claimed_stars, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (chat_id, day_key, month_key, user_id, winner_name, attempt_no, stars, datetime.now(timezone.utc).isoformat()),
        )
        conn.execute(
            """
            INSERT INTO monthly_claims (chat_id, month_key, sent_50, sent_25, sent_15)
            VALUES (?, ?, 0, 0, 0)
            ON CONFLICT(chat_id, month_key) DO NOTHING
            """,
            (chat_id, month_key),
        )
        if stars == 50:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_50 = sent_50 + 1
                WHERE chat_id = ? AND month_key = ?
                """,
                (chat_id, month_key),
            )
        elif stars == 25:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_25 = sent_25 + 1
                WHERE chat_id = ? AND month_key = ?
                """,
                (chat_id, month_key),
            )
        elif stars == 15:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_15 = sent_15 + 1
                WHERE chat_id = ? AND month_key = ?
                """,
                (chat_id, month_key),
            )
        conn.commit()
    # Attempts are tracked per prize cycle, so reset after a successful claim.
    reset_daily_attempt(chat_id, day_key)
    return True


def _api_call(token: str, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{API_BASE}/bot{token}/{method}"
    data = json.dumps(payload or {}).encode("utf-8")
    req = Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=20) as response:
            body = response.read().decode("utf-8")
    except HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API HTTP {err.code}: {body}") from err
    except URLError as err:
        raise RuntimeError(f"Telegram API network error: {err}") from err

    parsed = json.loads(body)
    if not parsed.get("ok"):
        raise RuntimeError(f"Telegram API error in {method}: {parsed}")
    return parsed["result"]


async def get_available_gifts(token: str) -> list[dict[str, Any]]:
    result = await asyncio.to_thread(_api_call, token, "getAvailableGifts")
    return result.get("gifts", [])


async def get_my_star_balance(token: str) -> int:
    result = await asyncio.to_thread(_api_call, token, "getMyStarBalance")
    amount = result.get("amount")
    if isinstance(amount, int):
        return amount
    return 0


async def get_business_account_gifts(
    token: str,
    business_connection_id: str,
    *,
    offset: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"business_connection_id": business_connection_id}
    if isinstance(offset, str) and offset:
        payload["offset"] = offset
    if isinstance(limit, int) and limit > 0:
        payload["limit"] = max(1, min(100, limit))
    result = await asyncio.to_thread(_api_call, token, "getBusinessAccountGifts", payload)
    if isinstance(result, dict):
        return result
    return {}


async def transfer_gift_to_user(
    token: str,
    business_connection_id: str,
    owned_gift_id: str,
    new_owner_chat_id: int,
    *,
    star_count: int | None = None,
) -> None:
    payload: dict[str, Any] = {
        "business_connection_id": business_connection_id,
        "owned_gift_id": owned_gift_id,
        "new_owner_chat_id": new_owner_chat_id,
    }
    if isinstance(star_count, int) and star_count > 0:
        payload["star_count"] = star_count
    await asyncio.to_thread(_api_call, token, "transferGift", payload)


async def transfer_business_account_stars(token: str, business_connection_id: str, star_count: int) -> None:
    payload = {
        "business_connection_id": business_connection_id,
        "star_count": star_count,
    }
    await asyncio.to_thread(_api_call, token, "transferBusinessAccountStars", payload)


async def maybe_auto_topup(token: str) -> None:
    if not parse_bool_env("AUTO_TOPUP_ENABLED", True):
        set_topup_status("disabled", "AUTO_TOPUP_ENABLED=0")
        return

    threshold_raw = os.getenv("AUTO_TOPUP_THRESHOLD", str(DEFAULT_AUTO_TOPUP_THRESHOLD))
    amount_raw = os.getenv("AUTO_TOPUP_AMOUNT", str(DEFAULT_AUTO_TOPUP_AMOUNT))
    try:
        threshold = max(1, int(threshold_raw))
    except ValueError:
        threshold = DEFAULT_AUTO_TOPUP_THRESHOLD
    try:
        transfer_amount = max(1, min(10000, int(amount_raw)))
    except ValueError:
        transfer_amount = DEFAULT_AUTO_TOPUP_AMOUNT

    balance = await get_my_star_balance(token)
    if balance >= threshold:
        set_topup_status("ok", f"balance={balance} >= threshold={threshold}")
        return

    business_connection_id = resolve_business_connection_id()
    if not business_connection_id:
        logger.warning("Auto top-up skipped: BUSINESS_CONNECTION_ID is unknown")
        set_topup_status("skipped", "BUSINESS_CONNECTION_ID is unknown")
        return

    try:
        await transfer_business_account_stars(token, business_connection_id, transfer_amount)
        logger.info(
            "Auto top-up completed: transferred %s Stars, previous balance was %s",
            transfer_amount,
            balance,
        )
        set_topup_status("ok", f"transferred={transfer_amount}, previous_balance={balance}")
    except Exception as err:
        err_text = str(err)
        if "BOT_ACCESS_FORBIDDEN" in err_text:
            set_topup_status("forbidden", err_text)
        else:
            set_topup_status("error", err_text)
        raise


def capture_business_connection_id(update: Update) -> str | None:
    business_connection = getattr(update, "business_connection", None)
    if business_connection and getattr(business_connection, "id", None):
        return business_connection.id

    business_message = getattr(update, "business_message", None)
    if business_message and getattr(business_message, "business_connection_id", None):
        return business_message.business_connection_id

    edited_business_message = getattr(update, "edited_business_message", None)
    if edited_business_message and getattr(edited_business_message, "business_connection_id", None):
        return edited_business_message.business_connection_id

    deleted_business_messages = getattr(update, "deleted_business_messages", None)
    if deleted_business_messages and getattr(deleted_business_messages, "business_connection_id", None):
        return deleted_business_messages.business_connection_id

    return None


async def on_any_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    business_connection_id = capture_business_connection_id(update)
    if not business_connection_id:
        return
    set_setting("business_connection_id", business_connection_id)


async def on_startup(application: Application) -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        return
    try:
        me = await application.bot.get_me()
        logger.info("Bot identity: id=%s username=@%s", me.id, me.username)
    except Exception as err:
        logger.warning("Failed to fetch bot identity: %s", err)
    try:
        await maybe_auto_topup(token)
    except Exception as err:
        logger.exception("Startup auto top-up failed: %s", err)


async def on_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat:
        return
    if chat.type != "private":
        return
    if not user:
        return

    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        await message.reply_text("BOT_TOKEN не налаштовано.")
        return

    day_key, month_key = current_keys()
    allowed = sorted(allowed_chat_ids())
    business_connection_id = resolve_business_connection_id() or "невідомо"
    auto_enabled = parse_bool_env("AUTO_TOPUP_ENABLED", True)
    threshold = os.getenv("AUTO_TOPUP_THRESHOLD", str(DEFAULT_AUTO_TOPUP_THRESHOLD))
    amount = os.getenv("AUTO_TOPUP_AMOUNT", str(DEFAULT_AUTO_TOPUP_AMOUNT))
    topup_status = get_setting("topup_status") or "unknown"
    topup_details = get_setting("topup_details") or "-"

    try:
        balance = await get_my_star_balance(token)
        balance_text = str(balance)
    except Exception as err:
        balance_text = f"помилка: {err}"

    lines = [
        "Статус бота",
        f"- Баланс бота: {balance_text} Stars",
        f"- DAILY_PRIZE_LIMIT: {get_daily_prize_limit()}",
        f"- AUTO_TOPUP_ENABLED: {1 if auto_enabled else 0}",
        f"- AUTO_TOPUP_THRESHOLD: {threshold}",
        f"- AUTO_TOPUP_AMOUNT: {amount}",
        f"- business_connection_id: {business_connection_id}",
        f"- TopUp status: {topup_status}",
        f"- TopUp details: {topup_details}",
        f"- Місяць: {month_key}",
        f"- День: {day_key}",
    ]
    nft_pool = [item.gift_id for item in parse_monthly_nft_pool()]
    claimed_nft_ids = get_claimed_monthly_nft_gift_ids(month_key)
    remaining_nft = [gift_id for gift_id in nft_pool if gift_id not in claimed_nft_ids]
    lines.append(
        f"- NFT pool ({month_key}): total={len(nft_pool)}, claimed={len(nft_pool) - len(remaining_nft)}, left={len(remaining_nft)}"
    )

    if not allowed:
        lines.append("- ALLOWED_CHAT_IDS: порожній (бот ніде не працює)")
    else:
        lines.append("- Групи у whitelist:")
        for chat_id in allowed:
            month_state = get_month_state(chat_id, month_key)
            attempts_today = get_daily_attempt(chat_id, day_key)
            sent_today = count_daily_claims(chat_id, day_key)
            claim = get_daily_claim(chat_id, day_key)
            if claim:
                winner_text = f"{claim.winner_name or f'ID {claim.user_id}'} (спроба #{claim.attempt_no})"
            else:
                winner_text = "ще не було"
            lines.append(
                f"  {chat_id}: видано за місяць 50/25/15 = {month_state.sent_50}/{month_state.sent_25}/{month_state.sent_15}, "
                f"спроб сьогодні = {attempts_today}, видано сьогодні = {sent_today}, останній переможець = {winner_text}"
            )

    await message.reply_text("\n".join(lines))


async def on_ids(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return

    logger.info(
        "ID probe: chat_id=%s chat_type=%s user_id=%s username=@%s",
        chat.id,
        chat.type,
        user.id,
        user.username or "",
    )
    await message.reply_text(
        "IDs\n"
        f"- chat_id: {chat.id}\n"
        f"- user_id: {user.id}\n"
        f"- username: @{user.username or '-'}"
    )


async def on_topup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return

    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        await message.reply_text("BOT_TOKEN не налаштовано.")
        return

    business_connection_id = resolve_business_connection_id()
    if not business_connection_id:
        await message.reply_text("Не знайдено business_connection_id. Підключи бізнес-бота або задай BUSINESS_CONNECTION_ID.")
        return

    if context.args:
        amount_raw = context.args[0]
    else:
        amount_raw = os.getenv("AUTO_TOPUP_AMOUNT", str(DEFAULT_AUTO_TOPUP_AMOUNT))

    try:
        amount = int(amount_raw)
    except ValueError:
        await message.reply_text("Сума має бути числом. Приклад: /topup 615")
        return

    if amount < 1 or amount > 10000:
        await message.reply_text("Сума має бути в діапазоні 1..10000.")
        return

    try:
        before_balance = await get_my_star_balance(token)
        await transfer_business_account_stars(token, business_connection_id, amount)
        after_balance = await get_my_star_balance(token)
        set_topup_status("ok", f"manual_transfer={amount}, before={before_balance}, after={after_balance}")
        await message.reply_text(
            f"Поповнення виконано: +{amount} Stars.\n"
            f"Баланс був: {before_balance}, став: {after_balance}."
        )
    except Exception as err:
        err_text = str(err)
        if "BOT_ACCESS_FORBIDDEN" in err_text:
            set_topup_status("forbidden", err_text)
        else:
            set_topup_status("error", err_text)
        await message.reply_text(f"Не вдалося поповнити Stars: {err}")


async def on_buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return

    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    if context.args:
        amount_raw = context.args[0]
    else:
        amount_raw = os.getenv("AUTO_TOPUP_AMOUNT", str(DEFAULT_AUTO_TOPUP_AMOUNT))

    try:
        amount = int(amount_raw)
    except ValueError:
        await message.reply_text("Сума має бути числом. Приклад: /buy 615")
        return

    if amount < 1 or amount > 10000:
        await message.reply_text("Сума має бути в діапазоні 1..10000.")
        return

    await context.bot.send_invoice(
        chat_id=chat.id,
        title="Поповнення балансу бота",
        description=f"Оплата {amount} Stars для балансу бота",
        payload=f"bot_topup_{amount}",
        currency="XTR",
        prices=[LabeledPrice(label="Stars", amount=amount)],
        provider_token="",
    )


async def on_gifts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return
    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        await message.reply_text("BOT_TOKEN не налаштовано.")
        return

    try:
        gifts = await get_available_gifts(token)
    except Exception as err:
        await message.reply_text(f"Не вдалося отримати подарунки: {err}")
        return

    if not gifts:
        await message.reply_text("Доступних подарунків зараз немає.")
        return

    lines = [f"Доступні подарунки: {len(gifts)}", "Режим /gifts: показує всі подарунки (без фільтра)"]
    for gift in sorted(gifts, key=lambda g: (g.get("star_count", 0), str(g.get("id", "")))):
        gift_id = gift.get("id")
        stars = gift.get("star_count")
        convert_value = get_gift_convert_value(gift)
        sellable = is_sellable_gift(gift)
        sold_out = gift.get("is_sold_out")
        lines.append(
            f"- id={gift_id} stars={stars} convert={convert_value} sellable={1 if sellable else 0} sold_out={1 if sold_out else 0}"
        )

    text = "\n".join(lines)
    if len(text) > 3500:
        text = text[:3500] + "\n...обрізано"
    await message.reply_text(text)


def is_unique_owned_gift(gift: dict[str, Any]) -> bool:
    gift_type = str(gift.get("type", "")).lower()
    if gift_type == "unique":
        return True
    inner = gift.get("gift")
    if isinstance(inner, dict):
        # Fallback heuristic for older/newer payload shapes.
        if any(key in inner for key in ("model", "symbol", "backdrop", "gift_id", "is_from_blockchain")):
            return True
    return False


async def on_nfts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return
    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        await message.reply_text("BOT_TOKEN не налаштовано.")
        return

    business_connection_id = resolve_business_connection_id()
    if not business_connection_id:
        await message.reply_text("Не знайдено business_connection_id. Підключи бізнес-бота або задай BUSINESS_CONNECTION_ID.")
        return

    all_gifts: list[dict[str, Any]] = []
    offset: str | None = None
    try:
        for _ in range(5):
            result = await get_business_account_gifts(token, business_connection_id, offset=offset, limit=100)
            batch = result.get("gifts", [])
            if isinstance(batch, list):
                all_gifts.extend([item for item in batch if isinstance(item, dict)])
            next_offset = result.get("next_offset")
            if not isinstance(next_offset, str) or not next_offset or next_offset == offset:
                break
            offset = next_offset
    except Exception as err:
        await message.reply_text(f"Не вдалося отримати gifts бізнес-акаунта: {err}")
        return

    unique_gifts = [gift for gift in all_gifts if is_unique_owned_gift(gift)]
    if not unique_gifts:
        await message.reply_text("NFT (unique gifts) не знайдено.")
        return

    lines = [f"NFT (unique gifts): {len(unique_gifts)}"]
    for gift in unique_gifts:
        owned_gift_id = gift.get("owned_gift_id")
        can_transfer = 1 if bool(gift.get("can_be_transferred")) else 0
        transfer_star_count = gift.get("transfer_star_count")
        next_transfer_date = gift.get("next_transfer_date")
        inner = gift.get("gift")
        gift_id: Any = None
        model: Any = None
        symbol: Any = None
        if isinstance(inner, dict):
            gift_id = inner.get("gift_id", inner.get("id"))
            model = inner.get("model")
            symbol = inner.get("symbol")
        lines.append(
            f"- owned_gift_id={owned_gift_id} gift_id={gift_id} model={model} symbol={symbol} "
            f"can_transfer={can_transfer} fee={transfer_star_count} next_transfer_date={next_transfer_date}"
        )

    text = "\n".join(lines)
    if len(text) > 3500:
        text = text[:3500] + "\n...обрізано"
    await message.reply_text(text)


async def on_transfernft(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return
    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        await message.reply_text("BOT_TOKEN не налаштовано.")
        return

    business_connection_id = resolve_business_connection_id()
    if not business_connection_id:
        await message.reply_text("Не знайдено business_connection_id. Підключи бізнес-бота або задай BUSINESS_CONNECTION_ID.")
        return

    if len(context.args) < 2:
        await message.reply_text("Формат: /transfernft <owned_gift_id> <user_id> [star_count]")
        return

    owned_gift_id = context.args[0].strip()
    if not owned_gift_id:
        await message.reply_text("owned_gift_id порожній.")
        return

    try:
        new_owner_chat_id = int(context.args[1])
    except ValueError:
        await message.reply_text("user_id має бути числом. Формат: /transfernft <owned_gift_id> <user_id> [star_count]")
        return

    star_count: int | None = None
    if len(context.args) >= 3:
        try:
            star_count = int(context.args[2])
        except ValueError:
            await message.reply_text("star_count має бути числом.")
            return
        if star_count < 0 or star_count > 100000:
            await message.reply_text("star_count має бути в діапазоні 0..100000.")
            return

    try:
        await transfer_gift_to_user(
            token,
            business_connection_id,
            owned_gift_id,
            new_owner_chat_id,
            star_count=star_count,
        )
        fee_text = f", fee={star_count}" if isinstance(star_count, int) and star_count > 0 else ""
        await message.reply_text(
            f"NFT передано успішно.\nowned_gift_id={owned_gift_id}\nnew_owner_chat_id={new_owner_chat_id}{fee_text}"
        )
    except Exception as err:
        await message.reply_text(f"Не вдалося передати NFT: {err}")


async def on_resetday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return
    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    allowed = sorted(allowed_chat_ids())
    target_chat_id: int | None = None
    target_day_key: str | None = None

    if context.args:
        try:
            target_chat_id = int(context.args[0])
        except ValueError:
            await message.reply_text("Невірний chat_id. Приклад: /resetday -1001234567890")
            return
        if len(context.args) > 1:
            target_day_key = context.args[1]
            try:
                datetime.strptime(target_day_key, "%Y-%m-%d")
            except ValueError:
                await message.reply_text("Невірна дата. Формат: YYYY-MM-DD")
                return
    elif len(allowed) == 1:
        target_chat_id = allowed[0]
    else:
        await message.reply_text("Вкажи chat_id: /resetday -1001234567890")
        return

    if target_chat_id not in allowed:
        await message.reply_text("Цей chat_id не входить до ALLOWED_CHAT_IDS.")
        return

    if not target_day_key:
        target_day_key, _ = current_keys()
    target_month_key = target_day_key[:7]

    removed, rollback_50, rollback_25, rollback_15 = reset_daily_claim(target_chat_id, target_day_key, target_month_key)
    if not removed:
        await message.reply_text(f"На {target_day_key} для чату {target_chat_id} прапорця переможця не було.")
        return

    await message.reply_text(
        f"Скинуто денний прапорець для чату {target_chat_id} на {target_day_key}.\n"
        f"Місячний лічильник відкотили: 50={rollback_50}, 25={rollback_25}, 15={rollback_15}."
    )


async def on_dailylimit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return
    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    if context.args:
        try:
            new_limit = int(context.args[0])
        except ValueError:
            await message.reply_text("Формат: /dailylimit [0..100]. Приклад: /dailylimit 2")
            return
        if new_limit < 0 or new_limit > 100:
            await message.reply_text("Ліміт має бути в діапазоні 0..100.")
            return
        set_daily_prize_limit(new_limit)
        logger.info("Daily prize limit updated by owner: %s", new_limit)

    limit = get_daily_prize_limit()
    day_key, _ = current_keys()
    days_in_month, monthly_estimate = estimate_monthly_stars_spend(limit)
    avg_stars = average_regular_prize_stars()
    lines = [f"Денний ліміт призів: {limit} (0 = вимкнути видачу).", f"Сьогодні ({day_key}) по групах:"]
    allowed = sorted(allowed_chat_ids())
    if not allowed:
        lines.append("- ALLOWED_CHAT_IDS порожній.")
    else:
        for chat_id in allowed:
            sent = count_daily_claims(chat_id, day_key)
            lines.append(f"- {chat_id}: {sent}/{limit}")
    lines.append(
        f"Оцінка витрат на місяць: ~{monthly_estimate:.1f} Stars "
        f"(днів={days_in_month}, середній приз={avg_stars:.2f})."
    )
    lines.append("Примітка: оцінка не включає NFT-призи.")
    await message.reply_text("\n".join(lines))


async def on_precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    if not query:
        return
    await query.answer(ok=True)


async def on_successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message or not message.successful_payment:
        return
    amount = message.successful_payment.total_amount
    currency = message.successful_payment.currency
    await message.reply_text(f"Платіж успішний: {amount} {currency}. Stars зарахуються на баланс бота.")


async def notify_winner_to_owner(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    winner_name: str,
    winner_username: str | None,
    winner_user_id: int,
    attempt_no: int,
    stars_tier: int,
    gift: dict[str, Any],
) -> None:
    if is_dry_run():
        return
    if not parse_bool_env("WINNER_NOTIFY_ENABLED", True):
        return

    owner_id = resolve_stats_owner_user_id()
    if owner_id is None:
        return

    gift_id = gift.get("id")
    sellable = 1 if is_sellable_gift(gift) else 0
    user_link = f"tg://user?id={winner_user_id}"
    username_text = f"@{winner_username}" if winner_username else "-"
    day_key, month_key = current_keys()
    text = (
        "Переможець дня\n"
        f"- Група: {chat_id}\n"
        f"- Користувач: {winner_name}\n"
        f"- Username: {username_text}\n"
        f"- Link: {user_link}\n"
        f"- Спроба: #{attempt_no}\n"
        f"- Тип подарунка: {stars_tier} Stars\n"
        f"- gift_id: {gift_id}\n"
        f"- sellable: {sellable}\n"
        f"- День/місяць: {day_key} / {month_key}"
    )
    await context.bot.send_message(chat_id=owner_id, text=text)


async def send_visual_gift_message(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    sticker_file_id: str,
    actual_stars: int,
    gift_id: Any,
) -> None:
    try:
        await message.reply_document(document=sticker_file_id)
        return
    except BadRequest as err:
        # Some Telegram file_ids are not accepted directly by sendDocument (Document_invalid).
        # In that case, download by get_file and re-upload as a fresh document.
        if "Document_invalid" in str(err):
            try:
                tg_file = await context.bot.get_file(sticker_file_id)
                file_bytes = BytesIO()
                await tg_file.download_to_memory(out=file_bytes)
                file_bytes.seek(0)
                filename = tg_file.file_path.split("/")[-1] if tg_file.file_path else f"gift_{gift_id}.tgs"
                await message.reply_document(document=InputFile(file_bytes, filename=filename))
                return
            except Exception:
                return
        return
    except Exception:
        return


async def send_visual_nft_by_slug(
    message,
    *,
    slug: str,
    gift_id: str,
) -> bool:
    slug_value = slug.strip()
    if not slug_value:
        return False
    url = f"https://nft.fragment.com/gift/{quote(slug_value)}.tgs"

    def _download() -> bytes:
        req = Request(
            url=url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
            },
        )
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        with urlopen(req, timeout=20, context=ssl_ctx) as response:
            return response.read()

    try:
        content = await asyncio.to_thread(_download)
    except Exception as err:
        logger.info("Failed to download NFT tgs by slug: gift_id=%s slug=%s err=%s", gift_id, slug_value, err)
        return False

    if not content:
        return False
    try:
        await message.reply_document(
            document=InputFile(BytesIO(content), filename=f"{slug_value}.tgs")
        )
        return True
    except Exception as err:
        logger.info("Failed to send NFT tgs by slug: gift_id=%s slug=%s err=%s", gift_id, slug_value, err)
        return False


async def deliver_gift_like_win(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    token: str,
    chat_id: int,
    user_id: int,
    winner_name: str,
    winner_username: str | None,
    attempt_no: int,
    day_key: str,
    month_key: str,
    stars_tier: int,
    persist_claim: bool,
    notify_owner: bool,
    announced_gift_id: str | None = None,
    success_caption: str | None = None,
) -> bool:
    gifts: list[dict[str, Any]] = []
    gift: dict[str, Any] | None = None
    actual_stars = stars_tier
    is_announced_flow = bool((announced_gift_id or "").strip())
    normalized_gift_id = (announced_gift_id or "").strip()
    visual_sent = False

    if normalized_gift_id:
        # For NFT flow prefer slug->fragment .tgs, because bot catalog is unrelated to payer inventory.
        slug = resolve_nft_slug(normalized_gift_id)
        if slug:
            visual_sent = await send_visual_nft_by_slug(
                message,
                slug=slug,
                gift_id=normalized_gift_id,
            )

        if not visual_sent:
            try:
                gifts = await get_available_gifts(token)
                gift = pick_gift_by_id(gifts, normalized_gift_id)
            except Exception as err:
                logger.info("Fallback getAvailableGifts failed for gift_id=%s: %s", normalized_gift_id, err)
                gift = None

            if gift:
                gift_stars = gift.get("star_count")
                if isinstance(gift_stars, int):
                    actual_stars = gift_stars
            else:
                logger.warning("Visual unavailable for NFT gift_id=%s: tgs download failed and gift not found in catalog", normalized_gift_id)
    else:
        try:
            gifts = await get_available_gifts(token)
        except Exception as err:
            await message.reply_text(f"Помилка Telegram API (getAvailableGifts): {err}")
            return False
        gift, picked_stars = pick_gift_by_stars(gifts, stars_tier)
        if not gift or picked_stars is None:
            await message.reply_text(
                f"Немає доступного подарунка для категорії {stars_tier} Stars "
                f"(або нижчих 25/15, якщо дозволено fallback)."
            )
            return False
        actual_stars = picked_stars
        gift_id = gift.get("id")
        if gift_id is not None:
            normalized_gift_id = str(gift_id)

    if gift:
        sticker_file_id = get_gift_sticker_file_id(gift)
        if sticker_file_id:
            await send_visual_gift_message(
                message,
                context,
                sticker_file_id=sticker_file_id,
                actual_stars=actual_stars,
                gift_id=gift.get("id"),
            )
            visual_sent = True
        else:
            gift_id = gift.get("id")
            if is_announced_flow:
                slug = resolve_nft_slug(str(gift_id))
                if slug:
                    visual_sent = await send_visual_nft_by_slug(
                        message,
                        slug=slug,
                        gift_id=str(gift_id),
                    )
            if not visual_sent and is_announced_flow:
                logger.warning("Visual unavailable for NFT gift_id=%s: no sticker.file_id and tgs download failed", gift_id)
            elif not visual_sent:
                await message.reply_text(
                    f"Подарунок знайдено, але без sticker.file_id (gift_id={gift_id}). "
                    "Тому показую текстом: виграшний тип подарунка визначено."
                )
                return False

    claim_saved = True
    if persist_claim:
        claim_saved = save_successful_claim(
            chat_id=chat_id,
            user_id=user_id,
            winner_name=winner_name,
            attempt_no=attempt_no,
            day_key=day_key,
            month_key=month_key,
            stars=actual_stars,
        )
        if not claim_saved:
            logger.warning(
                "Claim already exists, skipping owner notify: chat_id=%s day_key=%s user_id=%s",
                chat_id,
                day_key,
                user_id,
            )

    if notify_owner and claim_saved:
        try:
            await notify_winner_to_owner(
                context,
                chat_id=chat_id,
                winner_name=winner_name,
                winner_username=winner_username,
                winner_user_id=user_id,
                attempt_no=attempt_no,
                stars_tier=actual_stars,
                gift=gift or {"id": normalized_gift_id},
            )
        except Exception as err:
            logger.exception("Failed to notify owner about winner: %s", err)

    if success_caption:
        await message.reply_text(success_caption)
    else:
        if is_announced_flow:
            nft_title = resolve_nft_title(normalized_gift_id) if normalized_gift_id else None
            await message.reply_text(
                f"✅ Є переможець: {winner_name}. "
                f"Спроба: {attempt_no}.\n"
                + (f"Випав приз: {nft_title} 🎉" if nft_title else "Випав приз 🎉")
            )
        else:
            await message.reply_text(
                f"✅ Є переможець: {winner_name}. "
                f"Спроба: {attempt_no}.\n"
                f"Випав приз за {actual_stars} Stars 🎉"
            )
    return True


async def on_teststicker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.type != "private":
        return
    if resolve_stats_owner_user_id() is None and os.getenv("STATS_OWNER_USER_ID", "").strip():
        await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
        return
    if not is_stats_owner(user.id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        await message.reply_text("BOT_TOKEN не налаштовано.")
        return

    test_nft_mode = bool(context.args) and context.args[0].strip().lower() == "nft"
    if test_nft_mode:
        nft_pool = [item.gift_id for item in parse_monthly_nft_pool()]
        if not nft_pool:
            await message.reply_text("NFT-пул порожній (MONTHLY_NFT_POOL).")
            return
        selected_id = random.choice(nft_pool)
        day_key, month_key = current_keys()
        await deliver_gift_like_win(
            message,
            context,
            token=token,
            chat_id=chat.id,
            user_id=user.id,
            winner_name=(user.full_name or "Переможець"),
            winner_username=user.username,
            attempt_no=1,
            day_key=day_key,
            month_key=month_key,
            stars_tier=0,
            persist_claim=False,
            notify_owner=False,
            announced_gift_id=selected_id,
        )
        return

    target_stars = random.choice([15, 25, 50])
    day_key, month_key = current_keys()
    await deliver_gift_like_win(
        message,
        context,
        token=token,
        chat_id=chat.id,
        user_id=user.id,
        winner_name=(user.full_name or "Переможець"),
        winner_username=user.username,
        attempt_no=1,
        day_key=day_key,
        month_key=month_key,
        stars_tier=target_stars,
        persist_claim=False,
        notify_owner=False,
    )


def pick_regular_stars_tier() -> int:
    bag: list[int] = []
    for stars, weight in REGULAR_PRIZE_WEIGHTS.items():
        if weight > 0:
            bag.extend([stars] * weight)
    if not bag:
        return 15
    return random.choice(bag)


def pick_gift_by_stars(
    gifts: list[dict[str, Any]],
    target_stars: int,
) -> tuple[dict[str, Any] | None, int | None]:
    # Prefer exact tier, then lower tier, so budget is never accidentally exceeded.
    for stars in [target_stars, 25, 15]:
        if stars > target_stars:
            continue
        candidates = [gift for gift in gifts if gift.get("star_count") == stars]
        if candidates:
            return random.choice(candidates), stars
    return None, None


def pick_gift_by_id(
    gifts: list[dict[str, Any]],
    target_gift_id: str,
) -> dict[str, Any] | None:
    normalized = target_gift_id.strip()
    if not normalized:
        return None
    for gift in gifts:
        gift_id = gift.get("id")
        if gift_id is None:
            continue
        if str(gift_id) == normalized:
            return gift
    return None


async def on_slot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not message.dice or not chat or not user:
        return

    if message.dice.emoji != "🎰":
        return

    now_local = datetime.now(APP_TZ)
    day_key, month_key = now_local.strftime("%Y-%m-%d"), now_local.strftime("%Y-%m")
    chat_id = chat.id
    user_id = user.id

    # In groups: whitelist only. In private: only STATS_OWNER_USER_ID.
    if not is_slot_allowed_context(chat.type, chat_id, user_id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN is missing")
        return

    winner_name = user.full_name or "Переможець"
    winner_username = user.username
    daily_limit_text = DEFAULT_DAILY_LIMIT_TEXT
    daily_prize_limit = get_daily_prize_limit()

    async with CLAIM_LOCK:
        current_attempt_no = increment_daily_attempt(chat_id, day_key)

        claims_today = count_daily_claims(chat_id, day_key)
        if claims_today >= daily_prize_limit:
            claim = get_daily_claim(chat_id, day_key)
            if claim:
                winner_label = claim.winner_name or f"ID {claim.user_id}"
                await message.reply_text(
                    f"{daily_limit_text}\nЛіміт на сьогодні: {daily_prize_limit}. "
                    f"Вже видано: {claims_today}.\n"
                    f"Останній переможець: {winner_label} (спроба {claim.attempt_no})."
                )
            else:
                await message.reply_text(
                    f"{daily_limit_text}\nЛіміт на сьогодні: {daily_prize_limit}. Вже видано: {claims_today}."
                )
            return

        if not is_winning_slot_value(message.dice.value):
            return

        if is_valentine_day(now_local):
            try:
                gifts = await get_available_gifts(token)
                preferred_gift_id = pick_preferred_50_gift_id(gifts, valentine_50_gift_ids())
                await deliver_gift_like_win(
                    message,
                    context,
                    token=token,
                    chat_id=chat_id,
                    user_id=user_id,
                    winner_name=winner_name,
                    winner_username=winner_username,
                    attempt_no=current_attempt_no,
                    day_key=day_key,
                    month_key=month_key,
                    stars_tier=50,
                    persist_claim=True,
                    notify_owner=True,
                    announced_gift_id=preferred_gift_id,
                )
                return
            except Exception as err:
                logger.exception("Failed to send Valentine gift: %s", err)
                await message.reply_text(f"Помилка під час обробки Valentine-подарунка: {err}")
                return

        selected_nft_gift_id, nft_left, draws_left, nft_probability = pick_monthly_nft_gift(month_key)
        if selected_nft_gift_id:
            logger.info(
                "NFT prize selected: chat_id=%s user_id=%s month=%s gift_id=%s nft_left=%s draws_left=%s p=%.4f",
                chat_id,
                user_id,
                month_key,
                selected_nft_gift_id,
                nft_left,
                draws_left,
                nft_probability,
            )
            try:
                delivered = await deliver_gift_like_win(
                    message,
                    context,
                    token=token,
                    chat_id=chat_id,
                    user_id=user_id,
                    winner_name=winner_name,
                    winner_username=winner_username,
                    attempt_no=current_attempt_no,
                    day_key=day_key,
                    month_key=month_key,
                    stars_tier=0,
                    persist_claim=True,
                    notify_owner=True,
                    announced_gift_id=selected_nft_gift_id,
                )
                if delivered:
                    saved = save_monthly_nft_claim(
                        month_key=month_key,
                        gift_id=selected_nft_gift_id,
                        chat_id=chat_id,
                        day_key=day_key,
                        user_id=user_id,
                        attempt_no=current_attempt_no,
                    )
                    if not saved:
                        logger.warning(
                            "NFT claim was not saved (already exists): month=%s gift_id=%s",
                            month_key,
                            selected_nft_gift_id,
                        )
                return
            except Exception as err:
                logger.exception("Failed to send NFT prize: %s", err)
                await message.reply_text(f"Помилка під час обробки NFT-призу: {err}")
                return

        logger.info(
            "NFT not selected: chat_id=%s user_id=%s month=%s nft_left=%s draws_left=%s p=%.4f",
            chat_id,
            user_id,
            month_key,
            nft_left,
            draws_left,
            nft_probability,
        )

        stars_tier = pick_regular_stars_tier()

        try:
            await deliver_gift_like_win(
                message,
                context,
                token=token,
                chat_id=chat_id,
                user_id=user_id,
                winner_name=winner_name,
                winner_username=winner_username,
                attempt_no=current_attempt_no,
                day_key=day_key,
                month_key=month_key,
                stars_tier=stars_tier,
                persist_claim=True,
                notify_owner=True,
            )
        except Exception as err:
            logger.exception("Failed to send gift: %s", err)
            await message.reply_text(f"Помилка під час обробки подарунка: {err}")


def build_app() -> Application:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    init_db()
    app = Application.builder().token(token).post_init(on_startup).build()
    app.add_handler(TypeHandler(Update, on_any_update, block=False), group=-1)
    app.add_handler(CommandHandler("ids", on_ids))
    app.add_handler(CommandHandler("stats", on_stats, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("dailylimit", on_dailylimit, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("topup", on_topup, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("buy", on_buy, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("teststicker", on_teststicker, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("resetday", on_resetday, filters=filters.ChatType.PRIVATE))
    app.add_handler(PreCheckoutQueryHandler(on_precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_successful_payment))
    app.add_handler(
        MessageHandler(
            (filters.ChatType.GROUPS | filters.ChatType.PRIVATE) & filters.Dice.SLOT_MACHINE,
            on_slot,
        )
    )
    return app


if __name__ == "__main__":
    application = build_app()
    logger.info("Bot started")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
