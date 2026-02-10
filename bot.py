import json
import logging
import os
import random
import sqlite3
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, TypeHandler, filters

load_dotenv()


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# Monthly quotas per group for up to 31 gifts: 3x50, 6x25, 22x15.
MONTHLY_QUOTAS = {
    50: 3,
    25: 6,
    15: 22,
}

DEFAULT_FALLBACK_TEXT = "Сьогодні не вдалося надіслати подарунок, але удача на твоєму боці."
DEFAULT_DAILY_LIMIT_TEXT = "Сьогоднішній ліміт уже вичерпано: у цій групі вже видали 1 подарунок."
DEFAULT_MONTHLY_LIMIT_TEXT = "Місячний ліміт подарунків для цієї групи вже вичерпано."
DEFAULT_DAILY_NOTICE_COOLDOWN_MIN = 10
DEFAULT_AUTO_TOPUP_THRESHOLD = 100
DEFAULT_AUTO_TOPUP_AMOUNT = 615
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

    def remaining_units(self) -> list[int]:
        left_50 = max(0, MONTHLY_QUOTAS[50] - self.sent_50)
        left_25 = max(0, MONTHLY_QUOTAS[25] - self.sent_25)
        left_15 = max(0, MONTHLY_QUOTAS[15] - self.sent_15)
        return [50] * left_50 + [25] * left_25 + [15] * left_15


@dataclass
class DailyClaim:
    user_id: int
    winner_name: str
    attempt_no: int


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
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, day_key)
            )
            """
        )
        # Lightweight migration for existing DBs created before winner_name/attempt_no fields.
        cols = [row[1] for row in conn.execute("PRAGMA table_info(daily_claims)").fetchall()]
        if "winner_name" not in cols:
            conn.execute("ALTER TABLE daily_claims ADD COLUMN winner_name TEXT NOT NULL DEFAULT ''")
        if "attempt_no" not in cols:
            conn.execute("ALTER TABLE daily_claims ADD COLUMN attempt_no INTEGER NOT NULL DEFAULT 0")
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
        conn.commit()


def current_keys() -> tuple[str, str]:
    now = datetime.now(APP_TZ)
    return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m")


def has_daily_claim(chat_id: int, day_key: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM daily_claims WHERE chat_id = ? AND day_key = ?",
            (chat_id, day_key),
        ).fetchone()
    return row is not None


def get_daily_claim(chat_id: int, day_key: str) -> DailyClaim | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT user_id, winner_name, attempt_no
            FROM daily_claims
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
    if not row:
        return None
    return DailyClaim(user_id=row[0], winner_name=row[1], attempt_no=row[2])


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


def resolve_business_connection_id() -> str | None:
    env_value = os.getenv("BUSINESS_CONNECTION_ID")
    if env_value:
        return env_value
    return get_setting("business_connection_id")


def save_successful_claim(
    chat_id: int,
    user_id: int,
    winner_name: str,
    attempt_no: int,
    day_key: str,
    month_key: str,
    stars: int,
) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO daily_claims (chat_id, day_key, user_id, winner_name, attempt_no, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (chat_id, day_key, user_id, winner_name, attempt_no, datetime.now(timezone.utc).isoformat()),
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
        else:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_15 = sent_15 + 1
                WHERE chat_id = ? AND month_key = ?
                """,
                (chat_id, month_key),
            )
        conn.commit()


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


async def send_gift(token: str, user_id: int, gift_id: str, text: str) -> dict[str, Any]:
    payload = {
        "user_id": user_id,
        "gift_id": gift_id,
        "text": text,
    }
    return await asyncio.to_thread(_api_call, token, "sendGift", payload)


async def get_my_star_balance(token: str) -> int:
    result = await asyncio.to_thread(_api_call, token, "getMyStarBalance")
    amount = result.get("amount")
    if isinstance(amount, int):
        return amount
    return 0


async def transfer_business_account_stars(token: str, business_connection_id: str, star_count: int) -> None:
    payload = {
        "business_connection_id": business_connection_id,
        "star_count": star_count,
    }
    await asyncio.to_thread(_api_call, token, "transferBusinessAccountStars", payload)


async def maybe_auto_topup(token: str) -> None:
    if not parse_bool_env("AUTO_TOPUP_ENABLED", True):
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
        return

    business_connection_id = resolve_business_connection_id()
    if not business_connection_id:
        logger.warning("Auto top-up skipped: BUSINESS_CONNECTION_ID is unknown")
        return

    await transfer_business_account_stars(token, business_connection_id, transfer_amount)
    logger.info(
        "Auto top-up completed: transferred %s Stars, previous balance was %s",
        transfer_amount,
        balance,
    )


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

    owner_id_raw = os.getenv("STATS_OWNER_USER_ID", "").strip()
    if owner_id_raw:
        try:
            owner_id = int(owner_id_raw)
        except ValueError:
            await message.reply_text("STATS_OWNER_USER_ID має бути числом.")
            return
        if user.id != owner_id:
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

    try:
        balance = await get_my_star_balance(token)
        balance_text = str(balance)
    except Exception as err:
        balance_text = f"помилка: {err}"

    lines = [
        "Статус бота",
        f"- Баланс бота: {balance_text} Stars",
        f"- AUTO_TOPUP_ENABLED: {1 if auto_enabled else 0}",
        f"- AUTO_TOPUP_THRESHOLD: {threshold}",
        f"- AUTO_TOPUP_AMOUNT: {amount}",
        f"- business_connection_id: {business_connection_id}",
        f"- Місяць: {month_key}",
        f"- День: {day_key}",
    ]

    if not allowed:
        lines.append("- ALLOWED_CHAT_IDS: порожній (бот ніде не працює)")
    else:
        lines.append("- Групи у whitelist:")
        for chat_id in allowed:
            month_state = get_month_state(chat_id, month_key)
            left_50 = max(0, MONTHLY_QUOTAS[50] - month_state.sent_50)
            left_25 = max(0, MONTHLY_QUOTAS[25] - month_state.sent_25)
            left_15 = max(0, MONTHLY_QUOTAS[15] - month_state.sent_15)
            attempts_today = get_daily_attempt(chat_id, day_key)
            claim = get_daily_claim(chat_id, day_key)
            if claim:
                winner_text = f"{claim.winner_name or f'ID {claim.user_id}'} (спроба #{claim.attempt_no})"
            else:
                winner_text = "ще не було"
            lines.append(
                f"  {chat_id}: залишок 50/25/15 = {left_50}/{left_25}/{left_15}, "
                f"спроб сьогодні = {attempts_today}, переможець = {winner_text}"
            )

    await message.reply_text("\n".join(lines))


def pick_stars_by_remaining(state: GroupMonthState) -> int | None:
    bag = state.remaining_units()
    if not bag:
        return None
    return random.choice(bag)


def pick_gift_by_stars(gifts: list[dict[str, Any]], target_stars: int) -> tuple[dict[str, Any] | None, int | None]:
    # Prefer exact tier, then lower tier, so budget is never accidentally exceeded.
    for stars in [target_stars, 25, 15]:
        if stars > target_stars:
            continue
        candidates = [gift for gift in gifts if gift.get("star_count") == stars]
        if candidates:
            return random.choice(candidates), stars
    return None, None


async def on_slot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not message.dice or not chat or not user:
        return

    if message.dice.emoji != "🎰":
        return

    day_key, month_key = current_keys()
    chat_id = chat.id
    user_id = user.id

    # Additional guard: work only in explicitly whitelisted groups.
    if not is_chat_allowed(chat_id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN is missing")
        return

    fallback_text = os.getenv("FALLBACK_TEXT", DEFAULT_FALLBACK_TEXT)
    winner_name = user.full_name or "Переможець"
    daily_limit_text = os.getenv("DAILY_LIMIT_TEXT", DEFAULT_DAILY_LIMIT_TEXT)
    monthly_limit_text = os.getenv("MONTHLY_LIMIT_TEXT", DEFAULT_MONTHLY_LIMIT_TEXT)
    cooldown_min_raw = os.getenv("DAILY_NOTICE_COOLDOWN_MIN", str(DEFAULT_DAILY_NOTICE_COOLDOWN_MIN))
    try:
        daily_notice_cooldown_min = max(1, int(cooldown_min_raw))
    except ValueError:
        daily_notice_cooldown_min = DEFAULT_DAILY_NOTICE_COOLDOWN_MIN

    async with CLAIM_LOCK:
        current_attempt_no = increment_daily_attempt(chat_id, day_key)

        if has_daily_claim(chat_id, day_key):
            if should_send_daily_limit_notice(chat_id, day_key, daily_notice_cooldown_min):
                mark_daily_limit_notice(chat_id, day_key)
                claim = get_daily_claim(chat_id, day_key)
                if claim:
                    winner_label = claim.winner_name or f"ID {claim.user_id}"
                    await message.reply_text(
                        f"{daily_limit_text}\nСьогодні виграв(ла): {winner_label} (спроба #{claim.attempt_no})."
                    )
                else:
                    await message.reply_text(daily_limit_text)
            return

        if message.dice.value != 64:
            return

        month_state = get_month_state(chat_id, month_key)
        stars_tier = pick_stars_by_remaining(month_state)
        if stars_tier is None:
            await message.reply_text(monthly_limit_text)
            return

        try:
            await maybe_auto_topup(token)
            gifts = await get_available_gifts(token)
            gift, actual_stars = pick_gift_by_stars(gifts, stars_tier)
            if not gift or actual_stars is None:
                await message.reply_text(fallback_text)
                return

            gift_id = gift.get("id")
            if not gift_id:
                await message.reply_text(fallback_text)
                return

            congrats_text = f"{winner_name}, виграш за 777: подарунок на {actual_stars} Stars."
            await send_gift(token, user_id, gift_id, congrats_text)
            await maybe_auto_topup(token)
            save_successful_claim(
                chat_id=chat_id,
                user_id=user_id,
                winner_name=winner_name,
                attempt_no=current_attempt_no,
                day_key=day_key,
                month_key=month_key,
                stars=actual_stars,
            )
            await message.reply_text(
                f"Подарунок надіслано: {actual_stars} Stars.\n"
                f"Сьогодні вибив(ла) {winner_name} зі спроби #{current_attempt_no}."
            )
        except Exception as err:
            logger.exception("Failed to send gift: %s", err)
            await message.reply_text(fallback_text)


def build_app() -> Application:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    init_db()
    app = Application.builder().token(token).post_init(on_startup).build()
    app.add_handler(TypeHandler(Update, on_any_update, block=False), group=-1)
    app.add_handler(CommandHandler("stats", on_stats, filters=filters.ChatType.PRIVATE))
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & filters.Dice.SLOT_MACHINE,
            on_slot,
        )
    )
    return app


if __name__ == "__main__":
    application = build_app()
    logger.info("Bot started")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
