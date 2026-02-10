import json
import logging
import os
import random
import sqlite3
import asyncio
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv
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


# Monthly quotas per group for up to 31 gifts: 3x50, 6x25, 22x15.
MONTHLY_QUOTAS = {
    50: 3,
    25: 6,
    15: 22,
}

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
    claimed_stars: int


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
            SELECT user_id, winner_name, attempt_no, claimed_stars
            FROM daily_claims
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
    if not row:
        return None
    return DailyClaim(user_id=row[0], winner_name=row[1], attempt_no=row[2], claimed_stars=row[3])


def reset_daily_claim(chat_id: int, day_key: str, month_key: str) -> tuple[bool, int]:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT claimed_stars
            FROM daily_claims
            WHERE chat_id = ? AND day_key = ?
            """,
            (chat_id, day_key),
        ).fetchone()
        if not row:
            return False, 0

        claimed_stars = row[0] if isinstance(row[0], int) else 0
        conn.execute(
            "DELETE FROM daily_claims WHERE chat_id = ? AND day_key = ?",
            (chat_id, day_key),
        )
        conn.execute(
            "DELETE FROM daily_limit_notices WHERE chat_id = ? AND day_key = ?",
            (chat_id, day_key),
        )

        if claimed_stars == 50:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_50 = CASE WHEN sent_50 > 0 THEN sent_50 - 1 ELSE 0 END
                WHERE chat_id = ? AND month_key = ?
                """,
                (chat_id, month_key),
            )
        elif claimed_stars == 25:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_25 = CASE WHEN sent_25 > 0 THEN sent_25 - 1 ELSE 0 END
                WHERE chat_id = ? AND month_key = ?
                """,
                (chat_id, month_key),
            )
        elif claimed_stars == 15:
            conn.execute(
                """
                UPDATE monthly_claims
                SET sent_15 = CASE WHEN sent_15 > 0 THEN sent_15 - 1 ELSE 0 END
                WHERE chat_id = ? AND month_key = ?
                """,
                (chat_id, month_key),
            )

        conn.commit()
    return True, claimed_stars


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


def set_topup_status(status: str, details: str = "") -> None:
    set_setting("topup_status", status)
    set_setting("topup_details", details[:1000])


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
) -> None:
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
        f"- AUTO_TOPUP_ENABLED: {1 if auto_enabled else 0}",
        f"- AUTO_TOPUP_THRESHOLD: {threshold}",
        f"- AUTO_TOPUP_AMOUNT: {amount}",
        f"- business_connection_id: {business_connection_id}",
        f"- TopUp status: {topup_status}",
        f"- TopUp details: {topup_details}",
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

    removed, claimed_stars = reset_daily_claim(target_chat_id, target_day_key, target_month_key)
    if not removed:
        await message.reply_text(f"На {target_day_key} для чату {target_chat_id} прапорця переможця не було.")
        return

    await message.reply_text(
        f"Скинуто денний прапорець для чату {target_chat_id} на {target_day_key}.\n"
        f"Місячний лічильник відкотили на категорію: {claimed_stars} Stars."
    )


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
    winner_user_id: int,
    attempt_no: int,
    stars_tier: int,
    gift: dict[str, Any],
) -> None:
    if not parse_bool_env("WINNER_NOTIFY_ENABLED", True):
        return

    owner_id = resolve_stats_owner_user_id()
    if owner_id is None:
        return

    gift_id = gift.get("id")
    sellable = 1 if is_sellable_gift(gift) else 0
    user_link = f"tg://user?id={winner_user_id}"
    day_key, month_key = current_keys()
    text = (
        "Переможець дня\n"
        f"- Група: {chat_id}\n"
        f"- Користувач: {winner_name}\n"
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


async def deliver_gift_like_win(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    token: str,
    chat_id: int,
    user_id: int,
    winner_name: str,
    attempt_no: int,
    day_key: str,
    month_key: str,
    stars_tier: int,
    persist_claim: bool,
    notify_owner: bool,
) -> bool:
    try:
        gifts = await get_available_gifts(token)
    except Exception as err:
        await message.reply_text(f"Помилка Telegram API (getAvailableGifts): {err}")
        return False

    gift, actual_stars = pick_gift_by_stars(gifts, stars_tier)
    if not gift or actual_stars is None:
        await message.reply_text(
            f"Немає доступного подарунка для категорії {stars_tier} Stars "
            f"(або нижчих 25/15, якщо дозволено fallback)."
        )
        return False

    sticker_file_id = get_gift_sticker_file_id(gift)
    if not sticker_file_id:
        gift_id = gift.get("id")
        await message.reply_text(
            f"Подарунок знайдено, але без sticker.file_id (gift_id={gift_id}). "
            "Тому показую текстом: виграшний тип подарунка визначено."
        )
        return False

    await send_visual_gift_message(
        message,
        context,
        sticker_file_id=sticker_file_id,
        actual_stars=actual_stars,
        gift_id=gift.get("id"),
    )

    if persist_claim:
        save_successful_claim(
            chat_id=chat_id,
            user_id=user_id,
            winner_name=winner_name,
            attempt_no=attempt_no,
            day_key=day_key,
            month_key=month_key,
            stars=actual_stars,
        )

    if notify_owner:
        try:
            await notify_winner_to_owner(
                context,
                chat_id=chat_id,
                winner_name=winner_name,
                winner_user_id=user_id,
                attempt_no=attempt_no,
                stars_tier=actual_stars,
                gift=gift,
            )
        except Exception as err:
            logger.exception("Failed to notify owner about winner: %s", err)

    await message.reply_text(
        f"Візуальний подарунок показано в чаті: {actual_stars} Stars.\n"
        f"Сьогодні вибив(ла) {winner_name} зі спроби #{attempt_no}."
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

    target_stars = random.choice([15, 25, 50])
    day_key, month_key = current_keys()
    await deliver_gift_like_win(
        message,
        context,
        token=token,
        chat_id=chat.id,
        user_id=user.id,
        winner_name=(user.full_name or "Переможець"),
        attempt_no=1,
        day_key=day_key,
        month_key=month_key,
        stars_tier=target_stars,
        persist_claim=False,
        notify_owner=False,
    )


def pick_stars_by_remaining(state: GroupMonthState) -> int | None:
    bag = state.remaining_units()
    if not bag:
        return None
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

    # In groups: whitelist only. In private: only STATS_OWNER_USER_ID.
    if not is_slot_allowed_context(chat.type, chat_id, user_id):
        return

    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN is missing")
        return

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
            await deliver_gift_like_win(
                message,
                context,
                token=token,
                chat_id=chat_id,
                user_id=user_id,
                winner_name=winner_name,
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
    app.add_handler(CommandHandler("stats", on_stats, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("topup", on_topup, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("buy", on_buy, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("gifts", on_gifts, filters=filters.ChatType.PRIVATE))
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
