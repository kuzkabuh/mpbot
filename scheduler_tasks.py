"""
Версия файла: 1.3.1
Описание: Планировщик задач (уведомления/отчеты/остатки) для Telegram-бота аналитики WB/Ozon.
Дата изменения: 2026-01-22
Изменения:
- Приведено в полную совместимость с обновленным db_functions.py:
  * is_order_new(..., user_tg_id=...) учитывает пользователя (tg_id) и предотвращает коллизии
  * bulk_save_orders() сохраняет дату в Order.order_date, поэтому используем ключ order_date
- Удалены TypeError-fallback блоки (они больше не нужны и маскируют реальные ошибки).
- Упрощена логика дедупликации: if not await is_order_new(...): continue
- Пакетное сохранение заказов (bulk_save_orders) оставлено для снижения нагрузки на БД.
- Улучшена стабильность: проверки типов, безопасные парсеры, безопасная нарезка сообщений > 4096.
- Уважение notifications_enabled во всех уведомляющих задачах.
"""

from __future__ import annotations

import asyncio
import html
import logging
from datetime import datetime, timedelta
from typing import Any, List, Tuple

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from sqlalchemy import select

from database import async_session, User
from ozon_api import OzonAPI
from wb_api import WildberriesAPI

import db_functions as dbf
import reports

logger = logging.getLogger(__name__)

# Ограничитель одновременных отправок/запросов (защита от перегрузки и Flood Limit)
SEM_LIMIT = 5
sem = asyncio.Semaphore(SEM_LIMIT)

TELEGRAM_TEXT_LIMIT = 4096
DEFAULT_SLEEP_BETWEEN_SEND = 0.05  # 50ms


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def _safe_str(value: Any, max_len: int = 255, default: str = "Н/Д") -> str:
    """Безопасное приведение к строке."""
    s = str(value).strip() if value is not None else default
    if not s:
        s = default
    return s[:max_len]


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Безопасное приведение к float."""
    try:
        if value is None:
            return default
        if isinstance(value, str):
            v = value.strip().replace(" ", "").replace(",", ".")
            if v == "":
                return default
            return float(v)
        return float(value)
    except Exception:
        return default


def _wb_price_to_rub(value: Any) -> float:
    """
    WB может отдавать цену:
    - в копейках (int)
    - в рублях (float)
    - строкой

    Логика:
    - если значение похоже на "копейки" (большое число), делим на 100
    - иначе считаем рублями
    """
    v = _safe_float(value, 0.0)
    if v <= 0:
        return 0.0

    # Частый кейс WB: цена в копейках (например 129900 = 1299.00)
    # Порог 50000: уменьшает риск неверного деления для "рублевых" значений.
    if v >= 50000:
        return v / 100.0

    return v


def _split_long_message(text: str, limit: int = TELEGRAM_TEXT_LIMIT) -> List[str]:
    """Разбивает длинное сообщение на части, пытается резать по границам строк."""
    if not text:
        return [""]

    if len(text) <= limit:
        return [text]

    parts: List[str] = []
    chunk = ""
    for line in text.splitlines(keepends=True):
        if len(chunk) + len(line) <= limit:
            chunk += line
        else:
            if chunk:
                parts.append(chunk)
            while len(line) > limit:
                parts.append(line[:limit])
                line = line[limit:]
            chunk = line

    if chunk:
        parts.append(chunk)

    return parts


async def safe_send_message(
    bot: Bot,
    chat_id: int,
    text: str,
    parse_mode: str = "HTML",
    _attempt: int = 1,
    _max_attempts: int = 5,
) -> None:
    """
    Безопасная отправка сообщений:
    - semaphore
    - backoff на TelegramRetryAfter
    - защита от бесконечной рекурсии
    - разбиение >4096
    """
    if not text:
        return

    parts = _split_long_message(text, TELEGRAM_TEXT_LIMIT)

    async with sem:
        for part in parts:
            try:
                await bot.send_message(chat_id, part, parse_mode=parse_mode)
                await asyncio.sleep(DEFAULT_SLEEP_BETWEEN_SEND)
            except TelegramRetryAfter as e:
                retry_after = int(getattr(e, "retry_after", 1) or 1)
                logger.warning(f"Flood limit: sleep {retry_after}s (user={chat_id}, attempt={_attempt})")
                if _attempt >= _max_attempts:
                    logger.error(f"Flood limit: max attempts reached (user={chat_id})")
                    return
                await asyncio.sleep(retry_after)
                return await safe_send_message(
                    bot=bot,
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode,
                    _attempt=_attempt + 1,
                    _max_attempts=_max_attempts,
                )
            except TelegramForbiddenError:
                logger.info(f"Бот заблокирован пользователем {chat_id}. Пропускаем отправку.")
                return
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения пользователю {chat_id}: {e}")
                return


async def _load_users_for_tasks() -> List[User]:
    """Загружает пользователей для фоновых задач."""
    async with async_session() as session:
        try:
            res = await session.execute(select(User))
            return list(res.scalars().all())
        except Exception as e:
            logger.error(f"Ошибка загрузки пользователей: {e}")
            return []


def _notifications_enabled(user: User) -> bool:
    """Проверяет флаг уведомлений пользователя."""
    return bool(getattr(user, "notifications_enabled", True))


# =============================================================================
# ФОНОВЫЕ ЗАДАЧИ
# =============================================================================

async def check_new_orders_task(bot: Bot) -> None:
    """
    Периодическая проверка новых заказов на маркетплейсах.
    """
    users = await _load_users_for_tasks()
    if not users:
        return

    for user in users:
        if not _notifications_enabled(user):
            continue

        tasks = []

        wb_token = (user.wb_token or "").strip()
        if wb_token:
            tasks.append(_process_wb_orders(bot, user))

        ozon_client_id = (user.ozon_client_id or "").strip()
        ozon_api_key = (user.ozon_api_key or "").strip()
        if ozon_client_id and ozon_api_key:
            tasks.append(_process_ozon_orders(bot, user))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _process_wb_orders(bot: Bot, user: User) -> None:
    """Обработка WB заказов (FBS+FBO)."""
    try:
        token = (user.wb_token or "").strip()
        if not token:
            return

        wb = WildberriesAPI(token)
        all_wb = await wb.get_all_orders(days=1)
        if not isinstance(all_wb, dict):
            logger.error(f"WB API: некорректный формат (ожидался dict) user={user.tg_id}")
            return

        to_save: List[dict] = []

        # -------------------------
        # FBS
        # -------------------------
        fbs_list = all_wb.get("fbs", [])
        if isinstance(fbs_list, list):
            for order in fbs_list:
                if not isinstance(order, dict):
                    continue

                order_id = _safe_str(order.get("id"), max_len=128, default="")
                if not order_id:
                    continue

                # ВАЖНО: дедупликация с учетом пользователя
                if not await dbf.is_order_new(order_id, "wb", user_tg_id=user.tg_id):
                    continue

                article_raw = order.get("article") or order.get("nmId") or order.get("supplierArticle") or "Н/Д"
                article_msg = html.escape(_safe_str(article_raw, max_len=128, default="Н/Д"))

                raw_price = order.get("convertedPrice")
                if raw_price is None:
                    raw_price = order.get("price") or order.get("totalPrice") or 0

                price = _wb_price_to_rub(raw_price)

                msg = (
                    f"🚀 <b>Новый заказ Wildberries (FBS)!</b>\n\n"
                    f"📦 Номер: <code>{html.escape(order_id)}</code>\n"
                    f"🔢 Артикул: <code>{article_msg}</code>\n"
                    f"💰 К оплате: <b>{price:,.2f} ₽</b>"
                )
                await safe_send_message(bot, user.tg_id, msg)

                to_save.append(
                    {
                        "order_id": order_id,
                        "marketplace": "wb",
                        "amount": price,
                        "item_name": _safe_str(article_raw, 255, "Н/Д"),
                        "user_id": user.tg_id,
                        "order_date": datetime.now(),
                    }
                )

        # -------------------------
        # FBO
        # -------------------------
        fbo_list = all_wb.get("fbo", [])
        if isinstance(fbo_list, list):
            for order in fbo_list:
                if not isinstance(order, dict):
                    continue

                order_id = _safe_str(order.get("gNumber") or order.get("orderId"), max_len=128, default="")
                if not order_id:
                    continue

                if not await dbf.is_order_new(order_id, "wb", user_tg_id=user.tg_id):
                    continue

                article_raw = order.get("supplierArticle") or order.get("nmId") or order.get("article") or "Н/Д"
                article_msg = html.escape(_safe_str(article_raw, max_len=128, default="Н/Д"))

                price = _safe_float(order.get("totalPrice"), 0.0)

                msg = (
                    f"📦 <b>Продажа Wildberries (FBO)!</b>\n\n"
                    f"📦 Номер: <code>{html.escape(order_id)}</code>\n"
                    f"🔢 Артикул: <code>{article_msg}</code>\n"
                    f"💰 Сумма: <b>{price:,.2f} ₽</b>"
                )
                await safe_send_message(bot, user.tg_id, msg)

                to_save.append(
                    {
                        "order_id": order_id,
                        "marketplace": "wb",
                        "amount": price,
                        "item_name": _safe_str(article_raw, 255, "Н/Д"),
                        "user_id": user.tg_id,
                        "order_date": datetime.now(),
                    }
                )

        if to_save:
            await dbf.bulk_save_orders(to_save)

    except Exception as e:
        logger.error(f"WB task error (user={user.tg_id}): {e}")


async def _process_ozon_orders(bot: Bot, user: User) -> None:
    """
    Обработка Ozon заказов.

    Ожидается, что ozon_api.get_all_orders(days=1) возвращает dict:
      {
        "fbs": [
           {"order_id": "...", "article": "...", "name": "...", "price": 123.45, "date": "..."},
           ...
        ],
        "fbo": [...]
      }
    """
    try:
        client_id = (user.ozon_client_id or "").strip()
        api_key = (user.ozon_api_key or "").strip()
        if not client_id or not api_key:
            return

        ozon = OzonAPI(client_id, api_key)
        all_ozon = await ozon.get_all_orders(days=1)
        if not isinstance(all_ozon, dict):
            logger.error(f"Ozon API: некорректный формат (ожидался dict) user={user.tg_id}")
            return

        to_save: List[dict] = []

        fbs_orders = all_ozon.get("fbs", [])
        if isinstance(fbs_orders, list):
            for o in fbs_orders:
                if not isinstance(o, dict):
                    continue

                order_id = _safe_str(o.get("order_id"), max_len=128, default="")
                if not order_id:
                    continue

                if not await dbf.is_order_new(order_id, "ozon", user_tg_id=user.tg_id):
                    continue

                article_raw = o.get("article") or "Н/Д"
                name_raw = o.get("name") or "Товар"
                price = _safe_float(o.get("price"), 0.0)

                msg = (
                    f"🚀 <b>Новый заказ Ozon (FBS)!</b>\n\n"
                    f"📦 Номер: <code>{html.escape(order_id)}</code>\n"
                    f"🔢 Артикул: <code>{html.escape(_safe_str(article_raw, 128, 'Н/Д'))}</code>\n"
                    f"📦 Товар: <b>{html.escape(_safe_str(name_raw, 180, 'Товар'))}</b>\n"
                    f"💰 Сумма: <b>{price:,.2f} ₽</b>"
                )
                await safe_send_message(bot, user.tg_id, msg)

                to_save.append(
                    {
                        "order_id": order_id,
                        "marketplace": "ozon",
                        "amount": price,
                        "item_name": _safe_str(article_raw, 255, "Н/Д"),
                        "user_id": user.tg_id,
                        "order_date": datetime.now(),
                    }
                )

        # Если позже добавишь FBO для Ozon — обработай all_ozon["fbo"] аналогично.

        if to_save:
            await dbf.bulk_save_orders(to_save)

    except Exception as e:
        logger.error(f"Ozon task error (user={user.tg_id}): {e}")


async def send_morning_report(bot: Bot) -> None:
    """Рассылка финансовых итогов за прошедшие сутки (по расписанию)."""
    users = await _load_users_for_tasks()
    if not users:
        return

    yesterday = datetime.now() - timedelta(days=1)
    date_human = yesterday.strftime("%d.%m.%Y")
    date_iso = yesterday.strftime("%Y-%m-%d")

    for user in users:
        if not _notifications_enabled(user):
            continue

        report_parts: List[str] = [f"🌅 <b>Отчет за {date_human}</b>\n"]
        has_data = False

        # WB
        wb_token = (user.wb_token or "").strip()
        if wb_token:
            try:
                wb = WildberriesAPI(wb_token)
                sales = await wb.get_sales_report(days=1)

                if isinstance(sales, list) and sales:
                    balance = await wb.get_balance()
                    bal_val = balance if isinstance(balance, (int, float)) else 0.0

                    report_wb = await reports.generate_daily_report_text(
                        "Wildberries",
                        sales,
                        user_tg_id=user.tg_id,
                        balance=bal_val,
                    )
                    report_parts.append(report_wb)
                    has_data = True
            except Exception as e:
                logger.error(f"Ошибка утреннего отчета WB (user={user.tg_id}): {e}")

        # OZON
        ozon_client_id = (user.ozon_client_id or "").strip()
        ozon_api_key = (user.ozon_api_key or "").strip()
        if ozon_client_id and ozon_api_key:
            try:
                ozon = OzonAPI(ozon_client_id, ozon_api_key)
                stats = await ozon.get_daily_stats(date_iso)

                if stats and (isinstance(stats, list) or isinstance(stats, dict)):
                    balance = await ozon.get_balance()
                    bal_val = balance if isinstance(balance, (int, float)) else 0.0

                    report_ozon = await reports.generate_daily_report_text(
                        "Ozon",
                        stats,
                        user_tg_id=user.tg_id,
                        balance=bal_val,
                    )
                    report_parts.append(report_ozon)
                    has_data = True
            except Exception as e:
                logger.error(f"Ошибка утреннего отчета Ozon (user={user.tg_id}): {e}")

        if has_data:
            await safe_send_message(bot, user.tg_id, "\n\n".join(report_parts))


async def check_low_stock_task(bot: Bot) -> None:
    """Проверка остатков и уведомления при остатке <= threshold."""
    users = await _load_users_for_tasks()
    if not users:
        return

    for user in users:
        if not _notifications_enabled(user):
            continue

        threshold = getattr(user, "stock_threshold", 5) or 5
        try:
            threshold = int(threshold)
        except Exception:
            threshold = 5

        sources: List[Tuple[str, Any, List[Any]]] = [
            ("Wildberries", WildberriesAPI, [(user.wb_token or "").strip()]),
            ("Ozon", OzonAPI, [(user.ozon_client_id or "").strip(), (user.ozon_api_key or "").strip()]),
        ]

        for mp_name, api_class, args in sources:
            if not all(args):
                continue

            try:
                api = api_class(*args)
                stocks = await api.get_stock_info()

                if stocks and isinstance(stocks, list):
                    report_text = await reports.generate_stock_report(mp_name, stocks, threshold=threshold)
                    if report_text:
                        await safe_send_message(bot, user.tg_id, report_text)

            except Exception as e:
                logger.error(f"Ошибка проверки остатков {mp_name} (user={user.tg_id}): {e}")
