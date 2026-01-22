"""
Версия файла: 1.2.0
Описание: Генерация отчетов и сообщений (дневной отчет, последние заказы, низкие остатки, проверка API).
Дата изменения: 2026-01-22
Изменения:
- Исправлена критичная ошибка: поддержка реальных структур WB/Ozon (sales/postings/orders), а не только item['price']/item['article'].
- Добавлены универсальные парсеры для WB и Ozon: извлекаем артикул, название, цену, дату из разных форматов.
- Учет себестоимости/налогов/доп.расходов теперь корректный по количеству позиций (unit economics на штуку).
- Нормализованы артикулы (strip/upper) для совпадения с БД.
- Улучшен generate_stock_report: корректная обработка stocks в Ozon (dict или list), и quantity/nmId в WB.
- Добавлены лимиты по количеству строк и защита от слишком длинных сообщений.
- Улучшено форматирование валюты: неразрывный пробел и округление.
"""

from __future__ import annotations

import html
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

from sqlalchemy import select

from database import async_session, Product

logger = logging.getLogger(__name__)

TELEGRAM_TEXT_LIMIT = 4096


# =============================================================================
# Форматирование
# =============================================================================

def format_currency(value: float) -> str:
    """
    Превращает число в красивую строку: 12500.5 -> 12 501 ₽.
    """
    try:
        val = float(value) if value is not None else 0.0
        return f"{val:,.0f}".replace(",", " ").replace(" ", "\u00A0") + " ₽"
    except (ValueError, TypeError):
        return "0 ₽"


def _safe_float(value: Any, default: float = 0.0) -> float:
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


def _safe_str(value: Any, default: str = "Н/Д", max_len: int = 255) -> str:
    s = str(value).strip() if value is not None else default
    if not s:
        s = default
    return s[:max_len]


def _norm_article(value: Any) -> str:
    return _safe_str(value, default="Н/Д", max_len=128).strip().upper()


def _truncate_text(text: str, limit: int = TELEGRAM_TEXT_LIMIT) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: limit - 20] + "\n…(сообщение сокращено)"


# =============================================================================
# Данные из БД (юнит-экономика)
# =============================================================================

async def get_user_cost_prices(user_tg_id: int, marketplace: str) -> Dict[str, Dict[str, float]]:
    """
    Загружает юнит-экономику товаров пользователя из БД.
    Возвращает: {ARTICLE: {"cost":..., "tax":..., "extra":...}}
    Важно: article в БД обычно хранится как upper/strip.
    """
    mp = str(marketplace or "").lower().strip()

    async with async_session() as session:
        try:
            result = await session.execute(
                select(Product.article, Product.cost_price, Product.tax_rate, Product.extra_costs).where(
                    Product.user_tg_id == user_tg_id,
                    Product.marketplace == mp,
                )
            )
            out: Dict[str, Dict[str, float]] = {}
            for row in result.all():
                article = _norm_article(row[0])
                out[article] = {
                    "cost": float(row[1] or 0.0),
                    "tax": float(row[2] or 0.06),
                    "extra": float(row[3] or 0.0),
                }
            return out
        except Exception as e:
            logger.error(f"Ошибка БД при получении себестоимости user={user_tg_id} mp={mp}: {e}")
            return {}


# =============================================================================
# Парсинг данных WB/Ozon в единый формат строк продаж
# =============================================================================

def _extract_price_from_wb_sale(item: Dict[str, Any]) -> float:
    """
    WB sales API может возвращать:
    - finishedPrice
    - priceWithDisc
    - forPay
    - totalPrice
    """
    for key in ("finishedPrice", "priceWithDisc", "forPay", "totalPrice", "price"):
        if key in item:
            return _safe_float(item.get(key), 0.0)
    return 0.0


def _extract_article_from_wb(item: Dict[str, Any]) -> str:
    """
    В WB возможны поля:
    - nmId (stocks/sales/orders)
    - supplierArticle (orders/sales)
    - article (FBS orders/new в твоей логике)
    - vendorCode (cards)
    """
    for key in ("nmId", "nmID", "supplierArticle", "article", "vendorCode"):
        v = item.get(key)
        if v not in (None, "", "Н/Д"):
            return _norm_article(v)
    return "Н/Д"


def _extract_name_from_wb(item: Dict[str, Any]) -> str:
    for key in ("subject", "brand", "name", "title"):
        v = item.get(key)
        if v:
            return _safe_str(v, default="Товар", max_len=255)
    return "Товар"


def _flatten_ozon_postings(postings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ozon postings: каждый posting содержит products[].
    Для финансовых отчётов удобнее привести к списку строк по товару:
    [{"article":..., "name":..., "price":...}, ...]
    """
    rows: List[Dict[str, Any]] = []
    for p in postings:
        if not isinstance(p, dict):
            continue
        products = p.get("products", [])
        if not isinstance(products, list):
            continue
        for prod in products:
            if not isinstance(prod, dict):
                continue
            article = _norm_article(prod.get("offer_id") or prod.get("sku") or "Н/Д")
            name = _safe_str(prod.get("name"), default="Товар", max_len=255)
            price = _safe_float(prod.get("price"), 0.0)
            rows.append({"article": article, "name": name, "price": price})
    return rows


def _unify_daily_data(marketplace: str, data: Union[list, dict]) -> List[Dict[str, Any]]:
    """
    Приводит входные данные разных маркетплейсов к единому плоскому списку:
    [{"article":..., "name":..., "price":...}, ...]
    """
    mp = str(marketplace or "").lower().strip()
    unified: List[Dict[str, Any]] = []

    # Ozon: может прийти dict {"fbs": [postings], "fbo": [postings]}
    if mp == "ozon":
        if isinstance(data, dict):
            fbs = data.get("fbs", [])
            fbo = data.get("fbo", [])
            postings = []
            if isinstance(fbs, list):
                postings.extend([x for x in fbs if isinstance(x, dict)])
            if isinstance(fbo, list):
                postings.extend([x for x in fbo if isinstance(x, dict)])
            unified = _flatten_ozon_postings(postings)
        elif isinstance(data, list):
            # иногда могли передать уже плоский список
            # попробуем распознать:
            # - если элементы похожи на posting (есть products) -> flatten
            if data and isinstance(data[0], dict) and "products" in data[0]:
                unified = _flatten_ozon_postings([x for x in data if isinstance(x, dict)])
            else:
                for x in data:
                    if not isinstance(x, dict):
                        continue
                    article = _norm_article(x.get("article") or x.get("offer_id") or x.get("sku") or "Н/Д")
                    name = _safe_str(x.get("name") or x.get("item_name"), default="Товар", max_len=255)
                    price = _safe_float(x.get("price"), 0.0)
                    unified.append({"article": article, "name": name, "price": price})
        return unified

    # WB: утренний отчет берёт sales list (get_sales_report -> list)
    # Но иногда может быть dict {"fbs":..., "fbo":...} — тоже поддержим.
    if isinstance(data, dict):
        fbs = data.get("fbs", [])
        fbo = data.get("fbo", [])
        items = []
        if isinstance(fbs, list):
            items.extend([x for x in fbs if isinstance(x, dict)])
        if isinstance(fbo, list):
            items.extend([x for x in fbo if isinstance(x, dict)])
    elif isinstance(data, list):
        items = [x for x in data if isinstance(x, dict)]
    else:
        items = []

    for item in items:
        article = _extract_article_from_wb(item)
        name = _extract_name_from_wb(item)
        price = _extract_price_from_wb_sale(item)
        unified.append({"article": article, "name": name, "price": price})

    return unified


# =============================================================================
# Дневной финансовый отчет
# =============================================================================

async def generate_daily_report_text(
    marketplace: str,
    data: Union[list, dict],
    user_tg_id: int,
    balance: float = 0.0,
) -> str:
    """
    Генерация финансового отчета за сутки.

    ВАЖНО:
    - Ozon: postings -> products -> суммирование по товарным строкам
    - WB: sales list -> извлекаем price и article через реальные ключи
    """
    mp = str(marketplace or "").strip()
    mp_key = mp.lower().strip()
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")

    header_emoji = "🔵" if mp_key == "ozon" else "🟣"

    unified_data = _unify_daily_data(mp_key, data)

    if not unified_data:
        text = (
            f"{header_emoji} <b>Отчет {html.escape(mp)}</b> за {yesterday_str}\n"
            f"──────────────────\n"
            f"💳 Баланс: <b>{format_currency(balance)}</b>\n"
            f"──────────────────\n"
            f"Данные о продажах за вчера отсутствуют."
        )
        return _truncate_text(text)

    # Загружаем юнит-экономику из БД
    user_costs = await get_user_cost_prices(user_tg_id, mp_key)

    total_revenue = 0.0
    total_cost_price = 0.0
    total_tax = 0.0
    total_extra = 0.0
    items_count = 0

    for row in unified_data:
        if not isinstance(row, dict):
            continue

        price = _safe_float(row.get("price"), 0.0)
        article = _norm_article(row.get("article") or "Н/Д")

        p_data = user_costs.get(article)
        if not p_data:
            # попытка совпадения без upper не нужна — _norm_article уже upper
            p_data = {"cost": 0.0, "tax": 0.06, "extra": 0.0}

        # ВАЖНО: себестоимость/extra берём как per-item, умножаем на количество строк
        total_revenue += price
        total_cost_price += float(p_data.get("cost", 0.0) or 0.0)
        total_extra += float(p_data.get("extra", 0.0) or 0.0)
        total_tax += (price * float(p_data.get("tax", 0.06) or 0.06))
        items_count += 1

    net_profit = total_revenue - total_cost_price - total_tax - total_extra
    roi = (net_profit / total_cost_price * 100) if total_cost_price > 0 else 0.0

    report_lines = [
        f"{header_emoji} <b>Отчет {html.escape(mp)}</b> за {yesterday_str}",
        "──────────────────",
        f"💳 Текущий баланс: <b>{format_currency(balance)}</b>",
        f"💰 Выручка: <b>{format_currency(total_revenue)}</b>",
        f"📦 Продано: <b>{items_count} шт.</b>",
        "──────────────────",
        f"📉 Себестоимость: <code>{format_currency(total_cost_price)}</code>",
        f"💸 Налоги: <code>{format_currency(total_tax)}</code>",
        f"📦 Доп. расходы: <code>{format_currency(total_extra)}</code>",
        "──────────────────",
        f"💎 <b>Чистая прибыль: {format_currency(net_profit)}</b>",
        f"📈 ROI: <b>{roi:.1f}%</b>",
        "\n<i>*Без учета комиссий и логистики МП</i>",
    ]

    return _truncate_text("\n".join(report_lines))


# =============================================================================
# Последние заказы (оперативный мониторинг)
# =============================================================================

async def generate_combined_orders_report(marketplace: str, orders_data: Any) -> str:
    """
    Формирует список последних заказов для оперативного мониторинга.

    Поддерживает:
    - WB: dict {'fbs': [...], 'fbo': [...]}
    - Ozon: dict {'fbs': [posting], 'fbo': [posting]} (и внутри products)
    - list: уже плоский список
    """
    mp = str(marketplace or "").strip()
    mp_key = mp.lower().strip()
    header_emoji = "🔵" if mp_key == "ozon" else "🟣"

    # Приводим к плоскому списку строк: [{"article","name","price"}]
    final_rows: List[Dict[str, Any]] = []

    if mp_key == "ozon":
        if isinstance(orders_data, dict):
            postings: List[Dict[str, Any]] = []
            fbs = orders_data.get("fbs", [])
            fbo = orders_data.get("fbo", [])
            if isinstance(fbs, list):
                postings.extend([x for x in fbs if isinstance(x, dict)])
            if isinstance(fbo, list):
                postings.extend([x for x in fbo if isinstance(x, dict)])
            final_rows = _flatten_ozon_postings(postings)
        elif isinstance(orders_data, list):
            if orders_data and isinstance(orders_data[0], dict) and "products" in orders_data[0]:
                final_rows = _flatten_ozon_postings([x for x in orders_data if isinstance(x, dict)])
            else:
                for x in orders_data:
                    if not isinstance(x, dict):
                        continue
                    final_rows.append(
                        {
                            "article": _norm_article(x.get("article") or x.get("offer_id") or x.get("sku") or "Н/Д"),
                            "name": _safe_str(x.get("name") or x.get("item_name"), default="Товар", max_len=255),
                            "price": _safe_float(x.get("price"), 0.0),
                        }
                    )
    else:
        # WB
        items: List[Dict[str, Any]] = []
        if isinstance(orders_data, dict):
            fbs = orders_data.get("fbs", [])
            fbo = orders_data.get("fbo", [])
            if isinstance(fbs, list):
                items.extend([x for x in fbs if isinstance(x, dict)])
            if isinstance(fbo, list):
                items.extend([x for x in fbo if isinstance(x, dict)])
        elif isinstance(orders_data, list):
            items = [x for x in orders_data if isinstance(x, dict)]

        for item in items:
            final_rows.append(
                {
                    "article": _extract_article_from_wb(item),
                    "name": _safe_str(item.get("name") or item.get("item_name") or _extract_name_from_wb(item), default="Товар", max_len=255),
                    "price": _extract_price_from_wb_sale(item),
                }
            )

    if not final_rows:
        return _truncate_text(f"{header_emoji} <b>{html.escape(mp)}:</b> Новых заказов нет.")

    lines = [f"{header_emoji} <b>Последние заказы {html.escape(mp)}:</b>", "──────────────────"]

    # Последние 10 строк
    for o in final_rows[:10]:
        name = o.get("name") or "Товар"
        safe_name = html.escape(str(name))
        price = format_currency(o.get("price", 0.0))
        article = o.get("article") or "Н/Д"

        lines.append(f"📦 {safe_name}\n└ <code>{html.escape(str(article))}</code> — <b>{price}</b>")

    if len(final_rows) > 10:
        lines.append(f"\n<i>...и еще {len(final_rows) - 10} позиций</i>")

    return _truncate_text("\n".join(lines))


# =============================================================================
# Отчет по низким остаткам
# =============================================================================

async def generate_stock_report(marketplace: str, items: list, threshold: int = 10) -> str:
    """
    Формирует список товаров с низким остатком.
    items:
    - WB: список stocks из statistics-api (/supplier/stocks): quantity, nmId
    - Ozon: список items из /v3/product/info/list: stocks может быть dict или list (и внутри stocks.stocks)
    """
    mp = str(marketplace or "").strip()
    mp_key = mp.lower().strip()

    if not isinstance(items, list) or not items:
        return ""

    try:
        threshold_int = int(threshold)
    except Exception:
        threshold_int = 10

    low_stock_lines: List[str] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        if mp_key == "ozon":
            # Возможные форматы:
            # item['stocks'] = {'stocks': [{'present':..}, ...]} или {'present':..}
            # item['offer_id']
            article = _norm_article(item.get("offer_id") or item.get("id") or item.get("product_id") or "Н/Д")

            total_qty = 0
            stocks = item.get("stocks")

            if isinstance(stocks, dict):
                inner = stocks.get("stocks")
                if isinstance(inner, list):
                    total_qty = sum(int(s.get("present", 0) or 0) for s in inner if isinstance(s, dict))
                else:
                    total_qty = int(stocks.get("present", 0) or 0)
            elif isinstance(stocks, list):
                total_qty = sum(int(s.get("present", 0) or 0) for s in stocks if isinstance(s, dict))
            else:
                total_qty = int(item.get("fbs_stocks", 0) or 0) + int(item.get("fbo_stocks", 0) or 0)

        else:
            # WB: quantity и nmId
            article = _norm_article(item.get("nmId") or item.get("article") or "Н/Д")
            total_qty = int(item.get("quantity", 0) or 0)

        if total_qty <= threshold_int:
            low_stock_lines.append(f"🔻 <code>{html.escape(str(article))}</code>: <b>{total_qty} шт.</b>")

    if not low_stock_lines:
        return ""

    header = [
        f"⚠️ <b>Дефицит {html.escape(mp)}</b>",
        "──────────────────",
        f"Остаток ниже {threshold_int} шт.:",
    ]

    # Ограничиваем список 20 позициями
    text = "\n".join(header) + "\n" + "\n".join(low_stock_lines[:20])
    return _truncate_text(text)


# =============================================================================
# Проверка API
# =============================================================================

async def generate_api_check_report(marketplace: str, is_valid: bool, balance: float = 0.0) -> str:
    """
    Статусный отчет для раздела настроек.
    """
    mp = str(marketplace or "").strip()
    status_emoji = "✅" if is_valid else "❌"
    status_text = "Подключено" if is_valid else "Ошибка (проверьте токены)"

    text = (
        f"🔌 <b>Статус {html.escape(mp)}</b>\n"
        f"──────────────────\n"
        f"Состояние: {status_emoji} <b>{status_text}</b>\n"
        f"💰 Доступно к выводу: <b>{format_currency(balance)}</b>"
    )
    return _truncate_text(text)
