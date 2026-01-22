import logging
import html
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union
from sqlalchemy import select
from database import async_session, Product

# Настройка логирования для отслеживания ошибок формирования отчетов
logger = logging.getLogger(__name__)

def format_currency(value: float) -> str:
    """
    Превращает число в красивую строку: 12500.5 -> 12 501 ₽.
    """
    try:
        val = float(value) if value is not None else 0.0
        # Используем неразрывный пробел (\u00A0) для красоты, чтобы валюта не отрывалась от числа
        return f"{val:,.0f}".replace(',', ' ') + " ₽"
    except (ValueError, TypeError):
        return "0 ₽"

async def get_user_cost_prices(user_tg_id: int, marketplace: str) -> dict:
    """
    Загружает юнит-экономику товаров пользователя из БД.
    Возвращает словарь для быстрого поиска: {артикул: {данные}}
    """
    async with async_session() as session:
        try:
            result = await session.execute(
                select(Product.article, Product.cost_price, Product.tax_rate, Product.extra_costs)
                .where(
                    Product.user_tg_id == user_tg_id,
                    Product.marketplace == marketplace.lower()
                )
            )
            # Формируем словарь: ключ — артикул (приводим к строке для надежности)
            return {
                str(row[0]): {
                    "cost": row[1] or 0.0, 
                    "tax": row[2] or 0.06, 
                    "extra": row[3] or 0.0
                } for row in result.all()
            }
        except Exception as e:
            logger.error(f"Ошибка БД при получении цен для {user_tg_id}: {e}")
            return {}

async def generate_daily_report_text(marketplace: str, data: Union[list, dict], user_tg_id: int, balance: float = 0.0):
    """
    Главная функция генерации финансового отчета за сутки.
    Поддерживает как списки (Ozon), так и словари fbs/fbo (WB).
    """
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
    header_emoji = "🔵" if marketplace.lower() == "ozon" else "🟣"

    # 1. Унификация данных (приведение к плоскому списку)
    unified_data = []
    if isinstance(data, dict):
        # Логика для Wildberries: объединяем fbs и fbo
        fbs = data.get('fbs', [])
        fbo = data.get('fbo', [])
        unified_data = (fbs if isinstance(fbs, list) else []) + (fbo if isinstance(fbo, list) else [])
    elif isinstance(data, list):
        unified_data = data

    # Защита от пустых данных
    if not unified_data:
        return (
            f"{header_emoji} <b>Отчет {marketplace}</b> за {yesterday_str}\n"
            f"──────────────────\n"
            f"💳 Баланс: <b>{format_currency(balance)}</b>\n"
            f"──────────────────\n"
            f"Данные о продажах за вчера отсутствуют. 🤷‍♂️"
        )

    # 2. Загружаем данные о себестоимости
    user_costs = await get_user_cost_prices(user_tg_id, marketplace)

    total_revenue = 0.0    # Грязная выручка
    total_cost_price = 0.0 # Закупка (сумма)
    total_tax = 0.0        # Налоги
    total_extra = 0.0      # Прочие расходы
    items_count = 0

    # 3. Обработка списка заказов
    for item in unified_data:
        if not isinstance(item, dict):
            continue

        price = float(item.get('price', 0))
        # Проверяем разные варианты ключей артикула
        article = str(item.get('article') or item.get('offer_id') or item.get('nmId') or 'Н/Д')
        
        # Ищем товар в справочнике или берем дефолты
        p_data = user_costs.get(article, {"cost": 0.0, "tax": 0.06, "extra": 0.0})
        
        total_revenue += price
        total_cost_price += p_data["cost"]
        total_extra += p_data["extra"]
        total_tax += (price * p_data["tax"])
        items_count += 1

    # 4. Финансовые расчеты
    net_profit = total_revenue - total_cost_price - total_tax - total_extra
    roi = (net_profit / total_cost_price * 100) if total_cost_price > 0 else 0

    report = [
        f"{header_emoji} <b>Отчет {marketplace}</b> за {yesterday_str}",
        f"──────────────────",
        f"💳 Текущий баланс: <b>{format_currency(balance)}</b>",
        f"💰 Выручка: <b>{format_currency(total_revenue)}</b>",
        f"📦 Продано: <b>{items_count} шт.</b>",
        f"──────────────────",
        f"📉 Себестоимость: <code>{format_currency(total_cost_price)}</code>",
        f"💸 Налоги: <code>{format_currency(total_tax)}</code>",
        f"📦 Доп. расходы: <code>{format_currency(total_extra)}</code>",
        f"──────────────────",
        f"💎 <b>Чистая прибыль: {format_currency(net_profit)}</b>",
        f"📈 ROI: <b>{roi:.1f}%</b>",
        f"\n<i>*Без учета комиссий и логистики МП</i>"
    ]
    
    return "\n".join(report)

async def generate_combined_orders_report(marketplace: str, orders_data: Any) -> str:
    """
    Формирует список последних заказов для оперативного мониторинга.
    Исправлена ошибка slice(None, 10, None) путем преобразования в список.
    """
    header_emoji = "🔵" if marketplace.lower() == "ozon" else "🟣"
    
    # Приводим любые данные (dict или list) к единому списку
    final_list = []
    if isinstance(orders_data, dict):
        fbs = orders_data.get('fbs', [])
        fbo = orders_data.get('fbo', [])
        final_list = (fbs if isinstance(fbs, list) else []) + (fbo if isinstance(fbo, list) else [])
    elif isinstance(orders_data, list):
        final_list = orders_data

    if not final_list:
        return f"{header_emoji} <b>{marketplace}:</b> Новых заказов нет."

    lines = [f"{header_emoji} <b>Последние заказы {marketplace}:</b>", "──────────────────"]
    
    # Берем последние 10 заказов для краткости (теперь срез работает всегда)
    for o in final_list[:10]:
        name = o.get('name') or o.get('item_name') or "Товар"
        # Экранируем HTML символы, чтобы не сломать парсинг Telegram
        safe_name = html.escape(str(name))
        price = format_currency(o.get('price', 0))
        article = o.get('article') or o.get('offer_id') or o.get('nmId') or 'Н/Д'
        
        lines.append(f"📦 {safe_name}\n└ <code>{article}</code> — <b>{price}</b>")

    if len(final_list) > 10:
        lines.append(f"\n<i>...и еще {len(final_list) - 10} заказов</i>")
        
    return "\n".join(lines)

async def generate_stock_report(marketplace: str, items: list, threshold: int = 10):
    """
    Формирует список товаров с низким остатком.
    """
    if not isinstance(items, list) or not items:
        return ""

    low_stock_lines = []
    for item in items:
        if not isinstance(item, dict): 
            continue

        if marketplace.lower() == "ozon":
            stocks = item.get('stocks', [])
            total_qty = sum(s.get('present', 0) for s in stocks) if isinstance(stocks, list) else 0
            article = item.get('offer_id', 'Н/Д')
        else:
            total_qty = item.get('quantity', 0)
            article = item.get('nmId') or item.get('article', 'Н/Д')

        if total_qty <= threshold:
            low_stock_lines.append(f"🔻 <code>{article}</code>: <b>{total_qty} шт.</b>")

    if not low_stock_lines:
        return ""

    header = [
        f"\n⚠️ <b>Дефицит {marketplace}</b>",
        f"──────────────────",
        f"Остаток ниже {threshold} шт.:",
    ]
    # Ограничиваем список 20 позициями, чтобы сообщение влезло в лимиты Telegram
    return "\n".join(header) + "\n" + "\n".join(low_stock_lines[:20])

async def generate_api_check_report(marketplace: str, is_valid: bool, balance: float = 0.0) -> str:
    """
    Статусный отчет для раздела настроек.
    """
    status_emoji = "✅" if is_valid else "❌"
    status_text = "Подключено" if is_valid else "Ошибка (проверьте токены)"
    
    return (
        f"🔌 <b>Статус {marketplace}</b>\n"
        f"──────────────────\n"
        f"Состояние: {status_emoji} <b>{status_text}</b>\n"
        f"💰 Доступно к выводу: <b>{format_currency(balance)}</b>"
    )