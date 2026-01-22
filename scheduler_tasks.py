import logging
import asyncio
import html
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from database import async_session, User
from sqlalchemy import select
from wb_api import WildberriesAPI
from ozon_api import OzonAPI
import db_functions as dbf
import reports  # Модуль для формирования текста отчетов
from datetime import datetime, timedelta

# Настройка логирования
logger = logging.getLogger(__name__)

# Ограничитель одновременных запросов для защиты от Flood Limit Telegram
sem = asyncio.Semaphore(5)

async def safe_send_message(bot: Bot, chat_id: int, text: str, parse_mode: str = "HTML"):
    """
    Безопасная отправка сообщений с обработкой лимитов и блокировок бота пользователем.
    """
    async with sem:
        try:
            await bot.send_message(chat_id, text, parse_mode=parse_mode)
            # Пауза для соблюдения лимитов Telegram (не более 30 сообщений в секунду)
            await asyncio.sleep(0.05)
        except TelegramRetryAfter as e:
            logger.warning(f"Flood limit! Спим {e.retry_after}с. User: {chat_id}")
            await asyncio.sleep(e.retry_after)
            return await safe_send_message(bot, chat_id, text, parse_mode)
        except TelegramForbiddenError:
            logger.info(f"Бот заблокирован пользователем {chat_id}. Пропускаем отправку.")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка отправки сообщения пользователю {chat_id}: {e}")

async def check_new_orders_task(bot: Bot):
    """
    Фоновая задача: периодическая проверка новых заказов на маркетплейсах.
    """
    async with async_session() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()

    for user in users:
        # Мониторинг Wildberries
        if user.wb_token:
            await _process_wb_orders(bot, user)

        # Мониторинг Ozon
        if user.ozon_client_id and user.ozon_api_key:
            await _process_ozon_orders(bot, user)

async def _process_wb_orders(bot: Bot, user: User):
    """Внутренняя логика обработки и фильтрации заказов Wildberries."""
    try:
        wb = WildberriesAPI(user.wb_token)
        all_wb = await wb.get_all_orders(days=1)
        
        # Проверка, что API вернуло ожидаемый словарь, а не строку ошибки
        if not isinstance(all_wb, dict):
            logger.error(f"WB API вернул некорректный формат (ожидался dict, получен {type(all_wb)}) для {user.tg_id}")
            return

        # 1. Обработка FBS (Сборочные задания)
        fbs_list = all_wb.get('fbs', [])
        if isinstance(fbs_list, list):
            for order in fbs_list:
                order_id = str(order.get('id'))
                if order_id and await dbf.is_order_new(order_id, 'wb'):
                    article = html.escape(str(order.get('article') or 'Н/Д'))
                    raw_price = order.get('convertedPrice') or order.get('price', 0)
                    price = float(raw_price) / 100
                    
                    msg = (
                        f"🚀 <b>Новый заказ Wildberries (FBS)!</b>\n\n"
                        f"📦 Номер: <code>{order_id}</code>\n"
                        f"🔢 Артикул: <code>{article}</code>\n"
                        f"💰 К оплате: <b>{price:,.2f} ₽</b>"
                    )
                    await safe_send_message(bot, user.tg_id, msg)
                    await dbf.save_order(order_id, 'wb', amount=price, item_name=article, user_tg_id=user.tg_id)

        # 2. Обработка FBO (Продажи со склада маркетплейса)
        fbo_list = all_wb.get('fbo', [])
        if isinstance(fbo_list, list):
            for order in fbo_list:
                order_id = str(order.get('gNumber'))
                if order_id and await dbf.is_order_new(order_id, 'wb'):
                    article = html.escape(str(order.get('supplierArticle') or 'Н/Д'))
                    price = float(order.get('totalPrice', 0))
                    
                    msg = (
                        f"📦 <b>Продажа Wildberries (FBO)!</b>\n\n"
                        f"🔢 Артикул: <code>{article}</code>\n"
                        f"💰 Сумма: <b>{price:,.2f} ₽</b>"
                    )
                    await safe_send_message(bot, user.tg_id, msg)
                    await dbf.save_order(order_id, 'wb', amount=price, item_name=article, user_tg_id=user.tg_id)

    except Exception as e:
        logger.error(f"Ошибка в задаче WB для пользователя {user.tg_id}: {e}")

async def _process_ozon_orders(bot: Bot, user: User):
    """Внутренняя логика обработки и фильтрации заказов Ozon."""
    try:
        ozon = OzonAPI(user.ozon_client_id, user.ozon_api_key)
        all_ozon = await ozon.get_all_orders(days=1)
        
        if not isinstance(all_ozon, dict):
            return

        fbs_orders = all_ozon.get('fbs', [])
        if isinstance(fbs_orders, list):
            for order in fbs_orders:
                order_id = str(order.get('posting_number'))
                if order_id and await dbf.is_order_new(order_id, 'ozon'):
                    prods = order.get('products', [])
                    article = prods[0].get('offer_id') if prods else "Н/Д"
                    price = sum(float(p.get('price', 0)) for p in prods)
                    
                    msg = (
                        f"🚀 <b>Новый заказ Ozon (FBS)!</b>\n\n"
                        f"📦 Номер: <code>{order_id}</code>\n"
                        f"🔢 Артикул: <code>{article}</code>\n"
                        f"💰 Сумма: <b>{price:,.2f} ₽</b>"
                    )
                    await safe_send_message(bot, user.tg_id, msg)
                    await dbf.save_order(order_id, 'ozon', amount=price, item_name=article, user_tg_id=user.tg_id)
    except Exception as e:
        logger.error(f"Ошибка в задаче Ozon для пользователя {user.tg_id}: {e}")

async def send_morning_report(bot: Bot):
    """Рассылка финансовых итогов за прошедшие сутки (запускается по расписанию)."""
    async with async_session() as session:
        users = (await session.execute(select(User))).scalars().all()

    for user in users:
        report_parts = [f"🌅 <b>Отчет за {(datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')}</b>\n"]
        has_data = False
        
        # Сбор данных Wildberries
        if user.wb_token:
            try:
                wb = WildberriesAPI(user.wb_token)
                sales = await wb.get_sales_report(days=1)
                
                # ИСПРАВЛЕНИЕ: Проверка типа данных (защита от 'str' object has no attribute 'get')
                if sales and isinstance(sales, list):
                    balance = await wb.get_balance()
                    # Проверка валидности баланса перед передачей
                    bal_val = balance if isinstance(balance, (int, float)) else None
                    report_wb = await reports.generate_daily_report_text("Wildberries", sales, user.tg_id, balance=bal_val)
                    report_parts.append(report_wb)
                    has_data = True
            except Exception as e:
                logger.error(f"Ошибка утреннего отчета WB для {user.tg_id}: {e}")

        # Сбор данных Ozon
        if user.ozon_client_id and user.ozon_api_key:
            try:
                ozon = OzonAPI(user.ozon_client_id, user.ozon_api_key)
                yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                stats = await ozon.get_daily_stats(yesterday_str)
                
                if stats and (isinstance(stats, list) or isinstance(stats, dict)):
                    balance = await ozon.get_balance()
                    bal_val = balance if isinstance(balance, (int, float)) else None
                    report_ozon = await reports.generate_daily_report_text("Ozon", stats, user.tg_id, balance=bal_val)
                    report_parts.append(report_ozon)
                    has_data = True
            except Exception as e:
                logger.error(f"Ошибка утреннего отчета Ozon для {user.tg_id}: {e}")

        if has_data:
            await safe_send_message(bot, user.tg_id, "\n\n".join(report_parts))

async def check_low_stock_task(bot: Bot):
    """Проверка остатков товаров и уведомление при достижении критического порога."""
    async with async_session() as session:
        users = (await session.execute(select(User))).scalars().all()

    for user in users:
        threshold = getattr(user, 'stock_threshold', 5) or 5
        
        # Обход подключенных маркетплейсов
        for mp_name, api_class, token_data in [
            ("Wildberries", WildberriesAPI, [user.wb_token]),
            ("Ozon", OzonAPI, [user.ozon_client_id, user.ozon_api_key])
        ]:
            if all(token_data):
                try:
                    api = api_class(*token_data)
                    stocks = await api.get_stock_info()
                    if stocks and isinstance(stocks, list):
                        report = await reports.generate_stock_report(mp_name, stocks, threshold=threshold)
                        if report:
                            await safe_send_message(bot, user.tg_id, report)
                except Exception as e:
                    logger.error(f"Ошибка проверки остатков {mp_name} для {user.tg_id}: {e}")