import logging
import asyncio  # Добавлено для устранения ошибки "name 'asyncio' is not defined"
from datetime import datetime, timedelta
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery

# Импорт внутренних модулей проекта
import db_functions as dbf
import reports as report_gen  # Модуль генерации текстовых шаблонов
import keyboards as kb
from wb_api import WildberriesAPI
from ozon_api import OzonAPI
from financial_processor import fin_processor # Аналитический движок для расчета прибыли

# Настройка роутера и логирования
router = Router()
logger = logging.getLogger(__name__)

async def get_daily_stats_logic(user_id: int):
    """
    Центлизованная логика сбора статистики продаж и остатков на счетах за прошедшие сутки.
    """
    user_keys = await dbf.get_user_keys(user_id)
    results = []
    
    # --- СЕКЦИЯ OZON ---
    if user_keys.get('ozon_client_id') and user_keys.get('ozon_api_key'):
        try:
            ozon = OzonAPI(user_keys['ozon_client_id'], user_keys['ozon_api_key'])
            # Получаем заказы за 1 день и текущий баланс
            data = await ozon.get_all_orders(days=1) 
            balance = await ozon.get_balance() 
            
            report_text = await report_gen.generate_daily_report_text(
                "Ozon", data, user_tg_id=user_id, balance=balance 
            )
            if report_text:
                results.append(report_text)
        except Exception as e: 
            logger.error(f"Ошибка ежедневного отчета Ozon для {user_id}: {e}")
    
    # --- СЕКЦИЯ WILDBERRIES ---
    if user_keys.get('wb_token'):
        try:
            wb = WildberriesAPI(user_keys['wb_token'])
            # WB возвращает словарь с ключами 'fbs' и 'fbo'
            data = await wb.get_all_orders(days=1)
            balance = await wb.get_balance()
            
            report_text = await report_gen.generate_daily_report_text(
                "Wildberries", data, user_tg_id=user_id, balance=balance
            )
            if report_text:
                results.append(report_text)
        except Exception as e: 
            logger.error(f"Ошибка ежедневного отчета WB для {user_id}: {e}")
            
    return "\n\n".join(results) if results else "ℹ️ Данные за вчера отсутствуют или API ключи не активны."

@router.message(F.text == "📊 Сводка по всем")
async def show_total_summary(message: Message):
    """Отображение краткой сводки по всем подключенным площадкам."""
    status = await message.answer("⏳ Собираю общую сводку за вчера...")
    res = await get_daily_stats_logic(message.from_user.id)
    
    # Обновляем сообщение результатом с поддержкой HTML-разметки
    await status.edit_text(f"📈 <b>Общая сводка (Вчера)</b>\n\n{res}", parse_mode="HTML")

@router.message(F.text == "💰 Мой баланс")
async def show_balance_only(message: Message):
    """
    Вывод только финансовых остатков. 
    """
    status = await message.answer("⏳ Запрашиваю финансовые данные...")
    user_keys = await dbf.get_user_keys(message.from_user.id)
    balance_reports = []
    
    # Проверка и запрос баланса Ozon
    if user_keys.get('ozon_client_id') and user_keys.get('ozon_api_key'):
        try:
            ozon = OzonAPI(user_keys['ozon_client_id'], user_keys['ozon_api_key'])
            bal = await ozon.get_balance()
            balance_reports.append(f"🔵 <b>Ozon:</b> <code>{bal:,.2f}</code> ₽")
        except Exception as e:
            logger.error(f"Баланс Ozon (reports): {e}")

    # Проверка и запрос баланса Wildberries
    if user_keys.get('wb_token'):
        try:
            wb = WildberriesAPI(user_keys['wb_token'])
            bal = await wb.get_balance()
            balance_reports.append(f"🟣 <b>Wildberries:</b> <code>{bal:,.2f}</code> ₽")
        except Exception as e:
            logger.error(f"Баланс WB (reports): {e}")

    if not balance_reports:
        await status.edit_text("❌ API ключи не настроены или недоступны.")
    else:
        text = "💰 <b>Доступно к выводу:</b>\n\n" + "\n".join(balance_reports)
        await status.edit_text(text, parse_mode="HTML")

@router.message(F.text == "📦 Текущие заказы")
async def show_orders_menu(message: Message):
    """Вызов инлайн-меню для выбора площадки мониторинга заказов."""
    await message.answer(
        "📦 <b>Выберите маркетплейс для просмотра заказов:</b>", 
        reply_markup=kb.get_orders_menu(), 
        parse_mode="HTML"
    )

@router.message(F.text == "📈 Фин. отчет")
async def show_finance_menu(message: Message):
    """Переход в меню глубокой аналитики чистой прибыли."""
    await message.answer(
        "💎 <b>Глубокая аналитика прибыли</b>\n\n"
        "Расчет чистой прибыли включает:\n"
        "• Налоги (настроенные в профиле)\n"
        "• Комиссии и логистику маркетплейса\n"
        "• Загруженную себестоимость товаров\n\n"
        "Выберите период:", 
        reply_markup=kb.get_finance_periods_menu()
    )

@router.callback_query(F.data.startswith("fin_wb_7d"))
async def process_wb_weekly_finance(callback: CallbackQuery):
    """
    Генерация детального фин. отчета для Wildberries за 7 дней.
    """
    user_id = callback.from_user.id
    user_keys = await dbf.get_user_keys(user_id)
    
    if not user_keys.get('wb_token'):
        return await callback.answer("❌ Токен WB не настроен", show_alert=True)
    
    await callback.message.edit_text("⏳ <b>Идет расчет прибыли...</b>\nЭто может занять время, так как отчет WB очень объемный.")
    
    try:
        wb = WildberriesAPI(user_keys['wb_token'])
        now = datetime.now()
        date_to = now.strftime('%Y-%m-%d')
        date_from = (now - timedelta(days=7)).strftime('%Y-%m-%d')
        
        # Получение данных детализации через API WB
        raw_report = await wb.get_report_detail(date_from, date_to)
        
        if not raw_report:
            return await callback.message.edit_text("❌ Нет данных от WB за последние 7 дней.")

        # Расчет через фин. процессор (использует данные о себестоимости и налогах из БД)
        analytics = await fin_processor.process_wb_weekly_json(user_id, raw_report)
        
        if not analytics or analytics.get('sales_count', 0) == 0:
            return await callback.message.edit_text("❌ За этот период продаж не обнаружено.")
            
        res_text = (
            f"🟣 <b>Финансовый отчет WB (7 дней)</b>\n"
            f"📅 Период: {date_from} — {date_to}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📦 Продаж: <b>{analytics['sales_count']} шт.</b>\n"
            f"🔄 Возвратов: <b>{analytics['returns_count']} шт.</b>\n\n"
            f"💰 Выручка (к перечислению): <b>{analytics['revenue']:,.2f} ₽</b>\n"
            f"🚚 Логистика: <b>{analytics['delivery']:,.2f} ₽</b>\n"
            f"🧾 Налоги: <b>{analytics['tax']:,.2f} ₽</b>\n"
            f"📉 Себестоимость: <b>{analytics['cost']:,.2f} ₽</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"✅ <b>Чистая прибыль: {analytics['profit']:,.2f} ₽</b>\n"
            f"📈 Маржинальность: <b>{analytics['margin']}%</b>"
        )
        await callback.message.edit_text(res_text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Ошибка генерации фин. отчета WB: {e}")
        await callback.message.edit_text("❌ Ошибка при обработке данных. Возможно, API WB временно недоступен.")
    
    await callback.answer()

@router.callback_query(F.data.startswith("orders_"))
async def process_orders(callback: CallbackQuery):
    """Обработка запроса на список заказов и текущие остатки."""
    platform = callback.data.split("_")[1]
    user_keys = await dbf.get_user_keys(callback.from_user.id)
    
    await callback.message.edit_text(f"⏳ Загружаю данные {platform.upper()}...")
    
    try:
        if platform == "wb" and user_keys.get('wb_token'):
            api = WildberriesAPI(user_keys['wb_token'])
            marketplace_name = "Wildberries"
        elif platform == "ozon" and user_keys.get('ozon_client_id') and user_keys.get('ozon_api_key'):
            api = OzonAPI(user_keys['ozon_client_id'], user_keys['ozon_api_key'])
            marketplace_name = "Ozon"
        else:
            await callback.message.edit_text("❌ API ключи для этой площадки не настроены.")
            return

        # Параллельный запрос данных для ускорения работы
        orders_task = api.get_all_orders(days=1)
        stocks_task = api.get_stock_info()
        
        # Используем asyncio.gather, теперь asyncio импортирован корректно
        orders_data, stocks = await asyncio.gather(orders_task, stocks_task)
        
        # Генерация текстовых блоков отчета
        orders_report = await report_gen.generate_combined_orders_report(marketplace_name, orders_data)
        stock_report = await report_gen.generate_stock_report(marketplace_name, stocks)
        
        final_text = orders_report + ("\n\n" + stock_report if stock_report else "")
        
        # Отображение итогового отчета
        await callback.message.edit_text(final_text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Ошибка в обработчике заказов {platform}: {e}")
        await callback.message.edit_text(f"❌ Не удалось получить данные от {platform.upper()}. Проверьте ключи API.")
        
    await callback.answer()