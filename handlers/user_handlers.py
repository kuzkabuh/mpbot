import logging
import asyncio
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import db_functions as dbf
import reports as report_gen 
from ozon_api import OzonAPI
from wb_api import WildberriesAPI

# Настройка роутера
router = Router(name="user_router")
logger = logging.getLogger(__name__)

@router.message(Command("check_api"))
async def cmd_check_api(message: Message):
    """
    Команда для комплексной проверки статуса API и вывода текущих балансов.
    """
    user_id = message.from_user.id
    keys = await dbf.get_user_keys(user_id)
    
    # Исправлена логика проверки ключей (согласно именам в БД)
    if not keys or (not keys.get('wb_token') and not keys.get('ozon_api_key')):
        await message.answer("❌ У вас не привязаны API ключи.\nИспользуйте кнопку ⚙️ Настройки.")
        return

    wait_msg = await message.answer("🔄 Проверяю статус API и запрашиваю балансы...")
    
    results_text = []

    # --- Секция Wildberries ---
    if keys.get('wb_token'):
        try:
            wb = WildberriesAPI(keys['wb_token'])
            # Параллельный запуск проверки и баланса
            is_valid_task = wb.validate_token()
            balance_task = wb.get_balance()
            
            is_valid, balance = await asyncio.gather(is_valid_task, balance_task)
            
            wb_report = await report_gen.generate_api_check_report("Wildberries", is_valid, balance)
            results_text.append(wb_report)
        except Exception as e:
            logger.error(f"WB check error: {e}")
            results_text.append("🟣 <b>Wildberries:</b> ❌ Ошибка подключения")
    else:
        results_text.append("🟣 <b>Wildberries:</b> ⚪ Не настроен")

    # --- Секция Ozon ---
    # Исправлены названия ключей: ozon_client_id и ozon_api_key
    if keys.get('ozon_client_id') and keys.get('ozon_api_key'):
        try:
            ozon = OzonAPI(keys['ozon_client_id'], keys['ozon_api_key'])
            # В OzonAPI мы используем check_connection, который возвращает (bool, dict)
            is_valid, _ = await ozon.check_connection()
            balance = await ozon.get_balance() if is_valid else 0.0
            
            ozon_report = await report_gen.generate_api_check_report("Ozon", is_valid, balance)
            results_text.append(ozon_report)
        except Exception as e:
            logger.error(f"Ozon check error: {e}")
            results_text.append("🔵 <b>Ozon:</b> ❌ Ошибка подключения")
    else:
        results_text.append("🔵 <b>Ozon:</b> ⚪ Не настроен")

    await wait_msg.edit_text("\n\n".join(results_text), parse_mode="HTML")

@router.message(Command("profit"))
async def cmd_profit(message: Message):
    """
    Меню выбора периода для аналитического отчета.
    """
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Вчера", callback_query_data="profit_1"),
            InlineKeyboardButton(text="7 дней", callback_query_data="profit_7")
        ],
        [
            InlineKeyboardButton(text="30 дней", callback_query_data="profit_30"),
            InlineKeyboardButton(text="🔄 Проверить балансы", callback_query_data="check_api_cb")
        ]
    ])
    await message.answer(
        "📊 <b>Аналитика прибыли</b>\n\n"
        "Расчет включает себестоимость, логистику и налоги.\n"
        "Выберите период:", 
        reply_markup=kb, 
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("profit_"))
async def process_profit_report(callback: CallbackQuery):
    """
    Сбор данных и генерация консолидированного отчета по прибыли.
    """
    days = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    await callback.message.edit_text(f"⏳ Собираю данные за {days} дн. Подождите...")
    
    user_keys = await dbf.get_user_keys(user_id)
    all_orders = {'fbs': [], 'fbo': []} # Используем структуру словаря
    total_balance = 0.0

    try:
        # Сбор данных WB
        if user_keys.get('wb_token'):
            wb = WildberriesAPI(user_keys['wb_token'])
            wb_data = await wb.get_all_orders(days=days)
            all_orders['fbs'].extend(wb_data.get('fbs', []))
            all_orders['fbo'].extend(wb_data.get('fbo', []))
            total_balance += await wb.get_balance()

        # Сбор данных Ozon
        if user_keys.get('ozon_client_id') and user_keys.get('ozon_api_key'):
            ozon = OzonAPI(user_keys['ozon_client_id'], user_keys['ozon_api_key'])
            oz_data = await ozon.get_all_orders(days=days)
            all_orders['fbs'].extend(oz_data.get('fbs', []))
            all_orders['fbo'].extend(oz_data.get('fbo', []))
            total_balance += await ozon.get_balance()

        if not all_orders['fbs'] and not all_orders['fbo']:
            await callback.message.edit_text(f"❌ За последние {days} дн. данных о заказах не найдено.")
            return

        # Генерация текста через единый модуль отчетов
        report_text = await report_gen.generate_daily_report_text(
            "Общий (WB+Ozon)", 
            all_orders, 
            user_tg_id=user_id, 
            balance=total_balance
        )
        
        # Корректируем заголовок периода
        period_label = "Вчера" if days == 1 else f"{days} дн."
        report_text = report_text.replace("Вчера", period_label)
        
        await callback.message.edit_text(report_text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Ошибка консолидированной аналитики: {e}", exc_info=True)
        await callback.message.edit_text("❌ Ошибка при расчете. Проверьте корректность API ключей.")
    
    await callback.answer()

@router.callback_query(F.data == "check_api_cb")
async def callback_check_api(callback: CallbackQuery):
    """Триггер проверки API из инлайн-кнопок."""
    await callback.answer()
    # Перенаправляем логику на существующую функцию
    await cmd_check_api(callback.message)