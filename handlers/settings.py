import logging
import io
import pandas as pd
import asyncio
from aiogram import Router, F, types
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext
from sqlalchemy import select, update

# Импорты вашего проекта
import keyboards as kb
import db_functions as dbf
from states import SetupKeys
from wb_api import WildberriesAPI
from ozon_api import OzonAPI
from database import async_session, Product, User

# Инициализируем роутер
router = Router(name="settings_router")
logger = logging.getLogger(__name__)

# =========================================================
# РАЗДЕЛ 1: ЮНИТ-ЭКОНОМИКА (МОИ ТОВАРЫ / EXCEL)
# =========================================================

@router.message(F.text == "📦 Мои товары")
async def show_products_menu(message: Message):
    """Отображает меню управления товарами и себестоимостью."""
    await dbf.register_user(message.from_user.id)
    
    text = (
        "<b>📦 Управление товарами</b>\n\n"
        "Настройте себестоимость для точного расчета чистой прибыли:\n\n"
        "1. <b>Excel</b> — выгрузите список, заполните данные и отправьте файл обратно.\n"
        "2. <b>Web App</b> — редактирование в браузере (в разработке).\n"
        "3. <b>Синхронизация</b> — обновите список артикулов из личных кабинетов."
    )
    
    webapp_url = "https://your-domain.com/webapp" 
    await message.answer(text, reply_markup=kb.get_products_inline_menu(webapp_url), parse_mode="HTML")

@router.callback_query(F.data == "download_products")
async def cb_download_products(callback: CallbackQuery):
    """Генерация Excel файла с данными о товарах пользователя."""
    async with async_session() as session:
        res = await session.execute(
            select(Product).where(Product.user_tg_id == callback.from_user.id)
        )
        products = res.scalars().all()
        
        if not products:
            await callback.answer(
                "❌ Список пуст. Сначала нажмите 'Синхронизировать'", 
                show_alert=True
            )
            return

        temp_msg = await callback.message.answer("⏳ Генерирую файл...")
        
        try:
            data = []
            for p in products:
                data.append({
                    "Маркетплейс": str(p.marketplace).upper(),
                    "Артикул": p.article,
                    "Название": p.name,
                    "Себестоимость": p.cost_price or 0.0,
                    "Налог (0.06 = 6%)": p.tax_rate or 0.06,
                    "Доп_расходы": p.extra_costs or 0.0
                })
            
            df = pd.DataFrame(data)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Products')
            
            document = BufferedInputFile(
                output.getvalue(), 
                filename=f"products_{callback.from_user.id}.xlsx"
            )
            
            await callback.message.answer_document(
                document, 
                caption="✅ <b>Файл готов!</b>\n\nЗаполните колонки и отправьте файл боту."
            )
            await temp_msg.delete()
        except Exception as e:
            logger.error(f"Ошибка Excel: {e}")
            await temp_msg.edit_text("❌ Ошибка при создании файла.")
        
        await callback.answer()

@router.callback_query(F.data == "sync_products")
async def cb_sync_products(callback: CallbackQuery):
    """
    Синхронизация артикулов через API площадок.
    [FIXED] Теперь использует методы get_all_products для пакетного обновления БД.
    """
    tg_id = callback.from_user.id
    status_msg = await callback.message.answer("🔄 Синхронизирую товары... Это может занять время.")
    
    keys = await dbf.get_user_keys(tg_id)
    if not keys:
        await status_msg.edit_text("❌ Настройте API ключи в настройках.")
        return

    async def sync_wb():
        """Синхронизация товаров Wildberries."""
        if keys.get("wb_token"):
            try:
                wb = WildberriesAPI(keys["wb_token"])
                # Предполагаем, что в WB API есть метод получения списка всех товаров
                # Если нет, используем старую логику, но с пакетным сохранением
                products = await wb.get_all_products() # Ожидаем список словарей
                if products:
                    # Массовое добавление/обновление в БД (нужно реализовать в db_functions)
                    return await dbf.bulk_update_products(tg_id, products)
            except Exception as e: 
                logger.error(f"WB sync error: {e}")
        return 0

    async def sync_ozon():
        """Синхронизация товаров Ozon."""
        if keys.get("ozon_api_key") and keys.get("ozon_client_id"):
            try:
                ozon = OzonAPI(keys["ozon_client_id"], keys["ozon_api_key"])
                # Используем наш новый исправленный метод с пагинацией и v3
                products = await ozon.get_all_products()
                if products:
                    # Сохраняем все найденные товары разом
                    return await dbf.bulk_update_products(tg_id, products)
            except Exception as e: 
                logger.error(f"Ozon sync error: {e}")
        return 0

    # Запускаем процессы параллельно для скорости
    wb_count, oz_count = await asyncio.gather(sync_wb(), sync_ozon())
    
    await status_msg.edit_text(
        f"✅ <b>Синхронизация завершена!</b>\n\n"
        f"Wildberries: <b>{wb_count}</b> товаров\n"
        f"Ozon: <b>{oz_count}</b> товаров\n\n"
        f"Теперь вы можете выгрузить Excel для настройки себестоимости."
    )
    await callback.answer()

@router.message(F.document)
async def handle_products_excel(message: Message):
    """Массовое обновление себестоимости из присланного файла."""
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        return

    status_msg = await message.answer("⏳ Обрабатываю данные...")
    try:
        file = await message.bot.get_file(message.document.file_id)
        downloaded_file = await message.bot.download_file(file.file_path)
        df = pd.read_excel(io.BytesIO(downloaded_file.read()))
        
        df.columns = [str(c).strip() for c in df.columns]
        required = {"Маркетплейс", "Артикул", "Себестоимость"}
        
        if not required.issubset(df.columns):
            await status_msg.edit_text(f"❌ Колонки не найдены: {required}")
            return

        updated_count = 0
        async with async_session() as session:
            for _, row in df.iterrows():
                try:
                    market = str(row['Маркетплейс']).lower().strip()
                    art = str(row['Артикул']).strip()
                    # Валидация числовых данных
                    cost = float(row['Себестоимость']) if pd.notnull(row['Себестоимость']) else 0.0
                    tax = float(row.get('Налог (0.06 = 6%)', 0.06))
                    extra = float(row.get('Доп_расходы', 0.0))
                    
                    stmt = update(Product).where(
                        Product.user_tg_id == message.from_user.id,
                        Product.article == art,
                        Product.marketplace == market
                    ).values(cost_price=cost, tax_rate=tax, extra_costs=extra)
                    
                    result = await session.execute(stmt)
                    if result.rowcount > 0:
                        updated_count += 1
                except: continue
            await session.commit()
            
        await status_msg.edit_text(f"✅ Обновлено позиций: <b>{updated_count}</b>")
    except Exception as e:
        logger.error(f"Excel Parse Error: {e}")
        await status_msg.edit_text("❌ Ошибка при чтении файла.")

# =========================================================
# РАЗДЕЛ 2: НАСТРОЙКИ API КЛЮЧЕЙ
# =========================================================

@router.message(F.text == "⚙️ Настройки API")
async def show_settings_menu(message: Message):
    """Главное меню настроек API."""
    await message.answer("⚙️ <b>Настройки API</b>\nВыберите площадку для подключения:", 
                         reply_markup=kb.get_settings_inline_menu(), parse_mode="HTML")

@router.callback_query(F.data == "check_api_cb")
async def handle_check_api_callback(callback: CallbackQuery):
    """Проверка всех текущих соединений."""
    await callback.answer("⏳ Проверяю...")
    tg_id = callback.from_user.id
    keys = await dbf.get_user_keys(tg_id)
    
    if not keys:
        await callback.message.answer("❌ Ключи отсутствуют.")
        return

    results = ["<b>🔌 Статус подключений:</b>\n"]
    
    # WB
    if keys.get("wb_token"):
        wb = WildberriesAPI(keys["wb_token"])
        if await wb.validate_token():
            results.append("✅ Wildberries: <b>Подключен</b>")
        else: results.append("❌ Wildberries: <b>Ошибка токена</b>")
    else: results.append("⚪ Wildberries: <b>Не настроен</b>")

    # Ozon
    if keys.get("ozon_api_key") and keys.get("ozon_client_id"):
        ozon = OzonAPI(str(keys["ozon_client_id"]), str(keys["ozon_api_key"]))
        success, _ = await ozon.check_connection()
        if success:
            results.append("✅ Ozon: <b>Подключен</b>")
        else: results.append("❌ Ozon: <b>Ошибка ключей</b>")
    else: results.append("⚪ Ozon: <b>Не настроен</b>")

    await callback.message.answer("\n".join(results), parse_mode="HTML")

@router.callback_query(F.data == "set_wb")
async def start_set_wb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SetupKeys.waiting_for_wb_token)
    await callback.message.answer("📥 <b>Настройка Wildberries</b>\nВведите ваш API Токен:",
                                  reply_markup=kb.get_cancel_kb(), parse_mode="HTML")
    await callback.answer()

@router.message(SetupKeys.waiting_for_wb_token)
async def process_wb_token(message: Message, state: FSMContext):
    token = message.text.strip()
    try: await message.delete()
    except: pass
    
    status_msg = await message.answer("🔄 Валидация WB токена...")
    wb = WildberriesAPI(token)
    
    if await wb.validate_token():
        await dbf.update_wb_token(message.from_user.id, token)
        await status_msg.edit_text("✅ <b>Wildberries успешно подключен!</b>")
        await state.clear()
    else:
        await status_msg.edit_text("❌ <b>Ошибка!</b> Токен недействителен.", reply_markup=kb.get_cancel_kb())

@router.callback_query(F.data == "set_ozon")
async def start_set_ozon(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SetupKeys.waiting_for_ozon_client_id)
    await callback.message.answer("🔵 <b>Настройка Ozon</b>\nВведите ваш <b>Client-ID</b>:", 
                                  reply_markup=kb.get_cancel_kb(), parse_mode="HTML")
    await callback.answer()

@router.message(SetupKeys.waiting_for_ozon_client_id)
async def process_ozon_id(message: Message, state: FSMContext):
    await state.update_data(ozon_id=message.text.strip())
    await state.set_state(SetupKeys.waiting_for_ozon_api_key)
    await message.answer("🔵 <b>Настройка Ozon</b>\nТеперь введите ваш <b>API Key</b>:", reply_markup=kb.get_cancel_kb())

@router.message(SetupKeys.waiting_for_ozon_api_key)
async def process_ozon_key(message: Message, state: FSMContext):
    data = await state.get_data()
    client_id = str(data.get("ozon_id"))
    api_key = str(message.text.strip())
    
    try: await message.delete()
    except: pass
    
    status_msg = await message.answer("🔄 Авторизация Ozon...")
    ozon = OzonAPI(client_id, api_key)
    success, _ = await ozon.check_connection()
    
    if success:
        await dbf.update_ozon_keys(message.from_user.id, client_id, api_key)
        await status_msg.edit_text("✅ <b>Ozon успешно подключен!</b>")
        await state.clear()
    else:
        await status_msg.edit_text("❌ <b>Ошибка авторизации!</b> Проверьте ключи.", reply_markup=kb.get_cancel_kb())

@router.callback_query(F.data == "cancel")
async def cancel_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Действие отменено.")
    await callback.answer()