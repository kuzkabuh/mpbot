import logging
from aiogram import Router, F, Bot
from aiogram.filters import CommandStart, StateFilter
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext

# Импортируем наши модули
import keyboards as kb
import db_functions as dbf
# Импортируем классы API для работы с балансом
from wb_api import WildberriesAPI
from ozon_api import OzonAPI
# Так как excel_handlers лежит в той же папке handlers:
from . import excel_handlers as excel 

router = Router()
logger = logging.getLogger(__name__)

# --- ГЛАВНЫЕ КОМАНДЫ ---

@router.message(CommandStart())
async def cmd_start(message: Message):
    """Регистрация и приветствие."""
    user_id = message.from_user.id
    await dbf.register_user(user_id)
    
    keys = await dbf.get_user_keys(user_id)
    has_keys = keys.get('ozon_api_key') or keys.get('wb_token')

    text = (
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        f"Я помогу тебе считать чистую прибыль на маркетплейсах."
    )
    if not has_keys:
        text += "\n\n<b>⚠️ Начни с настройки API ключей в меню ниже.</b>"

    await message.answer(text, reply_markup=kb.get_permanent_menu(), parse_mode="HTML")

# --- НОВЫЙ ОБРАБОТЧИК: МОЙ БАЛАНС ---

@router.message(F.text == "💰 Мой баланс")
async def show_balance(message: Message):
    """
    Получает текущие балансы из API WB и Ozon и выводит пользователю.
    """
    user_id = message.from_user.id
    keys = await dbf.get_user_keys(user_id)
    
    # Проверка наличия ключей
    if not keys.get('wb_token') and not keys.get('ozon_api_key'):
        await message.answer("⚠️ Сначала добавьте API ключи в разделе 'Настройки'.")
        return

    wait_msg = await message.answer("🔄 Запрашиваю данные из маркетплейсов...")

    wb_balance = 0.0
    ozon_balance = 0.0

    # 1. Запрос баланса Wildberries
    if keys.get('wb_token'):
        try:
            wb = WildberriesAPI(keys['wb_token'])
            wb_balance = await wb.get_balance()
            logger.info(f"Баланс WB для {user_id}: {wb_balance}")
        except Exception as e:
            logger.error(f"Ошибка баланса WB: {e}")

    # 2. Запрос баланса Ozon
    if keys.get('ozon_api_key') and keys.get('ozon_client_id'):
        try:
            ozon = OzonAPI(keys['ozon_client_id'], keys['ozon_api_key'])
            ozon_balance = await ozon.get_balance()
            logger.info(f"Баланс Ozon для {user_id}: {ozon_balance}")
        except Exception as e:
            logger.error(f"Ошибка баланса Ozon: {e}")

    # Формируем итоговое сообщение
    # Используем :.2f для отображения копеек и разделения тысяч
    text = (
        "💰 <b>Доступно к выводу:</b>\n\n"
        f"🔵 <b>Ozon:</b> {ozon_balance:,.2f} ₽\n"
        f"🟣 <b>Wildberries:</b> {wb_balance:,.2f} ₽"
    )
    
    await wait_msg.delete() # Удаляем промежуточное сообщение
    await message.answer(text, parse_mode="HTML")

# --- РАЗДЕЛ "МОИ ТОВАРЫ" ---

@router.message(F.text == "📦 Мои товары")
async def show_products_section(message: Message):
    """Меню управления себестоимостью."""
    await message.answer(
        "📦 <b>Управление товарами</b>\n\n"
        "Здесь ты можешь загрузить себестоимость товаров через Excel.\n"
        "1. Скачай текущий шаблон.\n"
        "2. Внеси данные.\n"
        "3. Пришли файл .xlsx обратно.",
        reply_markup=kb.get_products_inline_menu(),
        parse_mode="HTML"
    )

@router.callback_query(F.data == "download_products")
async def handle_download_template(callback: CallbackQuery):
    """Генерация и отправка Excel файла."""
    user_id = callback.from_user.id
    await callback.answer("Генерирую файл...")
    
    try:
        products = await dbf.get_user_products(user_id)
        file_io = await excel.create_products_template(products)
        input_file = BufferedInputFile(
            file_io.getvalue(), 
            filename=f"products_{user_id}.xlsx"
        )
        await callback.message.answer_document(
            input_file,
            caption="📥 Заполни колонку <b>Себестоимость</b> и пришли файл обратно."
        )
    except Exception as e:
        logger.error(f"Ошибка при выгрузке Excel: {e}")
        await callback.message.answer("❌ Не удалось создать файл.")

@router.message(F.document)
async def handle_document_upload(message: Message, bot: Bot):
    """Прием Excel файла от пользователя."""
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        return

    wait_msg = await message.answer("⏳ Обрабатываю файл...")
    try:
        file_info = await bot.get_file(message.document.file_id)
        file_content = await bot.download_file(file_info.file_path)
        parsed_data = await excel.parse_products_excel(file_content.read())
        
        if not parsed_data:
            await wait_msg.edit_text("❌ Ошибка в структуре файла. Проверь заголовки.")
            return

        count = await dbf.bulk_update_products(message.from_user.id, parsed_data)
        await wait_msg.edit_text(
            f"✅ Успешно!\nОбновлено товаров: <b>{count}</b>.\n"
            f"Теперь отчеты будут учитывать себестоимость."
        )
    except Exception as e:
        logger.error(f"Ошибка при загрузке Excel: {e}")
        await wait_msg.edit_text("❌ Произошла ошибка при обработке файла.")

# --- ОБЩИЕ ОБРАБОТЧИКИ ---

@router.callback_query(F.data == "main_menu")
async def back_to_main(callback: CallbackQuery):
    """Возврат в главное меню."""
    await callback.message.answer("🏠 Главное меню", reply_markup=kb.get_permanent_menu())
    await callback.answer()

@router.message(StateFilter("*"), F.text.casefold() == "отмена")
async def cancel_handler(message: Message, state: FSMContext):
    """Сброс состояний FSM."""
    await state.clear()
    await message.answer("🚫 Действие отменено.", reply_markup=kb.get_permanent_menu())