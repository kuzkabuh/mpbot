import logging
import io
import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext

from sqlalchemy import select

import keyboards as kb
import db_functions as dbf
from states import SetupKeys
from wb_api import WildberriesAPI
from ozon_api import OzonAPI
from database import async_session, Product

router = Router(name="settings_router")
logger = logging.getLogger(__name__)

# Разрешаем только Excel
_EXCEL_SUFFIXES = (".xlsx", ".xls")

# Ожидаемые колонки Excel
COL_MARKETPLACE = "Маркетплейс"
COL_ARTICLE = "Артикул"
COL_NAME = "Название"
COL_COST = "Себестоимость"
COL_TAX = "Налог (0.06 = 6%)"
COL_EXTRA = "Доп_расходы"

REQUIRED_COLUMNS = {COL_MARKETPLACE, COL_ARTICLE, COL_COST}


def _safe_str(value: Any, max_len: int = 255, default: str = "") -> str:
    s = str(value).strip() if value is not None else default
    if not s:
        s = default
    return s[:max_len]


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


def _looks_like_products_template(file_name: str) -> bool:
    """
    Лёгкий фильтр, чтобы не обрабатывать любой Excel.
    Разрешаем:
    - products_<tg_id>.xlsx
    - products.xlsx
    """
    name = (file_name or "").lower().strip()
    return name.startswith("products_") or name == "products.xlsx" or name == "products.xls"


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
        "1) <b>Excel</b> — выгрузите список, заполните данные и отправьте файл обратно.\n"
        "2) <b>Web App</b> — редактирование в браузере (в разработке).\n"
        "3) <b>Синхронизация</b> — обновите список артикулов из личных кабинетов."
    )

    # TODO: заменить на реальный домен/URL webapp
    webapp_url = "https://your-domain.com/webapp"
    await message.answer(text, reply_markup=kb.get_products_inline_menu(webapp_url), parse_mode="HTML")


@router.callback_query(F.data == "download_products")
async def cb_download_products(callback: CallbackQuery):
    """Генерация Excel файла с данными о товарах пользователя."""
    tg_id = callback.from_user.id

    async with async_session() as session:
        res = await session.execute(select(Product).where(Product.user_tg_id == tg_id))
        products = list(res.scalars().all())

    if not products:
        await callback.answer("❌ Список пуст. Сначала нажмите «Синхронизировать».", show_alert=True)
        return

    temp_msg = await callback.message.answer("⏳ Генерирую файл...")

    try:
        rows: List[Dict[str, Any]] = []
        for p in products:
            rows.append({
                COL_MARKETPLACE: _safe_str(p.marketplace, 32, "").upper(),
                COL_ARTICLE: _safe_str(p.article, 128, ""),
                COL_NAME: _safe_str(p.name, 255, ""),
                COL_COST: float(p.cost_price or 0.0),
                COL_TAX: float(p.tax_rate or 0.06),
                COL_EXTRA: float(p.extra_costs or 0.0),
            })

        df = pd.DataFrame(rows)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Products")

        document = BufferedInputFile(output.getvalue(), filename=f"products_{tg_id}.xlsx")

        await callback.message.answer_document(
            document,
            caption="✅ <b>Файл готов!</b>\n\nЗаполните колонки и отправьте файл боту.",
            parse_mode="HTML",
        )
        await temp_msg.delete()
    except Exception as e:
        logger.error(f"Ошибка создания Excel (tg_id={tg_id}): {e}")
        await temp_msg.edit_text("❌ Ошибка при создании файла.")
    finally:
        await callback.answer()


@router.callback_query(F.data == "sync_products")
async def cb_sync_products(callback: CallbackQuery):
    """
    Синхронизация артикулов через API площадок.
    Использует методы get_all_products() и dbf.bulk_update_products().
    """
    tg_id = callback.from_user.id
    await dbf.register_user(tg_id)

    status_msg = await callback.message.answer("🔄 Синхронизирую товары... Это может занять время.")

    keys = await dbf.get_user_keys(tg_id)
    if not keys:
        await status_msg.edit_text("❌ Настройте API ключи в настройках.")
        await callback.answer()
        return

    async def sync_wb() -> int:
        if keys.get("wb_token"):
            try:
                wb = WildberriesAPI(keys["wb_token"])
                products = await wb.get_all_products()  # ожидаем список dict
                if products:
                    return int(await dbf.bulk_update_products(tg_id, products))
            except Exception as e:
                logger.error(f"WB sync error (tg_id={tg_id}): {e}")
        return 0

    async def sync_ozon() -> int:
        if keys.get("ozon_api_key") and keys.get("ozon_client_id"):
            try:
                ozon = OzonAPI(keys["ozon_client_id"], keys["ozon_api_key"])
                products = await ozon.get_all_products()
                if products:
                    return int(await dbf.bulk_update_products(tg_id, products))
            except Exception as e:
                logger.error(f"Ozon sync error (tg_id={tg_id}): {e}")
        return 0

    wb_count, oz_count = await asyncio.gather(sync_wb(), sync_ozon())

    await status_msg.edit_text(
        "✅ <b>Синхронизация завершена!</b>\n\n"
        f"Wildberries: <b>{wb_count}</b> товаров\n"
        f"Ozon: <b>{oz_count}</b> товаров\n\n"
        "Теперь вы можете выгрузить Excel для настройки себестоимости.",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(F.document)
async def handle_products_excel(message: Message):
    """
    Массовое обновление/добавление себестоимости из присланного Excel.
    Важно:
    - не обновляем через raw UPDATE, а используем dbf.bulk_update_products (upsert + нормализация)
    - фильтруем «левые» Excel
    """
    doc = message.document
    if not doc or not doc.file_name:
        return

    file_name = doc.file_name
    if not file_name.lower().endswith(_EXCEL_SUFFIXES):
        return

    # Первый фильтр — по названию файла (быстро отсеивает большинство «левых» excel)
    if not _looks_like_products_template(file_name):
        # всё равно можем попробовать второй фильтр по колонкам, но аккуратно:
        # чтобы не грузить тяжёлые файлы, лучше отказать сразу.
        await message.answer(
            "❌ Похоже, это не шаблон товаров.\n"
            "Пожалуйста, сначала нажмите «📦 Мои товары → Excel → Выгрузить файл», "
            "заполните его и отправьте обратно."
        )
        return

    status_msg = await message.answer("⏳ Обрабатываю Excel...")

    try:
        file = await message.bot.get_file(doc.file_id)
        downloaded = await message.bot.download_file(file.file_path)
        content = downloaded.read()
        df = pd.read_excel(io.BytesIO(content))

        # Нормализация заголовков
        df.columns = [str(c).strip() for c in df.columns]

        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            await status_msg.edit_text(
                "❌ В файле не найдены обязательные колонки:\n"
                f"<code>{', '.join(missing)}</code>\n\n"
                "Ожидаемые колонки минимум:\n"
                f"<code>{COL_MARKETPLACE}, {COL_ARTICLE}, {COL_COST}</code>",
                parse_mode="HTML",
            )
            return

        products: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            market = _safe_str(row.get(COL_MARKETPLACE), 32, "").lower()
            article = _safe_str(row.get(COL_ARTICLE), 128, "")
            if not market or not article:
                continue

            name = _safe_str(row.get(COL_NAME), 255, "") if COL_NAME in df.columns else ""
            cost = _safe_float(row.get(COL_COST), 0.0)
            tax = _safe_float(row.get(COL_TAX), 0.06) if COL_TAX in df.columns else 0.06
            extra = _safe_float(row.get(COL_EXTRA), 0.0) if COL_EXTRA in df.columns else 0.0

            products.append({
                "marketplace": market,
                "article": article,
                "name": name,
                "cost_price": cost,
                "tax_rate": tax,
                "extra_costs": extra,
            })

        if not products:
            await status_msg.edit_text("❌ В файле нет корректных строк для обновления.")
            return

        # upsert + нормализация внутри bulk_update_products
        updated = await dbf.bulk_update_products(message.from_user.id, products)

        await status_msg.edit_text(
            f"✅ Готово! Обработано позиций: <b>{len(products)}</b>\n"
            f"✅ Синхронизировано (upsert): <b>{updated}</b>",
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"Excel Parse Error (tg_id={message.from_user.id}): {e}")
        await status_msg.edit_text("❌ Ошибка при чтении файла. Проверьте, что это корректный Excel (.xlsx).")


# =========================================================
# РАЗДЕЛ 2: НАСТРОЙКИ API КЛЮЧЕЙ
# =========================================================

@router.message(F.text == "⚙️ Настройки API")
async def show_settings_menu(message: Message):
    await dbf.register_user(message.from_user.id)
    await message.answer(
        "⚙️ <b>Настройки API</b>\nВыберите площадку для подключения:",
        reply_markup=kb.get_settings_inline_menu(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "check_api_cb")
async def handle_check_api_callback(callback: CallbackQuery):
    await callback.answer("⏳ Проверяю...")
    tg_id = callback.from_user.id
    keys = await dbf.get_user_keys(tg_id)

    if not keys:
        await callback.message.answer("❌ Ключи отсутствуют.")
        return

    results = ["<b>🔌 Статус подключений:</b>\n"]

    # WB
    wb_token = keys.get("wb_token")
    if wb_token:
        try:
            wb = WildberriesAPI(wb_token)
            ok = await wb.validate_token()
            results.append("✅ Wildberries: <b>Подключен</b>" if ok else "❌ Wildberries: <b>Ошибка токена</b>")
        except Exception as e:
            logger.error(f"WB validate error (tg_id={tg_id}): {e}")
            results.append("❌ Wildberries: <b>Ошибка проверки</b>")
    else:
        results.append("⚪ Wildberries: <b>Не настроен</b>")

    # Ozon
    ozon_client_id = keys.get("ozon_client_id")
    ozon_api_key = keys.get("ozon_api_key")
    if ozon_client_id and ozon_api_key:
        try:
            ozon = OzonAPI(str(ozon_client_id), str(ozon_api_key))
            success, _ = await ozon.check_connection()
            results.append("✅ Ozon: <b>Подключен</b>" if success else "❌ Ozon: <b>Ошибка ключей</b>")
        except Exception as e:
            logger.error(f"Ozon validate error (tg_id={tg_id}): {e}")
            results.append("❌ Ozon: <b>Ошибка проверки</b>")
    else:
        results.append("⚪ Ozon: <b>Не настроен</b>")

    await callback.message.answer("\n".join(results), parse_mode="HTML")


@router.callback_query(F.data == "set_wb")
async def start_set_wb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SetupKeys.waiting_for_wb_token)
    await callback.message.answer(
        "📥 <b>Настройка Wildberries</b>\nВведите ваш API токен:",
        reply_markup=kb.get_cancel_kb(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(SetupKeys.waiting_for_wb_token)
async def process_wb_token(message: Message, state: FSMContext):
    token = (message.text or "").strip()
    try:
        await message.delete()
    except Exception:
        pass

    if not token:
        await message.answer("❌ Токен пустой. Введите токен ещё раз.", reply_markup=kb.get_cancel_kb())
        return

    status_msg = await message.answer("🔄 Проверяю WB токен...")

    try:
        wb = WildberriesAPI(token)
        ok = await wb.validate_token()
    except Exception as e:
        logger.error(f"WB validate error (tg_id={message.from_user.id}): {e}")
        ok = False

    if ok:
        await dbf.update_wb_token(message.from_user.id, token)
        await status_msg.edit_text("✅ <b>Wildberries успешно подключен!</b>", parse_mode="HTML")
        await state.clear()
    else:
        await status_msg.edit_text(
            "❌ <b>Ошибка!</b> Токен недействителен.\nПроверьте токен и попробуйте ещё раз.",
            reply_markup=kb.get_cancel_kb(),
            parse_mode="HTML",
        )


@router.callback_query(F.data == "set_ozon")
async def start_set_ozon(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SetupKeys.waiting_for_ozon_client_id)
    await callback.message.answer(
        "🔵 <b>Настройка Ozon</b>\nВведите ваш <b>Client-ID</b>:",
        reply_markup=kb.get_cancel_kb(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(SetupKeys.waiting_for_ozon_client_id)
async def process_ozon_id(message: Message, state: FSMContext):
    cid = (message.text or "").strip()
    if not cid:
        await message.answer("❌ Client-ID пустой. Введите Client-ID ещё раз.", reply_markup=kb.get_cancel_kb())
        return

    await state.update_data(ozon_id=cid)
    await state.set_state(SetupKeys.waiting_for_ozon_api_key)
    await message.answer(
        "🔵 <b>Настройка Ozon</b>\nТеперь введите ваш <b>API Key</b>:",
        reply_markup=kb.get_cancel_kb(),
        parse_mode="HTML",
    )


@router.message(SetupKeys.waiting_for_ozon_api_key)
async def process_ozon_key(message: Message, state: FSMContext):
    data = await state.get_data()
    client_id = str(data.get("ozon_id") or "").strip()
    api_key = (message.text or "").strip()

    try:
        await message.delete()
    except Exception:
        pass

    if not client_id or not api_key:
        await message.answer("❌ Client-ID или API Key пустые. Начните настройку заново.", reply_markup=kb.get_cancel_kb())
        return

    status_msg = await message.answer("🔄 Проверяю ключи Ozon...")

    try:
        ozon = OzonAPI(client_id, api_key)
        success, _ = await ozon.check_connection()
    except Exception as e:
        logger.error(f"Ozon validate error (tg_id={message.from_user.id}): {e}")
        success = False

    if success:
        await dbf.update_ozon_keys(message.from_user.id, client_id, api_key)
        await status_msg.edit_text("✅ <b>Ozon успешно подключен!</b>", parse_mode="HTML")
        await state.clear()
    else:
        await status_msg.edit_text(
            "❌ <b>Ошибка авторизации!</b> Проверьте Client-ID и API Key.",
            reply_markup=kb.get_cancel_kb(),
            parse_mode="HTML",
        )


@router.callback_query(F.data == "cancel")
async def cancel_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Действие отменено.")
    await callback.answer()
