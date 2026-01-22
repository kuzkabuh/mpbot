from aiogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    ReplyKeyboardMarkup, 
    KeyboardButton,
    WebAppInfo
)

def get_permanent_menu():
    """
    Главное нижнее меню (Reply Keyboard).
    Обеспечивает быстрый доступ к основным функциям.
    """
    buttons = [
        [
            KeyboardButton(text="📊 Сводка по всем"),
            KeyboardButton(text="💰 Мой баланс") 
        ],
        [
            KeyboardButton(text="📦 Текущие заказы"), 
            KeyboardButton(text="📈 Фин. отчет") # Заменили графики на отчет (согласно reports.py)
        ],
        [
            KeyboardButton(text="📦 Мои товары"),
            KeyboardButton(text="⚙️ Настройки API")
        ]
    ]
    return ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        input_field_placeholder="Выберите раздел меню...",
        is_persistent=True
    )

def get_finance_periods_menu():
    """
    Меню выбора периода для финансового отчета (Чистая прибыль).
    Используется в хендлере show_finance_menu.
    """
    buttons = [
        [
            InlineKeyboardButton(text="🟣 WB: Прибыль за 7 дней", callback_data="fin_wb_7d")
        ],
        [
            InlineKeyboardButton(text="🔵 Ozon: Прибыль за 7 дней (скоро)", callback_data="fin_ozon_7d")
        ],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_products_inline_menu(webapp_url: str = None):
    """
    Инлайн-меню раздела управления товарами (Юнит-экономика).
    """
    buttons = [
        [
            InlineKeyboardButton(text="📥 Скачать Excel шаблон", callback_data="download_products")
        ],
        [
            InlineKeyboardButton(text="📤 Загрузить данные (Excel)", callback_data="upload_instructions")
        ]
    ]
    
    if webapp_url:
        buttons.append([InlineKeyboardButton(text="🌐 Открыть Web-редактор", web_app=WebAppInfo(url=webapp_url))])
        
    buttons.append([InlineKeyboardButton(text="🔄 Синхронизировать список", callback_data="sync_products")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_orders_menu():
    """
    Меню выбора маркетплейса для просмотра текущих заказов (мониторинг).
    """
    buttons = [
        [
            InlineKeyboardButton(text="🟣 Wildberries", callback_data="orders_wb"),
            InlineKeyboardButton(text="🔵 Ozon", callback_data="orders_ozon")
        ],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_settings_inline_menu():
    """
    Меню настроек и финансовой проверки.
    """
    buttons = [
        [
            InlineKeyboardButton(text="🟣 Wildberries API", callback_data="set_wb"),
            InlineKeyboardButton(text="🔵 Ozon API", callback_data="set_ozon")
        ],
        [
            # Убедитесь, что этот callback обрабатывается в ваших хендлерах
            InlineKeyboardButton(text="🔌 Проверить статус API", callback_data="check_api_cb")
        ],
        [
            InlineKeyboardButton(text="⚙️ Налог и Порог остатков", callback_data="setup_profile"),
            InlineKeyboardButton(text="📋 Инструкция", callback_data="help_info")
        ],
        [
            InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_kb():
    """Кнопка отмены для состояний ожидания."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel")]
    ])

def get_back_to_main():
    """Универсальная кнопка возврата."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="main_menu")]
    ])