from aiogram.fsm.state import StatesGroup, State

class SetupKeys(StatesGroup):
    """
    Машина состояний (FSM) для процесса настройки API ключей 
    и пользовательских предпочтений.
    """
    
    # --- Wildberries ---
    # Ожидание токена (Стандартный API ключ)
    waiting_for_wb_token = State()
    
    # --- Ozon ---
    # Шаг 1: Client-ID
    waiting_for_ozon_client_id = State()
    # Шаг 2: API-key
    waiting_for_ozon_api_key = State()

    # --- Юнит-экономика (НОВОЕ для Шага 1) ---
    # Состояние, когда пользователь нажал "Загрузить Excel" 
    # и бот ждет от него файл .xlsx с себестоимостью
    waiting_for_products_excel = State()

    # --- Общие настройки ---
    # Порог критических остатков
    waiting_for_stock_threshold = State()
    
    # Время автоматической рассылки утреннего отчета
    waiting_for_report_time = State()