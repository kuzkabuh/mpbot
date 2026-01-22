import logging
from typing import Any, Awaitable, Callable, Dict
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from aiogram.fsm.context import FSMContext

# Импортируем состояния для безопасности
from states import SetupKeys

# Настройка логгера
logger = logging.getLogger(__name__)

class UserActionLogger(BaseMiddleware):
    """
    Middleware для логирования действий пользователей и безопасности данных.
    Автоматически маскирует API ключи и защищает чувствительную информацию.
    """
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Извлекаем информацию о пользователе и состоянии
        user = data.get("event_from_user")
        state: FSMContext = data.get("state")
        
        user_id = user.id if user else "Unknown"
        username = f"@{user.username}" if user and user.username else "no_username"
        user_info = f"ID:{user_id} | {username}"

        if isinstance(event, Message):
            # Получаем текущее состояние FSM
            current_state = await state.get_state() if state else None
            
            # 1. Защита API токенов
            sensitive_states = [
                SetupKeys.waiting_for_wb_token, 
                SetupKeys.waiting_for_ozon_api_key
            ]
            
            # 2. Логирование сообщений
            if current_state in sensitive_states:
                content = "[SENSITIVE_DATA_MASKED]"
            elif event.document:
                # Маскируем имя файла, если это загрузка Excel с себестоимостью
                content = f"[DOCUMENT: {event.document.file_name}]"
            else:
                content = event.text or f"[{event.content_type}]"
            
            logger.info(f"👤 MSG [{user_info}]: {content}")

        elif isinstance(event, CallbackQuery):
            # 3. Логирование нажатий кнопок
            logger.info(f"🔘 BTN [{user_info}]: data='{event.data}'")

        # Передаем управление дальше по цепочке
        try:
            return await handler(event, data)
        except Exception as e:
            # Централизованное логирование ошибок выполнения хендлеров
            logger.error(f"❌ ERROR [{user_info}]: {e}", exc_info=True)
            raise e

# Дополнительный полезный Middleware для Шага 2
class DatabaseSessionMiddleware(BaseMiddleware):
    """
    Обеспечивает автоматическое управление сессиями БД (опционально, 
    если вы не используете контекстные менеджеры внутри функций).
    """
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Здесь можно внедрить зависимости, которые понадобятся в каждом хендлере
        return await handler(event, data)