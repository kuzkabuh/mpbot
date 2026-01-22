from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, Field, ValidationError, AliasChoices
import logging
import sys
import os

class Settings(BaseSettings):
    """
    Класс конфигурации приложения. 
    Использует Pydantic Settings для автоматической загрузки переменных из .env.
    """
    
    # --- Telegram ---
    bot_token: SecretStr
    admin_id: int = Field(default=0, validation_alias=AliasChoices('admin_id', 'admin_tg_id'))

    # --- Database ---
    db_url: str = Field(
        default="sqlite+aiosqlite:///./bot_database.db",
        validation_alias=AliasChoices('db_url', 'database_url')
    )

    # --- Logging & Time ---
    log_level: str = Field(default="INFO")
    timezone: str = Field(default="Europe/Moscow")
    log_file_path: str = Field(default="bot_log.log")
    
    # ИСПРАВЛЕНО: Добавлено поле, которое запрашивает main.py
    report_time: str = Field(default="09:00")

    # --- Web Admin (FastAPI) ---
    web_host: str = Field(default="0.0.0.0")
    web_port: int = Field(default=8000)
    admin_user: str = Field(default="admin")
    admin_pass: str = Field(default="admin")

    # --- API Settings (Шаг 2) ---
    api_retry_attempts: int = Field(default=3)
    temp_files_path: str = Field(default="./temp")

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False
    )

# Попытка инициализации объекта настроек
try:
    config = Settings()

    # Создаем папку для временных файлов, если её нет
    if not os.path.exists(config.temp_files_path):
        os.makedirs(config.temp_files_path)
    
    # ПРИМЕЧАНИЕ: Настройка logging.basicConfig удалена отсюда, 
    # так как она дублирует настройку в main.py и вызывает двойные логи.
    
except ValidationError as e:
    print(f"❌ Критическая ошибка валидации .env:")
    for error in e.errors():
        location = " -> ".join(map(str, error['loc']))
        print(f"   - Поле [{location}]: {error['msg']}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Ошибка инициализации конфигурации: {e}")
    sys.exit(1)