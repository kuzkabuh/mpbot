import asyncio
import logging
import uvicorn
import sys
from logging.handlers import RotatingFileHandler

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone

# Импорт конфигурации и инициализации БД
from config import config
from database import init_db

# Импорт роутеров обработчиков команд
from handlers import common, reports, settings

# Импорт фоновых задач для планировщика
from scheduler_tasks import (
    check_new_orders_task, 
    send_morning_report, 
    check_low_stock_task
)

# Импорт экземпляра FastAPI приложения для админ-панели
from admin_panel import app as admin_app

def setup_logging():
    """
    Комплексная настройка логирования с ротацией файлов.
    """
    logger = logging.getLogger()
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    logger.setLevel(config.log_level)
    
    # Формат: Дата - Имя модуля - Уровень - Сообщение
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 1. Консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Файл (макс 5МБ, храним 5 последних копий)
    file_handler = RotatingFileHandler(
        config.log_file_path, 
        maxBytes=5*1024*1024, 
        backupCount=5, 
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Уменьшаем уровень шума от сторонних библиотек
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

async def main():
    """
    Главная точка входа. Запускает Telegram Bot, FastAPI и APScheduler.
    """
    setup_logging()
    logging.info("=== СТАРТ ПРИЛОЖЕНИЯ MARKETPLACE BOT ===")
    
    # Шаг 1: Инициализация БД
    logging.info("Инициализация базы данных...")
    await init_db()

    # Шаг 2: Инициализация Bot и Dispatcher
    bot = Bot(
        token=config.bot_token.get_secret_value(), 
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    # Регистрируем роутеры
    dp.include_router(common.router)
    dp.include_router(reports.router)
    dp.include_router(settings.router)

    # Шаг 3: Настройка планировщика
    tz = timezone(config.timezone)
    scheduler = AsyncIOScheduler(timezone=tz)
    
    # Задача: Проверка заказов (каждые 5 минут)
    scheduler.add_job(check_new_orders_task, 'interval', minutes=5, args=[bot])
    
    # Задача: Утренний отчет
    try:
        rep_h, rep_m = map(int, config.report_time.split(':'))
        scheduler.add_job(send_morning_report, 'cron', hour=rep_h, minute=rep_m, args=[bot])
    except Exception as e:
        logging.error(f"Ошибка конфига времени отчета: {e}. Ставим 09:00.")
        scheduler.add_job(send_morning_report, 'cron', hour=9, minute=0, args=[bot])
    
    # Задача: Проверка остатков (10:00 и 18:00)
    scheduler.add_job(check_low_stock_task, 'cron', hour='10,18', args=[bot])
    
    scheduler.start()
    logging.info("APScheduler запущен.")

    # Шаг 4: Запуск FastAPI админки в фоне
    server_config = uvicorn.Config(
        admin_app, 
        host=config.web_host, 
        port=config.web_port, 
        log_level="warning"
    )
    server = uvicorn.Server(server_config)
    
    # Создаем задачу для веб-сервера
    web_task = asyncio.create_task(server.serve())
    logging.info(f"Админ-панель: http://{config.web_host}:{config.web_port}")

    # Шаг 5: Запуск Long Polling
    await bot.delete_webhook(drop_pending_updates=True)
    
    try:
        logging.info("Бот вышел в онлайн и готов к работе.")
        # Передаем bot в workflow_data (полезно для middlewares)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logging.critical(f"Ошибка во время работы бота: {e}")
    finally:
        # ЗАВЕРШЕНИЕ РАБОТЫ (Graceful Shutdown)
        logging.warning("Начало процесса остановки...")
        
        # Останавливаем планировщик
        if scheduler.running:
            scheduler.shutdown()
        
        # Закрываем бота
        await bot.session.close()
        
        # Останавливаем FastAPI
        server.should_exit = True
        await web_task
        
        logging.info("Приложение полностью остановлено.")

if __name__ == "__main__":
    if sys.platform == "win32":
        # Исправление для Windows (актуально для асинхронных задач)
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен пользователем.")