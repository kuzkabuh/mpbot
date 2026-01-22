"""
Версия файла: 1.1.0
Описание: Точка входа проекта. Запуск Telegram-бота (aiogram), планировщика APScheduler и FastAPI админ-панели (uvicorn).
Дата изменения: 2026-01-22
Изменения:
- Добавлены стабильные job id + replace_existing, coalesce, misfire_grace_time и max_instances=1 для предотвращения дублей задач.
- Улучшен graceful shutdown: корректная остановка планировщика, uvicorn и закрытие сессии бота.
- Добавлена устойчивость: если админ-панель не стартует, бот продолжает работать.
- Улучшена настройка логирования и шумоподавление сторонних логгеров.
- Подготовлена точка подключения middleware (если используется middlewares.py).
"""

from __future__ import annotations

import asyncio
import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone

from config import config
from database import init_db

from handlers import common, reports, settings

# ВАЖНО: убедись, что используешь один файл задач.
# В проекте у тебя есть scheduler_tasks.py и scheduler_task.py.
# Этот main.py импортирует scheduler_tasks.py как и было ранее.
from scheduler_tasks import (
    check_new_orders_task,
    send_morning_report,
    check_low_stock_task,
)

from admin_panel import app as admin_app


def setup_logging() -> None:
    """
    Комплексная настройка логирования с ротацией файлов.
    """
    root_logger = logging.getLogger()

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(getattr(logging, str(config.log_level).upper(), logging.INFO))

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # 1) Консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 2) Файл (макс 5МБ, храним 5 последних копий)
    file_handler = RotatingFileHandler(
        config.log_file_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Уменьшаем уровень шума от сторонних библиотек
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)
    logging.getLogger("aiogram.dispatcher").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.INFO)


def _build_scheduler(tz_name: str) -> AsyncIOScheduler:
    """
    Создаёт APScheduler с настройками, предотвращающими дубли задач и «залипание» при misfire.
    """
    tz = timezone(tz_name)
    scheduler = AsyncIOScheduler(timezone=tz)

    # Настройки по умолчанию на уровне job:
    # - coalesce=True: если пропущено несколько запусков, выполнить только один
    # - max_instances=1: не выполнять параллельно один и тот же job
    # - misfire_grace_time: если опоздали — всё ещё выполнить в рамках окна
    scheduler.configure(
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 300,  # 5 минут
        }
    )
    return scheduler


def _schedule_jobs(scheduler: AsyncIOScheduler, bot: Bot) -> None:
    """
    Регистрирует задачи APScheduler.
    ВАЖНО: даём ID и replace_existing=True, чтобы при рестарте не плодились дубликаты.
    """
    # 1) Проверка новых заказов (каждые 5 минут)
    scheduler.add_job(
        check_new_orders_task,
        trigger="interval",
        minutes=5,
        args=[bot],
        id="check_new_orders",
        replace_existing=True,
    )

    # 2) Утренний отчет
    rep_h, rep_m = 9, 0
    try:
        rep_h, rep_m = map(int, str(config.report_time).split(":"))
    except Exception as e:
        logging.error(f"Ошибка конфига report_time={config.report_time}: {e}. Используем 09:00.")

    scheduler.add_job(
        send_morning_report,
        trigger="cron",
        hour=rep_h,
        minute=rep_m,
        args=[bot],
        id="send_morning_report",
        replace_existing=True,
    )

    # 3) Проверка остатков (10:00 и 18:00)
    scheduler.add_job(
        check_low_stock_task,
        trigger="cron",
        hour="10,18",
        minute=0,
        args=[bot],
        id="check_low_stock",
        replace_existing=True,
    )


async def _start_admin_panel() -> Optional[asyncio.Task]:
    """
    Запускает FastAPI админку (uvicorn) в фоне.
    Возвращает asyncio.Task или None, если запуск не нужен/невозможен.
    """
    try:
        server_config = uvicorn.Config(
            admin_app,
            host=config.web_host,
            port=config.web_port,
            log_level="warning",
            access_log=False,
        )
        server = uvicorn.Server(server_config)

        async def _serve() -> None:
            try:
                await server.serve()
            except asyncio.CancelledError:
                # корректное завершение
                return
            except Exception as e:
                logging.error(f"Админ-панель упала: {e}")

        task = asyncio.create_task(_serve(), name="admin_panel_server")
        logging.info(f"Админ-панель: http://{config.web_host}:{config.web_port}")
        # ВАЖНО: сохранить server для остановки через task.cancel недостаточно,
        # но для uvicorn.Server serve() обычно корректно завершается по cancel.
        return task
    except Exception as e:
        logging.error(f"Не удалось запустить админ-панель: {e}")
        return None


async def main() -> None:
    """
    Главная точка входа. Запускает Telegram Bot, APScheduler и FastAPI-админку.
    """
    setup_logging()
    logging.info("=== СТАРТ ПРИЛОЖЕНИЯ MARKETPLACE BOT ===")

    # Шаг 1: Инициализация БД
    logging.info("Инициализация базы данных...")
    await init_db()
    logging.info("База данных готова.")

    # Шаг 2: Инициализация Bot и Dispatcher
    bot = Bot(
        token=config.bot_token.get_secret_value(),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # Регистрируем роутеры
    dp.include_router(common.router)
    dp.include_router(reports.router)
    dp.include_router(settings.router)

    # Если у тебя есть middlewares.py — сюда можно подключить:
    # from middlewares import SomeMiddleware
    # dp.message.middleware(SomeMiddleware())

    # Шаг 3: Планировщик
    scheduler = _build_scheduler(config.timezone)
    _schedule_jobs(scheduler, bot)

    scheduler.start()
    logging.info("APScheduler запущен и задачи зарегистрированы.")

    # Шаг 4: Админка FastAPI в фоне
    web_task: Optional[asyncio.Task] = await _start_admin_panel()

    # Шаг 5: Запуск Long Polling
    await bot.delete_webhook(drop_pending_updates=True)

    try:
        logging.info("Бот вышел в онлайн и готов к работе.")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
        logging.info("Остановка по сигналу/прерыванию.")
    except Exception as e:
        logging.critical(f"Критическая ошибка во время работы бота: {e}")
    finally:
        logging.warning("Начало процесса остановки...")

        # Останавливаем планировщик
        try:
            if scheduler.running:
                scheduler.shutdown(wait=False)
        except Exception as e:
            logging.error(f"Ошибка остановки APScheduler: {e}")

        # Останавливаем веб задачу
        if web_task is not None:
            try:
                web_task.cancel()
                await asyncio.gather(web_task, return_exceptions=True)
            except Exception as e:
                logging.error(f"Ошибка остановки админ-панели: {e}")

        # Закрываем сессию бота
        try:
            await bot.session.close()
        except Exception as e:
            logging.error(f"Ошибка закрытия сессии бота: {e}")

        logging.info("Приложение полностью остановлено.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен пользователем.")
