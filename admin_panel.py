import html
import secrets
import logging
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import select, func

# Импортируем настройки и базу данных
from config import config
from database import async_session, User, Product, Order

# Инициализируем FastAPI
app = FastAPI(title="Marketplace Bot Admin")
security = HTTPBasic()

# Настройка логирования для самой админки
logger = logging.getLogger(__name__)

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    """
    HTTP Basic авторизация. Данные берутся из config (из .env файла).
    """
    # Предполагается, что в config добавлены поля admin_user и admin_pass
    # Если их нет в config, используем значения по умолчанию (но лучше добавить в .env)
    correct_user = getattr(config, "admin_user", "admin")
    correct_pass = getattr(config, "admin_pass", "secure_password_123")

    is_user_ok = secrets.compare_digest(credentials.username.encode("utf8"), correct_user.encode("utf8"))
    is_pass_ok = secrets.compare_digest(credentials.password.encode("utf8"), correct_pass.encode("utf8"))

    if not (is_user_ok and is_pass_ok):
        logger.warning(f"⚠️ Неудачная попытка входа в админ-панель: {credentials.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Доступ запрещен",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(username: str = Depends(authenticate)):
    """
    Главная страница мониторинга: Статистика БД + Логи.
    """
    # 1. Сбор статистики из БД
    async with async_session() as session:
        try:
            user_count = (await session.execute(select(func.count(User.id)))).scalar() or 0
            product_count = (await session.execute(select(func.count(Product.id)))).scalar() or 0
            order_count = (await session.execute(select(func.count(Order.id)))).scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения статистики БД: {e}")
            user_count, product_count, order_count = "Error", "Error", "Error"

    # 2. Чтение логов (используем путь из конфига)
    log_file = config.log_file_path
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            # Читаем последние 100 строк для производительности
            lines = f.readlines()[-100:]
            lines.reverse()
    except FileNotFoundError:
        lines = ["Файл логов еще не создан. Проверьте путь в .env"]
    except Exception as e:
        lines = [f"Ошибка чтения логов: {e}"]

    log_rows = ""
    for line in lines:
        bg_color = "#ffffff"
        text_color = "#2c3e50"
        
        if "ERROR" in line or "CRITICAL" in line:
            bg_color = "#f8d7da"
            text_color = "#721c24"
        elif "WARNING" in line:
            bg_color = "#fff3cd"
            text_color = "#856404"
        elif "INFO" in line:
            bg_color = "#d1ecf1"
            text_color = "#0c5460"
        
        log_rows += f"""
        <tr style='background-color: {bg_color}; color: {text_color};'>
            <td style='border-bottom: 1px solid rgba(0,0,0,0.05);'>{html.escape(line)}</td>
        </tr>
        """

    # 3. HTML Шаблон с улучшенным стилем
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MarketBot Admin</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f4f7f6; margin: 0; padding: 20px; color: #333; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; border-bottom: 2px solid #ddd; padding-bottom: 10px; }}
            .stats-container {{ display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 30px; }}
            .card {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); flex: 1; min-width: 250px; text-align: center; border-top: 4px solid #3498db; }}
            .card h3 {{ margin: 0; color: #7f8c8d; font-size: 14px; text-transform: uppercase; letter-spacing: 1px; }}
            .card p {{ font-size: 32px; font-weight: bold; margin: 10px 0 0; color: #2c3e50; }}
            .log-section {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
            table {{ width: 100%; border-collapse: collapse; }}
            td {{ padding: 10px; font-size: 13px; font-family: 'Consolas', 'Monaco', monospace; line-height: 1.5; }}
            .status-live {{ display: inline-block; width: 10px; height: 10px; background: #2ecc71; border-radius: 50%; margin-right: 5px; animation: blink 2s infinite; }}
            @keyframes blink {{ 0% {{ opacity: 1; }} 50% {{ opacity: 0.3; }} 100% {{ opacity: 1; }} }}
            h2 {{ margin: 0; font-weight: 600; }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h2><span class="status-live"></span> Панель управления MarketBot</h2>
                <div style="font-size: 14px; color: #7f8c8d;">Администратор: <b>{username}</b></div>
            </header>
            
            <div class="stats-container">
                <div class="card"><h3>👤 Пользователей</h3><p>{user_count}</p></div>
                <div class="card" style="border-top-color: #e67e22;"><h3>📦 Товаров в базе</h3><p>{product_count}</p></div>
                <div class="card" style="border-top-color: #2ecc71;"><h3>💰 Заказов обработано</h3><p>{order_count}</p></div>
            </div>

            <div class="log-section">
                <h3 style="margin-top: 0; color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 10px;">📝 Журнал событий (последние 100)</h3>
                <div style="overflow-x: auto;">
                    <table>{log_rows}</table>
                </div>
            </div>
        </div>

        <script>
            // Автообновление каждые 30 секунд
            setTimeout(function(){{ location.reload(); }}, 30000);
        </script>
    </body>
    </html>
    """
    return html_content