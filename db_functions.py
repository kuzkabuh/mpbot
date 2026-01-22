import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import select, update, func, delete
from sqlalchemy.dialects.sqlite import insert
# Импортируем async_session и модели.
from database import async_session, User, Order, Product, KeywordTrack, KeywordHistory

# Настройка логирования
logger = logging.getLogger(__name__)

# --- РАБОТА С ПОЛЬЗОВАТЕЛЯМИ ---

async def register_user(tg_id: int):
    """Регистрирует нового пользователя или обновляет существующего."""
    async with async_session() as session:
        try:
            # Устанавливаем базовые настройки при первой регистрации
            stmt = insert(User).values(
                tg_id=tg_id,
                notifications_enabled=True,
                stock_threshold=5,
                tax_rate_default=0.06
            ).on_conflict_do_nothing()
            
            await session.execute(stmt)
            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка регистрации {tg_id}: {e}")
            await session.rollback()

async def get_user_tax_rate(tg_id: int) -> float:
    """Получает ставку налога пользователя (по умолчанию 6%)."""
    async with async_session() as session:
        result = await session.execute(select(User.tax_rate_default).where(User.tg_id == tg_id))
        rate = result.scalar_one_or_none()
        return rate if rate is not None else 0.06

async def get_user_keys(tg_id: int) -> Dict[str, Any]:
    """Возвращает все ключи и настройки пользователя для работы API."""
    async with async_session() as session:
        try:
            result = await session.execute(select(User).where(User.tg_id == tg_id))
            user = result.scalar_one_or_none()
            if user:
                return {
                    "wb_token": user.wb_token,
                    "ozon_client_id": user.ozon_client_id,
                    "ozon_api_key": user.ozon_api_key,
                    "threshold": user.stock_threshold,
                    "tax_default": user.tax_rate_default
                }
            return {}
        except Exception as e:
            logger.error(f"Ошибка получения ключей {tg_id}: {e}")
            return {}

async def get_all_active_users() -> List[User]:
    """Возвращает список всех пользователей, у которых настроены токены."""
    async with async_session() as session:
        result = await session.execute(
            select(User).where(
                (User.wb_token != None) | 
                ((User.ozon_client_id != None) & (User.ozon_api_key != None))
            )
        )
        return list(result.scalars().all())

# --- РАБОТА С ТОВАРАМИ И СЕБЕСТОИМОСТЬЮ ---

# ИСПРАВЛЕНО: Добавлен 5-й аргумент 'name' и изменен порядок аргументов для соответствия вызову в settings.py
async def update_product_cost(user_tg_id: int, marketplace: str, article: str, cost: float, name: str = None):
    """
    Обновляет себестоимость и название конкретного товара.
    Аргументы: ID пользователя, маркетплейс, артикул, себестоимость, название.
    """
    async with async_session() as session:
        try:
            # Сначала проверяем существование товара для Upsert логики (вставка или обновление)
            clean_article = str(article).strip().upper()
            clean_market = marketplace.lower()

            # Используем insert с on_conflict_do_update для атомарности
            stmt = insert(Product).values(
                user_tg_id=user_tg_id,
                marketplace=clean_market,
                article=clean_article,
                name=name or f"Товар {clean_article}",
                cost_price=cost
            )

            stmt = stmt.on_conflict_do_update(
                index_elements=['user_tg_id', 'marketplace', 'article'],
                set_={
                    # Обновляем имя всегда, чтобы оно было актуальным из API
                    "name": stmt.excluded.name,
                    # Обновляем цену только если она передана не нулевая (чтобы не затереть текущую при синхронизации остатков)
                    "cost_price": func.coalesce(func.nullif(stmt.excluded.cost_price, 0), Product.cost_price)
                }
            )
            
            await session.execute(stmt)
            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка update_product_cost для {article}: {e}")
            await session.rollback()
            return False

async def bulk_update_products(user_tg_id: int, products_list: List[Dict[str, Any]]) -> int:
    """
    Массовое обновление/вставка товаров. 
    Объединяет товары по артикулу (приведение к верхнему регистру и очистка).
    """
    if not products_list:
        return 0

    async with async_session() as session:
        try:
            count = 0
            for p in products_list:
                clean_article = str(p['article']).strip().upper()
                
                stmt = insert(Product).values(
                    user_tg_id=user_tg_id,
                    marketplace=p['marketplace'].lower(),
                    article=clean_article,
                    name=p['name'],
                    cost_price=p.get('cost_price', 0.0),
                    extra_costs=p.get('extra_costs', 0.0),
                    tax_rate=p.get('tax_rate', 0.06)
                )
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=['user_tg_id', 'marketplace', 'article'],
                    set_={
                        "name": stmt.excluded.name,
                        "cost_price": func.coalesce(func.nullif(stmt.excluded.cost_price, 0), Product.cost_price),
                        "extra_costs": func.coalesce(func.nullif(stmt.excluded.extra_costs, 0), Product.extra_costs)
                    }
                )
                await session.execute(stmt)
                count += 1
            
            await session.commit()
            logger.info(f"User {user_tg_id}: синхронизировано {count} товаров.")
            return count
        except Exception as e:
            logger.error(f"Ошибка массового обновления товаров: {e}")
            await session.rollback()
            return 0

async def get_user_products(user_tg_id: int) -> List[Product]:
    """Загружает список всех товаров пользователя."""
    async with async_session() as session:
        result = await session.execute(
            select(Product).where(Product.user_tg_id == user_tg_id)
        )
        return list(result.scalars().all())

# --- МОНИТОРИНГ ЗАКАЗОВ ---

async def is_order_new(order_id: str, marketplace: str) -> bool:
    """Проверяет, является ли заказ новым (отсутствует в БД)."""
    async with async_session() as session:
        result = await session.execute(
            select(Order.id).where(
                Order.order_id == str(order_id),
                Order.marketplace == marketplace.lower()
            )
        )
        return result.scalar_one_or_none() is None

async def save_order(order_id: str, marketplace: str, amount: float, item_name: str, user_tg_id: int):
    """Сохраняет одиночный заказ для истории."""
    async with async_session() as session:
        try:
            stmt = insert(Order).values(
                order_id=str(order_id),
                marketplace=marketplace.lower(),
                amount=amount,
                item_name=str(item_name)[:255] if item_name else "Н/Д",
                user_id=user_tg_id,
                created_at=datetime.now()
            ).on_conflict_do_nothing()
            
            await session.execute(stmt)
            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения заказа {order_id}: {e}")
            await session.rollback()

async def bulk_save_orders(orders_data: List[Dict[str, Any]]):
    """Массовое сохранение заказов для оптимизации."""
    if not orders_data:
        return
    
    async with async_session() as session:
        try:
            for order in orders_data:
                stmt = insert(Order).values(
                    order_id=str(order['order_id']),
                    marketplace=order['marketplace'].lower(),
                    amount=order['amount'],
                    item_name=str(order.get('item_name', 'Н/Д'))[:255],
                    user_id=order['user_id'],
                    created_at=order.get('created_at', datetime.now())
                ).on_conflict_do_nothing()
                await session.execute(stmt)
            
            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка массового сохранения заказов: {e}")
            await session.rollback()

# --- ОТЧЕТНОСТЬ И АНАЛИТИКА ---

async def get_analytics_data(user_tg_id: int) -> Dict[str, Dict[str, float]]:
    """
    Получает полные данные о товарах (себестоимость и налог).
    Возвращает словарь { 'marketplace:article': {'cost': 100, 'tax': 0.06} }
    """
    async with async_session() as session:
        result = await session.execute(
            select(Product).where(Product.user_tg_id == user_tg_id)
        )
        products = result.scalars().all()
        return {
            f"{p.marketplace}:{p.article}": {
                "cost": (p.cost_price or 0.0) + (p.extra_costs or 0.0),
                "tax": p.tax_rate or 0.06
            } for p in products
        }

async def update_user_profile(tg_id: int, **kwargs):
    """Универсальный метод для обновления настроек пользователя."""
    async with async_session() as session:
        try:
            await session.execute(
                update(User).where(User.tg_id == tg_id).values(**kwargs)
            )
            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка обновления профиля {tg_id}: {e}")
            await session.rollback()

async def get_orders_stats(user_tg_id: int, days: int = 1) -> List[Order]:
    """Получает заказы из БД за определенный период для отчетов."""
    async with async_session() as session:
        date_limit = datetime.now() - timedelta(days=days)
        result = await session.execute(
            select(Order).where(
                Order.user_id == user_tg_id,
                Order.created_at >= date_limit
            )
        )
        return list(result.scalars().all())

# --- МОНИТОРИНГ КЛЮЧЕВЫХ СЛОВ ---

async def add_keyword_track(user_id: int, marketplace: str, article: str, keyword: str):
    """Добавляет новый запрос для отслеживания позиций."""
    async with async_session() as session:
        try:
            stmt = insert(KeywordTrack).values(
                user_tg_id=user_id,
                marketplace=marketplace.lower(),
                article=str(article).strip().upper(),
                keyword=keyword.lower().strip()
            ).on_conflict_do_nothing()
            await session.execute(stmt)
            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления ключа: {e}")
            await session.rollback()
            return False

async def get_user_keywords(user_id: int) -> List[KeywordTrack]:
    """Возвращает список всех отслеживаемых ключей пользователя."""
    async with async_session() as session:
        result = await session.execute(
            select(KeywordTrack).where(KeywordTrack.user_tg_id == user_id)
        )
        return list(result.scalars().all())

async def delete_keyword_track(track_id: int, user_id: int):
    """Удаляет отслеживание ключевого слова."""
    async with async_session() as session:
        try:
            await session.execute(
                delete(KeywordTrack).where(
                    KeywordTrack.id == track_id,
                    KeywordTrack.user_tg_id == user_id
                )
            )
            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка удаления ключа {track_id}: {e}")
            await session.rollback()