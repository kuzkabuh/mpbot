"""
Версия файла: 1.2.0
Описание: DB-функции для Telegram-бота аналитики продаж (WB/Ozon): пользователи, ключи, товары, заказы, ключевые слова, аналитика.
Дата изменения: 2026-01-22
Изменения:
- Приведены в полное соответствие с текущими моделями database.py:
  * Order: используется поле order_date (вместо несуществующего created_at)
  * KeywordTrack: используется поле user_id (вместо несуществующего user_tg_id)
  * KeywordHistory: используются поля check_date и position (без user_id/checked_at)
- UPSERT сделан кросс-СУБД (SQLite/PostgreSQL) через выбор диалекта engine.dialect.name.
- is_order_new теперь корректно учитывает user_tg_id (tg_id) и предотвращает коллизии между пользователями.
- save_order/bulk_save_orders: единая нормализация и запись в Order.order_date.
- bulk_update_products/update_product_cost: безопасные upsert-операции, защита от затирания cost/extra нулями.
- Функции работы с keywords исправлены под реальные поля моделей.
- Повышена устойчивость: rollback, логирование контекста, мягкие дефолты.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import select, update, func, delete, and_, or_
from sqlalchemy.sql import Insert

from database import async_session, engine, User, Order, Product, KeywordTrack, KeywordHistory

logger = logging.getLogger(__name__)


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (нормализация и безопасность)
# =============================================================================

def _norm_marketplace(value: Any) -> str:
    """
    Приводит маркетплейс к канону: 'wb' или 'ozon'.
    Не бросает исключений: в худшем случае возвращает нижний регистр исходного.
    """
    s = str(value or "").strip().lower()
    if s in ("wildberries", "wb", "w"):
        return "wb"
    if s in ("ozon", "o3", "o"):
        return "ozon"
    return s


def _norm_article(value: Any) -> str:
    """
    Нормализация артикула/offer_id/nmId.
    Важно: приводим к строке, убираем пробелы, upper() — чтобы совпадало в БД.
    """
    return str(value or "").strip().upper()


def _norm_keyword(value: Any) -> str:
    """Нормализация ключевого слова для трекинга позиций."""
    return str(value or "").strip().lower()


def _safe_str(value: Any, max_len: int = 255, default: str = "Н/Д") -> str:
    """Безопасная строка для сохранения в БД."""
    s = str(value).strip() if value is not None else default
    if not s:
        s = default
    return s[:max_len]


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Безопасное приведение к float. Пустые/битые значения -> default."""
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


def _dialect_name() -> str:
    """Текущий диалект SQLAlchemy engine."""
    try:
        return str(getattr(engine, "dialect", None).name).lower()
    except Exception:
        return ""


def _insert_stmt(model) -> Insert:
    """
    Возвращает Insert для текущего диалекта, чтобы поддерживать on_conflict_do_update / do_nothing.
    SQLite: sqlalchemy.dialects.sqlite.insert
    Postgres: sqlalchemy.dialects.postgresql.insert
    """
    d = _dialect_name()
    if d == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert  # type: ignore
        return sqlite_insert(model)
    if d in ("postgresql", "postgres"):
        from sqlalchemy.dialects.postgresql import insert as pg_insert  # type: ignore
        return pg_insert(model)
    # Фоллбек: может не поддержать on_conflict_*, но хотя бы не упадём на импорте.
    from sqlalchemy import insert as sa_insert  # type: ignore
    return sa_insert(model)


def _supports_on_conflict() -> bool:
    """Поддерживает ли диалект on_conflict_do_*."""
    return _dialect_name() in ("sqlite", "postgresql", "postgres")


# =============================================================================
# РАБОТА С ПОЛЬЗОВАТЕЛЯМИ
# =============================================================================

async def register_user(tg_id: int) -> None:
    """
    Регистрирует нового пользователя или гарантирует, что у существующего
    заполнены дефолтные значения (не затирая токены/ключи).
    """
    async with async_session() as session:
        try:
            if _supports_on_conflict():
                stmt = _insert_stmt(User).values(
                    tg_id=tg_id,
                    notifications_enabled=True,
                    stock_threshold=5,
                    tax_rate_default=0.06,
                ).on_conflict_do_nothing()
                await session.execute(stmt)
            else:
                # Фоллбек для редких диалектов: проверяем вручную
                res = await session.execute(select(User.id).where(User.tg_id == tg_id))
                if res.scalar_one_or_none() is None:
                    session.add(
                        User(
                            tg_id=tg_id,
                            notifications_enabled=True,
                            stock_threshold=5,
                            tax_rate_default=0.06,
                        )
                    )

            # Для существующего: добиваем дефолты, если они None
            await session.execute(
                update(User)
                .where(User.tg_id == tg_id)
                .values(
                    notifications_enabled=func.coalesce(User.notifications_enabled, True),
                    stock_threshold=func.coalesce(User.stock_threshold, 5),
                    tax_rate_default=func.coalesce(User.tax_rate_default, 0.06),
                )
            )

            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка register_user (tg_id={tg_id}): {e}")
            await session.rollback()


async def get_user_tax_rate(tg_id: int) -> float:
    """Получает ставку налога пользователя (по умолчанию 6%)."""
    async with async_session() as session:
        try:
            result = await session.execute(
                select(User.tax_rate_default).where(User.tg_id == tg_id)
            )
            rate = result.scalar_one_or_none()
            return float(rate) if rate is not None else 0.06
        except Exception as e:
            logger.error(f"Ошибка get_user_tax_rate (tg_id={tg_id}): {e}")
            return 0.06


async def get_user_keys(tg_id: int) -> Dict[str, Any]:
    """
    Возвращает ключи и настройки пользователя для работы с API.
    Важно: ключи могут быть пустыми строками — считаем их как "нет".
    """
    async with async_session() as session:
        try:
            result = await session.execute(select(User).where(User.tg_id == tg_id))
            user = result.scalar_one_or_none()
            if not user:
                return {}

            wb_token = (user.wb_token or "").strip() if user.wb_token is not None else ""
            ozon_client_id = (user.ozon_client_id or "").strip() if user.ozon_client_id is not None else ""
            ozon_api_key = (user.ozon_api_key or "").strip() if user.ozon_api_key is not None else ""

            return {
                "wb_token": wb_token if wb_token else None,
                "ozon_client_id": ozon_client_id if ozon_client_id else None,
                "ozon_api_key": ozon_api_key if ozon_api_key else None,
                "threshold": int(user.stock_threshold) if user.stock_threshold is not None else 5,
                "tax_default": float(user.tax_rate_default) if user.tax_rate_default is not None else 0.06,
                "notifications_enabled": bool(user.notifications_enabled) if user.notifications_enabled is not None else True,
            }
        except Exception as e:
            logger.error(f"Ошибка get_user_keys (tg_id={tg_id}): {e}")
            return {}


async def get_all_active_users(only_notifications_enabled: bool = False) -> List[User]:
    """
    Возвращает список всех пользователей, у которых настроены токены.
    По желанию можно возвращать только тех, у кого включены уведомления.
    """
    async with async_session() as session:
        try:
            cond_wb = and_(User.wb_token.isnot(None), func.length(func.trim(User.wb_token)) > 0)
            cond_oz = and_(
                User.ozon_client_id.isnot(None),
                func.length(func.trim(User.ozon_client_id)) > 0,
                User.ozon_api_key.isnot(None),
                func.length(func.trim(User.ozon_api_key)) > 0,
            )

            q = select(User).where(or_(cond_wb, cond_oz))
            if only_notifications_enabled:
                q = q.where(User.notifications_enabled.is_(True))

            res = await session.execute(q)
            return list(res.scalars().all())
        except Exception as e:
            logger.error(f"Ошибка get_all_active_users: {e}")
            return []


async def update_user_profile(tg_id: int, **kwargs) -> bool:
    """
    Универсальный метод для обновления настроек пользователя.
    Возвращает True/False для удобства обработчиков.
    """
    async with async_session() as session:
        try:
            await session.execute(update(User).where(User.tg_id == tg_id).values(**kwargs))
            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка update_user_profile (tg_id={tg_id}, keys={list(kwargs.keys())}): {e}")
            await session.rollback()
            return False


async def set_notifications_enabled(tg_id: int, enabled: bool) -> bool:
    """Включить/выключить уведомления пользователя."""
    return await update_user_profile(tg_id, notifications_enabled=bool(enabled))


async def set_stock_threshold(tg_id: int, threshold: int) -> bool:
    """Установить порог низких остатков."""
    try:
        t = int(threshold)
        if t < 0:
            t = 0
    except Exception:
        t = 5
    return await update_user_profile(tg_id, stock_threshold=t)


async def set_tax_rate_default(tg_id: int, tax_rate: float) -> bool:
    """Установить ставку налога по умолчанию (например 0.06)."""
    rate = _safe_float(tax_rate, 0.06)
    if rate < 0:
        rate = 0.0
    if rate > 1:
        rate = rate / 100.0
    return await update_user_profile(tg_id, tax_rate_default=rate)


# =============================================================================
# РАБОТА С КЛЮЧАМИ API
# =============================================================================

async def update_wb_token(tg_id: int, token: str) -> bool:
    """Обновление токена WB пользователя."""
    clean = str(token or "").strip()
    if not clean:
        clean = None

    async with async_session() as session:
        try:
            await session.execute(
                update(User).where(User.tg_id == tg_id).values(wb_token=clean)
            )
            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка update_wb_token (tg_id={tg_id}): {e}")
            await session.rollback()
            return False


async def update_ozon_keys(tg_id: int, client_id: str, api_key: str) -> bool:
    """Сохраняет ключи Ozon."""
    cid = str(client_id or "").strip()
    key = str(api_key or "").strip()

    if not cid:
        cid = None
    if not key:
        key = None

    async with async_session() as session:
        try:
            await session.execute(
                update(User)
                .where(User.tg_id == tg_id)
                .values(ozon_client_id=cid, ozon_api_key=key)
            )
            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка update_ozon_keys (tg_id={tg_id}): {e}")
            await session.rollback()
            return False


# =============================================================================
# РАБОТА С ТОВАРАМИ И СЕБЕСТОИМОСТЬЮ
# =============================================================================

async def update_product_cost(
    user_tg_id: int,
    marketplace: str,
    article: str,
    cost: float,
    name: Optional[str] = None,
) -> bool:
    """
    Upsert: обновляет/создаёт товар и себестоимость.
    IMPORTANT: себестоимость не затираем нулём (если cost=0 пришёл случайно).
    """
    async with async_session() as session:
        try:
            clean_article = _norm_article(article)
            clean_market = _norm_marketplace(marketplace)
            clean_name = _safe_str(name, max_len=255, default=f"Товар {clean_article}")
            new_cost = _safe_float(cost, 0.0)

            if _supports_on_conflict():
                stmt = _insert_stmt(Product).values(
                    user_tg_id=user_tg_id,
                    marketplace=clean_market,
                    article=clean_article,
                    name=clean_name,
                    cost_price=new_cost,
                )

                stmt = stmt.on_conflict_do_update(
                    index_elements=["user_tg_id", "marketplace", "article"],
                    set_={
                        "name": stmt.excluded.name,
                        "cost_price": func.coalesce(func.nullif(stmt.excluded.cost_price, 0), Product.cost_price),
                    },
                )

                await session.execute(stmt)
            else:
                # Фоллбек: ручной upsert
                res = await session.execute(
                    select(Product).where(
                        Product.user_tg_id == user_tg_id,
                        Product.marketplace == clean_market,
                        Product.article == clean_article,
                    )
                )
                prod = res.scalar_one_or_none()
                if not prod:
                    session.add(
                        Product(
                            user_tg_id=user_tg_id,
                            marketplace=clean_market,
                            article=clean_article,
                            name=clean_name,
                            cost_price=new_cost,
                        )
                    )
                else:
                    prod.name = clean_name
                    if new_cost != 0:
                        prod.cost_price = new_cost

            await session.commit()
            return True
        except Exception as e:
            logger.error(
                f"Ошибка update_product_cost (user={user_tg_id}, mp={marketplace}, article={article}): {e}"
            )
            await session.rollback()
            return False


async def bulk_update_products(user_tg_id: int, products_list: List[Dict[str, Any]]) -> int:
    """
    Массовое обновление/вставка товаров (upsert).
    Ожидает список словарей, где минимум: marketplace, article, name (name может быть пустым).
    Поддерживает поля: cost_price, extra_costs, tax_rate.
    """
    if not products_list:
        return 0

    async with async_session() as session:
        try:
            count = 0

            for p in products_list:
                if not isinstance(p, dict):
                    continue

                clean_market = _norm_marketplace(p.get("marketplace"))
                clean_article = _norm_article(p.get("article"))
                if not clean_market or not clean_article:
                    continue

                clean_name = _safe_str(p.get("name"), max_len=255, default=f"Товар {clean_article}")

                cost_price = _safe_float(p.get("cost_price"), 0.0)
                extra_costs = _safe_float(p.get("extra_costs"), 0.0)
                tax_rate = _safe_float(p.get("tax_rate"), 0.06)

                if tax_rate > 1:
                    tax_rate = tax_rate / 100.0
                if tax_rate < 0:
                    tax_rate = 0.0

                if _supports_on_conflict():
                    stmt = _insert_stmt(Product).values(
                        user_tg_id=user_tg_id,
                        marketplace=clean_market,
                        article=clean_article,
                        name=clean_name,
                        cost_price=cost_price,
                        extra_costs=extra_costs,
                        tax_rate=tax_rate,
                    )

                    stmt = stmt.on_conflict_do_update(
                        index_elements=["user_tg_id", "marketplace", "article"],
                        set_={
                            "name": stmt.excluded.name,
                            "cost_price": func.coalesce(func.nullif(stmt.excluded.cost_price, 0), Product.cost_price),
                            "extra_costs": func.coalesce(func.nullif(stmt.excluded.extra_costs, 0), Product.extra_costs),
                            "tax_rate": func.coalesce(stmt.excluded.tax_rate, Product.tax_rate),
                        },
                    )
                    await session.execute(stmt)
                else:
                    # Фоллбек: ручной upsert
                    res = await session.execute(
                        select(Product).where(
                            Product.user_tg_id == user_tg_id,
                            Product.marketplace == clean_market,
                            Product.article == clean_article,
                        )
                    )
                    prod = res.scalar_one_or_none()
                    if not prod:
                        session.add(
                            Product(
                                user_tg_id=user_tg_id,
                                marketplace=clean_market,
                                article=clean_article,
                                name=clean_name,
                                cost_price=cost_price,
                                extra_costs=extra_costs,
                                tax_rate=tax_rate,
                            )
                        )
                    else:
                        prod.name = clean_name
                        if cost_price != 0:
                            prod.cost_price = cost_price
                        if extra_costs != 0:
                            prod.extra_costs = extra_costs
                        prod.tax_rate = tax_rate

                count += 1

            await session.commit()
            logger.info(f"User {user_tg_id}: синхронизировано {count} товаров.")
            return count
        except Exception as e:
            logger.error(f"Ошибка bulk_update_products (user={user_tg_id}): {e}")
            await session.rollback()
            return 0


async def get_user_products(user_tg_id: int) -> List[Product]:
    """Загружает список всех товаров пользователя."""
    async with async_session() as session:
        try:
            result = await session.execute(
                select(Product).where(Product.user_tg_id == user_tg_id)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Ошибка get_user_products (user={user_tg_id}): {e}")
            return []


# =============================================================================
# МОНИТОРИНГ ЗАКАЗОВ
# =============================================================================

async def is_order_new(order_id: str, marketplace: str, user_tg_id: Optional[int] = None) -> bool:
    """
    Проверяет, является ли заказ новым (отсутствует в БД).

    ВАЖНО:
    - учитываем user_tg_id, чтобы исключить коллизии между пользователями
    """
    oid = _safe_str(order_id, max_len=128, default="")
    mp = _norm_marketplace(marketplace)

    if not oid or not mp:
        return False

    async with async_session() as session:
        try:
            cond = [Order.order_id == oid, Order.marketplace == mp]
            if user_tg_id is not None:
                cond.append(Order.user_id == user_tg_id)

            result = await session.execute(select(Order.id).where(and_(*cond)))
            return result.scalar_one_or_none() is None
        except Exception as e:
            logger.error(f"Ошибка is_order_new (order_id={order_id}, mp={marketplace}, user={user_tg_id}): {e}")
            return False


async def save_order(
    order_id: str,
    marketplace: str,
    amount: float,
    item_name: str,
    user_tg_id: int,
    order_date: Optional[datetime] = None,
) -> None:
    """Сохраняет одиночный заказ для истории."""
    oid = _safe_str(order_id, max_len=128, default="")
    mp = _norm_marketplace(marketplace)
    if not oid or not mp:
        return

    async with async_session() as session:
        try:
            if _supports_on_conflict():
                stmt = _insert_stmt(Order).values(
                    order_id=oid,
                    marketplace=mp,
                    amount=_safe_float(amount, 0.0),
                    item_name=_safe_str(item_name, max_len=255, default="Н/Д"),
                    user_id=user_tg_id,
                    order_date=order_date or datetime.now(),
                ).on_conflict_do_nothing()
                await session.execute(stmt)
            else:
                # Фоллбек: manual do-nothing
                res = await session.execute(
                    select(Order.id).where(
                        Order.order_id == oid,
                        Order.marketplace == mp,
                        Order.user_id == user_tg_id,
                    )
                )
                if res.scalar_one_or_none() is None:
                    session.add(
                        Order(
                            order_id=oid,
                            marketplace=mp,
                            amount=_safe_float(amount, 0.0),
                            item_name=_safe_str(item_name, 255, "Н/Д"),
                            user_id=user_tg_id,
                            order_date=order_date or datetime.now(),
                        )
                    )

            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка save_order (order_id={order_id}, user={user_tg_id}): {e}")
            await session.rollback()


async def bulk_save_orders(orders_data: List[Dict[str, Any]]) -> None:
    """
    Массовое сохранение заказов.

    Ожидаемый формат элемента списка:
    {
      'order_id': str,
      'marketplace': 'wb'|'ozon',
      'amount': float,
      'item_name': str,
      'user_id': int,
      'order_date': datetime (optional)
    }
    """
    if not orders_data:
        return

    async with async_session() as session:
        try:
            for o in orders_data:
                if not isinstance(o, dict):
                    continue

                oid = _safe_str(o.get("order_id"), max_len=128, default="")
                mp = _norm_marketplace(o.get("marketplace"))
                uid = o.get("user_id")

                if not oid or not mp or uid is None:
                    continue

                amount = _safe_float(o.get("amount"), 0.0)
                item_name = _safe_str(o.get("item_name", "Н/Д"), max_len=255, default="Н/Д")
                odt = o.get("order_date") or o.get("created_at") or datetime.now()  # совместимость входов

                if _supports_on_conflict():
                    stmt = _insert_stmt(Order).values(
                        order_id=oid,
                        marketplace=mp,
                        amount=amount,
                        item_name=item_name,
                        user_id=int(uid),
                        order_date=odt,
                    ).on_conflict_do_nothing()
                    await session.execute(stmt)
                else:
                    # Фоллбек: manual do-nothing
                    res = await session.execute(
                        select(Order.id).where(
                            Order.order_id == oid,
                            Order.marketplace == mp,
                            Order.user_id == int(uid),
                        )
                    )
                    if res.scalar_one_or_none() is None:
                        session.add(
                            Order(
                                order_id=oid,
                                marketplace=mp,
                                amount=amount,
                                item_name=item_name,
                                user_id=int(uid),
                                order_date=odt,
                            )
                        )

            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка bulk_save_orders: {e}")
            await session.rollback()


async def get_orders_stats(user_tg_id: int, days: int = 1) -> List[Order]:
    """Получает заказы из БД за период (days) для отчетов."""
    async with async_session() as session:
        try:
            date_limit = datetime.now() - timedelta(days=int(days))
            result = await session.execute(
                select(Order)
                .where(
                    Order.user_id == user_tg_id,
                    Order.order_date >= date_limit,
                )
                .order_by(Order.order_date.desc())
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Ошибка get_orders_stats (user={user_tg_id}, days={days}): {e}")
            return []


# =============================================================================
# ОТЧЕТНОСТЬ И АНАЛИТИКА
# =============================================================================

async def get_analytics_data(user_tg_id: int) -> Dict[str, Dict[str, float]]:
    """
    Получает полные данные о товарах (себестоимость + доп. расходы и налог).
    Возвращает словарь:
      { 'marketplace:article': {'cost': float, 'tax': float} }
    """
    async with async_session() as session:
        try:
            result = await session.execute(
                select(Product).where(Product.user_tg_id == user_tg_id)
            )
            products = result.scalars().all()
            out: Dict[str, Dict[str, float]] = {}
            for p in products:
                key = f"{_norm_marketplace(p.marketplace)}:{_norm_article(p.article)}"
                cost = _safe_float(p.cost_price, 0.0) + _safe_float(p.extra_costs, 0.0)
                tax = _safe_float(p.tax_rate, 0.06)
                if tax > 1:
                    tax = tax / 100.0
                if tax < 0:
                    tax = 0.0
                out[key] = {"cost": float(cost), "tax": float(tax)}
            return out
        except Exception as e:
            logger.error(f"Ошибка get_analytics_data (user={user_tg_id}): {e}")
            return {}


async def get_orders_summary_by_marketplace(user_tg_id: int, days: int = 7) -> Dict[str, Dict[str, float]]:
    """
    Сводка по заказам за период по каждому МП:
    {
      'wb': {'orders': 10, 'amount_sum': 12345.0},
      'ozon': {'orders': 5, 'amount_sum': 6789.0}
    }
    """
    async with async_session() as session:
        try:
            date_limit = datetime.now() - timedelta(days=int(days))
            q = (
                select(
                    Order.marketplace,
                    func.count(Order.id),
                    func.coalesce(func.sum(Order.amount), 0.0),
                )
                .where(
                    Order.user_id == user_tg_id,
                    Order.order_date >= date_limit,
                )
                .group_by(Order.marketplace)
            )
            res = await session.execute(q)
            out: Dict[str, Dict[str, float]] = {
                "wb": {"orders": 0, "amount_sum": 0.0},
                "ozon": {"orders": 0, "amount_sum": 0.0},
            }
            for mp, cnt, sm in res.all():
                mpn = _norm_marketplace(mp)
                out.setdefault(mpn, {"orders": 0, "amount_sum": 0.0})
                out[mpn]["orders"] = int(cnt or 0)
                out[mpn]["amount_sum"] = float(sm or 0.0)
            return out
        except Exception as e:
            logger.error(f"Ошибка get_orders_summary_by_marketplace (user={user_tg_id}): {e}")
            return {
                "wb": {"orders": 0, "amount_sum": 0.0},
                "ozon": {"orders": 0, "amount_sum": 0.0},
            }


# =============================================================================
# МОНИТОРИНГ КЛЮЧЕВЫХ СЛОВ (SEO / позиции)
# =============================================================================

async def add_keyword_track(user_id: int, marketplace: str, article: str, keyword: str) -> bool:
    """
    Добавляет новый запрос для отслеживания позиций.

    ВАЖНО: соответствует database.py
    KeywordTrack.user_id (а не user_tg_id)
    """
    async with async_session() as session:
        try:
            if _supports_on_conflict():
                stmt = _insert_stmt(KeywordTrack).values(
                    user_id=user_id,
                    marketplace=_norm_marketplace(marketplace),
                    article=_norm_article(article),
                    keyword=_norm_keyword(keyword),
                    last_position=None,
                    previous_position=0,
                ).on_conflict_do_nothing()
                await session.execute(stmt)
            else:
                # Фоллбек: manual do-nothing
                res = await session.execute(
                    select(KeywordTrack.id).where(
                        KeywordTrack.user_id == user_id,
                        KeywordTrack.article == _norm_article(article),
                        KeywordTrack.keyword == _norm_keyword(keyword),
                    )
                )
                if res.scalar_one_or_none() is None:
                    session.add(
                        KeywordTrack(
                            user_id=user_id,
                            marketplace=_norm_marketplace(marketplace),
                            article=_norm_article(article),
                            keyword=_norm_keyword(keyword),
                            last_position=None,
                            previous_position=0,
                        )
                    )

            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка add_keyword_track (user={user_id}, mp={marketplace}, article={article}): {e}")
            await session.rollback()
            return False


async def get_user_keywords(user_id: int) -> List[KeywordTrack]:
    """Возвращает список всех отслеживаемых ключей пользователя."""
    async with async_session() as session:
        try:
            result = await session.execute(
                select(KeywordTrack).where(KeywordTrack.user_id == user_id)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Ошибка get_user_keywords (user={user_id}): {e}")
            return []


async def delete_keyword_track(track_id: int, user_id: int) -> None:
    """Удаляет отслеживание ключевого слова (только владельцу)."""
    async with async_session() as session:
        try:
            await session.execute(
                delete(KeywordTrack).where(
                    KeywordTrack.id == track_id,
                    KeywordTrack.user_id == user_id,
                )
            )
            await session.commit()
        except Exception as e:
            logger.error(f"Ошибка delete_keyword_track (track_id={track_id}, user={user_id}): {e}")
            await session.rollback()


async def save_keyword_position(track_id: int, position: Optional[int], check_date: Optional[datetime] = None) -> bool:
    """
    Сохраняет историю позиции (KeywordHistory).

    ВАЖНО: соответствует database.py:
    - KeywordHistory.track_id
    - KeywordHistory.check_date
    - KeywordHistory.position
    """
    async with async_session() as session:
        try:
            pos = int(position) if position is not None else 0
            dt = check_date or datetime.now()

            if _supports_on_conflict():
                stmt = _insert_stmt(KeywordHistory).values(
                    track_id=track_id,
                    check_date=dt,
                    position=pos,
                ).on_conflict_do_nothing()
                await session.execute(stmt)
            else:
                session.add(
                    KeywordHistory(
                        track_id=track_id,
                        check_date=dt,
                        position=pos,
                    )
                )

            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка save_keyword_position (track_id={track_id}): {e}")
            await session.rollback()
            return False
