"""
Версия файла: 1.2.0
Описание: Асинхронная БД (SQLAlchemy): engine, session, модели (User/Order/Product/KeywordTrack/KeywordHistory) и init_db().
Дата изменения: 2026-01-22
Изменения:
- Исправлены несовместимости с db_functions.py:
  - Order.created_at вместо order_date
  - KeywordTrack.user_tg_id вместо user_id
- KeywordHistory.check_date переведен на Date (без времени), корректный default.
- Уточнены типы колонок (String длины, nullable, индексы), добавлены индексы для частых запросов.
- Убраны дублирующие функции работы с ключевыми словами (их место в db_functions.py).
- init_db(): create_all + безопасные миграции для SQLite через PRAGMA table_info (добавляем только отсутствующие колонки).
"""

from __future__ import annotations

import logging
from datetime import datetime, date

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    String,
    UniqueConstraint,
    Index,
    func,
    text,
)
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from config import config

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Engine / Session
# -----------------------------------------------------------------------------

engine = create_async_engine(
    url=config.db_url,
    echo=False,
    pool_pre_ping=True,
)

async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
    """Базовый класс для всех моделей данных."""
    pass


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------

class User(Base):
    """Модель пользователя бота и его настроек."""
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)

    # Токены и ключи API маркетплейсов
    wb_token: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    ozon_client_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ozon_api_key: Mapped[str | None] = mapped_column(String(512), nullable=True)

    # Настройки уведомлений и фильтров
    notifications_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    stock_threshold: Mapped[int] = mapped_column(default=5)

    # Налоговая ставка по умолчанию (например, 0.06 для 6%)
    tax_rate_default: Mapped[float] = mapped_column(Float, default=0.06)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # Relationships
    orders: Mapped[list["Order"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    products: Mapped[list["Product"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    keywords: Mapped[list["KeywordTrack"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class Order(Base):
    """Модель лога заказов (для дедупликации уведомлений и отчетов)."""
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Идентификатор заказа/поста (WB: id/gNumber, Ozon: posting_number)
    order_id: Mapped[str] = mapped_column(String(128), index=True)

    # 'wb' или 'ozon'
    marketplace: Mapped[str] = mapped_column(String(20), index=True)

    amount: Mapped[float] = mapped_column(Float, default=0.0)
    item_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # ВАЖНО: в db_functions.py используется created_at
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)

    # user_id тут хранит tg_id (так сделано в текущем проекте)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.tg_id"), index=True)
    user: Mapped["User"] = relationship(back_populates="orders")

    __table_args__ = (
        UniqueConstraint("order_id", "marketplace", "user_id", name="ux_orders_order_market_user"),
        Index("ix_orders_user_market_created", "user_id", "marketplace", "created_at"),
    )


class Product(Base):
    """Модель товара для юнит-экономики."""
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)

    # FK на users.tg_id
    user_tg_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.tg_id"), index=True)

    # 'wb' или 'ozon'
    marketplace: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    # Артикул: для WB — nmID (строкой), для Ozon — offer_id
    article: Mapped[str] = mapped_column(String(128), index=True)

    name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    cost_price: Mapped[float] = mapped_column(Float, default=0.0)
    tax_rate: Mapped[float] = mapped_column(Float, default=0.06)
    extra_costs: Mapped[float] = mapped_column(Float, default=0.0)

    user: Mapped["User"] = relationship(back_populates="products")

    __table_args__ = (
        UniqueConstraint("user_tg_id", "marketplace", "article", name="ux_products_user_market_article"),
        Index("ix_products_user_market", "user_tg_id", "marketplace"),
    )


class KeywordTrack(Base):
    """Модель для отслеживания текущих позиций ключевых слов."""
    __tablename__ = "keyword_tracks"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Приводим к единому стандарту проекта: user_tg_id (FK -> users.tg_id)
    user_tg_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.tg_id"), index=True)

    marketplace: Mapped[str] = mapped_column(String(20), index=True)  # 'wb' или 'ozon'
    article: Mapped[str] = mapped_column(String(128), index=True)     # Артикул товара
    keyword: Mapped[str] = mapped_column(String(255), index=True)     # Поисковая фраза

    last_position: Mapped[int | None] = mapped_column(nullable=True)
    previous_position: Mapped[int] = mapped_column(nullable=True, default=0)

    updated_at: Mapped[datetime] = mapped_column(DateTime, onupdate=func.now(), server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="keywords")
    history: Mapped[list["KeywordHistory"]] = relationship(back_populates="track", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("user_tg_id", "article", "keyword", name="ux_kw_user_article_keyword"),
        Index("ix_kw_user_market", "user_tg_id", "marketplace"),
    )


class KeywordHistory(Base):
    """Хранение истории позиций по дням для аналитики и графиков."""
    __tablename__ = "keyword_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    track_id: Mapped[int] = mapped_column(ForeignKey("keyword_tracks.id"), index=True)

    # Дата проверки (без времени)
    check_date: Mapped[date] = mapped_column(Date, server_default=func.date("now"), index=True)
    position: Mapped[int] = mapped_column(default=0)

    track: Mapped["KeywordTrack"] = relationship(back_populates="history")

    __table_args__ = (
        Index("ix_kw_hist_track_date", "track_id", "check_date"),
    )


# -----------------------------------------------------------------------------
# DB init + light migrations (SQLite-friendly)
# -----------------------------------------------------------------------------

async def _sqlite_has_column(conn, table: str, column: str) -> bool:
    """
    Проверка наличия колонки в SQLite таблице.
    Используется в init_db() для мягких миграций без Alembic.
    """
    res = await conn.execute(text(f"PRAGMA table_info({table});"))
    rows = res.fetchall()
    cols = {r[1] for r in rows}  # (cid, name, type, notnull, dflt_value, pk)
    return column in cols


async def init_db():
    """
    Инициализация и обновление таблиц базы данных.
    Важно: это не полноценные миграции (как Alembic), но безопасный минимум для SQLite.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Определяем драйвер (sqlite/postgres/и т.п.)
        dialect = conn.dialect.name.lower()

        if dialect == "sqlite":
            # Мягкие миграции для SQLite: добавляем колонки, если отсутствуют
            try:
                # users.tax_rate_default
                if await _sqlite_has_column(conn, "users", "tax_rate_default") is False:
                    await conn.execute(text("ALTER TABLE users ADD COLUMN tax_rate_default FLOAT DEFAULT 0.06"))

                # products.extra_costs
                if await _sqlite_has_column(conn, "products", "extra_costs") is False:
                    await conn.execute(text("ALTER TABLE products ADD COLUMN extra_costs FLOAT DEFAULT 0.0"))

                # keyword_tracks.previous_position
                if await _sqlite_has_column(conn, "keyword_tracks", "previous_position") is False:
                    await conn.execute(text("ALTER TABLE keyword_tracks ADD COLUMN previous_position INTEGER DEFAULT 0"))

                # orders.created_at (если база была создана со старым order_date)
                # В SQLite нет простого RENAME COLUMN + пересборки во всех версиях.
                # Поэтому: если старое поле order_date существует, а created_at нет — добавляем created_at.
                has_created_at = await _sqlite_has_column(conn, "orders", "created_at")
                if not has_created_at:
                    await conn.execute(text("ALTER TABLE orders ADD COLUMN created_at DATETIME DEFAULT (datetime('now'))"))

                # keyword_tracks.user_tg_id (если база была создана со старым user_id)
                # Аналогично: просто добавляем новую колонку. Перенос данных — отдельная миграция при необходимости.
                has_user_tg_id = await _sqlite_has_column(conn, "keyword_tracks", "user_tg_id")
                if not has_user_tg_id:
                    await conn.execute(text("ALTER TABLE keyword_tracks ADD COLUMN user_tg_id BIGINT"))

            except Exception as e:
                logger.error(f"SQLite migration error: {e}")

        else:
            # Для PostgreSQL/MySQL лучше использовать Alembic.
            # Здесь — минимальная совместимость, без ALTER.
            pass

    logger.info(f"База данных синхронизирована. Таблицы: {', '.join(Base.metadata.tables.keys())}")
