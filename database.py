from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import BigInteger, String, DateTime, Float, Boolean, func, ForeignKey, UniqueConstraint, text, select
from datetime import datetime, date
from config import config
import logging

# Настройка логирования для отслеживания работы БД
logger = logging.getLogger(__name__)

# Создаем асинхронный движок базы данных
engine = create_async_engine(
    url=config.db_url, 
    echo=False,
    pool_pre_ping=True
)

# Фабрика сессий для работы с транзакциями
async_session = async_sessionmaker(engine, expire_on_commit=False)

class Base(AsyncAttrs, DeclarativeBase):
    """Базовый класс для всех моделей данных"""
    pass

class User(Base):
    """Модель пользователя бота и его настроек"""
    __tablename__ = 'users'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    
    # Токены и ключи API маркетплейсов
    wb_token: Mapped[str] = mapped_column(String, nullable=True)
    ozon_client_id: Mapped[str] = mapped_column(String, nullable=True)
    ozon_api_key: Mapped[str] = mapped_column(String, nullable=True)
    
    # Настройки уведомлений и фильтров
    notifications_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    stock_threshold: Mapped[int] = mapped_column(default=5)
    
    # Налоговая ставка по умолчанию (например, 0.06 для 6%)
    tax_rate_default: Mapped[float] = mapped_column(Float, default=0.06)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    
    # Отношения
    orders: Mapped[list["Order"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    products: Mapped[list["Product"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    keywords: Mapped[list["KeywordTrack"]] = relationship(back_populates="user", cascade="all, delete-orphan")

class Order(Base):
    """Модель лога заказов"""
    __tablename__ = 'orders'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[str] = mapped_column(String, index=True)
    marketplace: Mapped[str] = mapped_column(String(20))      # 'wb' или 'ozon'
    
    amount: Mapped[float] = mapped_column(Float, default=0.0)
    item_name: Mapped[str] = mapped_column(String, nullable=True)
    order_date: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('users.tg_id'))
    user: Mapped["User"] = relationship(back_populates="orders")

    __table_args__ = (UniqueConstraint('order_id', 'marketplace', 'user_id', name='_order_user_market_uc'),)

class Product(Base):
    """Модель товара для юнит-экономики"""
    __tablename__ = 'products'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    user_tg_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('users.tg_id'))
    
    marketplace: Mapped[str] = mapped_column(String(20), nullable=True) 
    article: Mapped[str] = mapped_column(String, index=True)
    name: Mapped[str] = mapped_column(String, nullable=True)
    
    cost_price: Mapped[float] = mapped_column(Float, default=0.0)
    tax_rate: Mapped[float] = mapped_column(Float, default=0.06)
    extra_costs: Mapped[float] = mapped_column(Float, default=0.0)

    user: Mapped["User"] = relationship(back_populates="products")

    __table_args__ = (UniqueConstraint('user_tg_id', 'marketplace', 'article', name='_user_market_article_uc'),)

class KeywordTrack(Base):
    """Модель для отслеживания текущих позиций ключевых слов"""
    __tablename__ = 'keyword_tracks'

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('users.tg_id'))
    
    marketplace: Mapped[str] = mapped_column(String(20)) # 'wb' или 'ozon'
    article: Mapped[str] = mapped_column(String)        # Артикул товара
    keyword: Mapped[str] = mapped_column(String)        # Поисковая фраза
    
    last_position: Mapped[int] = mapped_column(nullable=True) 
    # Позиция вчера (для отображения динамики +/-)
    previous_position: Mapped[int] = mapped_column(nullable=True, default=0) 
    
    updated_at: Mapped[datetime] = mapped_column(DateTime, onupdate=func.now(), server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="keywords")
    # Связь с историей для графиков
    history: Mapped[list["KeywordHistory"]] = relationship(back_populates="track", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint('user_id', 'article', 'keyword', name='_user_article_keyword_uc'),)

class KeywordHistory(Base):
    """НОВОЕ: Хранение истории позиций по дням для аналитики и графиков"""
    __tablename__ = 'keyword_history'

    id: Mapped[int] = mapped_column(primary_key=True)
    track_id: Mapped[int] = mapped_column(ForeignKey('keyword_tracks.id'))
    
    check_date: Mapped[date] = mapped_column(DateTime, server_default=func.now())
    position: Mapped[int] = mapped_column(default=0)

    track: Mapped["KeywordTrack"] = relationship(back_populates="history")

# --- ФУНКЦИИ РАБОТЫ С БД ---

async def add_keyword(user_id: int, marketplace: str, article: str, keyword: str):
    """Добавление нового ключевого слова для отслеживания"""
    async with async_session() as session:
        new_track = KeywordTrack(
            user_id=user_id,
            marketplace=marketplace,
            article=article,
            keyword=keyword.strip().lower()
        )
        session.add(new_track)
        try:
            await session.commit()
            return True
        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при добавлении ключевого слова: {e}")
            return False

async def get_user_keywords(user_id: int):
    """Получение всех отслеживаемых слов пользователя"""
    async with async_session() as session:
        result = await session.execute(
            select(KeywordTrack).where(KeywordTrack.user_id == user_id)
        )
        return result.scalars().all()

async def update_keyword_position(track_id: int, new_pos: int):
    """Обновление позиции и запись в историю"""
    async with async_session() as session:
        track = await session.get(KeywordTrack, track_id)
        if track:
            # Сдвигаем текущую позицию в "предыдущую" перед обновлением
            track.previous_position = track.last_position if track.last_position else 0
            track.last_position = new_pos
            
            # Добавляем запись в историю
            history_entry = KeywordHistory(track_id=track_id, position=new_pos)
            session.add(history_entry)
            
            await session.commit()

async def init_db():
    """Инициализация и обновление таблиц базы данных"""
    async with engine.begin() as conn:
        # Автоматическое создание новых таблиц
        await conn.run_sync(Base.metadata.create_all)
        
        # Миграции для существующих баз (добавление новых колонок)
        try:
            # Добавляем колонки для SEO динамики
            await conn.execute(text("ALTER TABLE keyword_tracks ADD COLUMN previous_position INTEGER DEFAULT 0"))
            # Если таблицы продуктов/пользователей уже были, убедимся в наличии доп. полей
            await conn.execute(text("ALTER TABLE products ADD COLUMN extra_costs FLOAT DEFAULT 0.0"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN tax_rate_default FLOAT DEFAULT 0.06"))
        except Exception:
            # Если колонки уже существуют, SQL выдаст ошибку, просто пропускаем
            pass
            
    print(f">>> База данных синхронизирована (таблицы: {', '.join(Base.metadata.tables.keys())})")