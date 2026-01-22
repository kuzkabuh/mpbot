import pandas as pd
import logging
import io
from datetime import datetime
from database import async_session, Product, User
from sqlalchemy import select
import db_functions as dbf

# Настройка логирования
logger = logging.getLogger(__name__)

class FinancialProcessor:
    """
    Класс для глубокой аналитики финансовых данных маркетплейсов.
    Обрабатывает отчеты, считает чистую прибыль и ROI.
    """

    @staticmethod
    async def get_user_products_cost(user_tg_id: int):
        """
        Вспомогательный метод для получения словаря себестоимостей всех товаров пользователя.
        Ключ - артикул, значение - себестоимость.
        """
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.user_tg_id == user_tg_id)
            )
            products = result.scalars().all()
            # Создаем словарь для быстрого поиска в Pandas {арт: цена}
            return {p.article: p.cost_price for p in products}

    async def process_wb_weekly_json(self, user_tg_id: int, raw_data: list):
        """
        Обработка детализированного отчета Wildberries (полученного через API).
        """
        if not raw_data:
            logger.warning(f"Нет данных для обработки отчета пользователя {user_tg_id}")
            return None

        try:
            # 1. Загружаем данные в DataFrame
            df = pd.DataFrame(raw_data)

            # 2. Получаем себестоимость и налоги
            cost_map = await self.get_user_products_cost(user_tg_id)
            tax_rate = await dbf.get_user_tax_rate(user_tg_id) # Напр. 0.06

            # 3. Фильтруем только продажи (исключаем возвраты для чистоты базового расчета)
            # В отчетах WB: 'sale' - продажа, 'return' - возврат
            # 'ppvz_for_pay' - сумма к выплате продавцу (уже за вычетом комиссии)
            
            # Добавляем колонку себестоимости к каждой строке отчета по артикулу (sa_name)
            df['cost_price'] = df['sa_name'].map(cost_map).fillna(0)

            # Считаем показатели
            total_sales_count = len(df[df['doc_type_name'] == 'Продажа'])
            total_returns_count = len(df[df['doc_type_name'] == 'Возврат'])
            
            # Сумма, которую WB фактически перечислит (уже без комиссий)
            revenue = df['ppvz_for_pay'].sum()
            
            # Логистика
            delivery_cost = df['delivery_rub'].sum()
            
            # Прочие удержания (штрафы, доплаты)
            penalties = df['penalty'].sum() if 'penalty' in df.columns else 0
            
            # Налог считается с 'retail_amount' (цена до вычета комиссии WB)
            total_tax = df['retail_amount'].sum() * tax_rate
            
            # Общая себестоимость проданных товаров (только для продаж)
            total_cost = df[df['doc_type_name'] == 'Продажа']['cost_price'].sum()

            # --- ИТОГОВАЯ ФОРМУЛА ---
            # Чистая прибыль = Выплата - Себестоимость - Налог - Прочие расходы (штрафы и т.д.)
            net_profit = revenue - total_cost - total_tax - penalties

            # Маржинальность
            margin = (net_profit / revenue * 100) if revenue > 0 else 0

            return {
                "sales_count": total_sales_count,
                "returns_count": total_returns_count,
                "revenue": round(revenue, 2),
                "delivery": round(delivery_cost, 2),
                "tax": round(total_tax, 2),
                "cost": round(total_cost, 2),
                "profit": round(net_profit, 2),
                "margin": round(margin, 2)
            }

        except Exception as e:
            logger.error(f"Ошибка в FinancialProcessor (WB): {e}")
            return None

    async def process_ozon_finance(self, user_tg_id: int, finance_data: list):
        """
        Обработка финансовых транзакций Ozon.
        """
        # Логика для Ozon будет добавлена следующей, 
        # так как там другая структура (транзакционный отчет)
        pass

# Создаем экземпляр для импорта в другие модули
fin_processor = FinancialProcessor()