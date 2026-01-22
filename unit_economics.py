import logging

# Настройка логгера для отслеживания ошибок в расчетах
logger = logging.getLogger(__name__)

async def calculate_profit(
    marketplace: str, 
    price: float, 
    cost_price: float, 
    tax_rate: float, 
    fees: dict,
    extra_costs: float = 0.0
):
    """
    Универсальный расчет чистой прибыли с одной единицы товара.
    
    :param marketplace: Название площадки (WB/Ozon)
    :param price: Цена продажи (фактическая, со всеми скидками)
    :param cost_price: Себестоимость (закупка)
    :param tax_rate: Ставка налога (например, 0.06 для 6%)
    :param fees: Данные от API (комиссии, логистика, реклама)
    :param extra_costs: Доп. расходы из БД (упаковка, маркировка, доставка до СЦ)
    """
    try:
        # 1. Налоги: Считаются от цены реализации (Price)
        # ВАЖНО: Маркетплейсы являются агентами, налог платится со всей суммы, которую оплатил покупатель
        tax_amount = price * tax_rate
        
        # 2. Расходы маркетплейса (из API)
        # Комиссия маркетплейса
        commission_val = fees.get('commission_percent', 0)
        m_commission = price * (commission_val / 100)
        
        # Логистика и хранение
        m_logistics = float(fees.get('logistics', 0))
        m_storage = float(fees.get('storage', 0))
        
        # Рекламные расходы (если API их отдает в привязке к товару)
        m_ads = float(fees.get('ads_share', 0))
        
        # 3. Итоговая формула чистой прибыли
        # Вычитаем: Себестоимость - Налог - Комиссию - Логистику - Хранение - Рекламу - Доп.расходы
        total_expenses = cost_price + tax_amount + m_commission + m_logistics + m_storage + m_ads + extra_costs
        net_profit = price - total_expenses
        
        # 4. Расчет показателей эффективности
        # ROI (Return on Investment) - сколько копеек прибыли приносит каждый рубль вложенный в закупку
        # Формула: (Прибыль / Себестоимость закупа) * 100
        roi = (net_profit / cost_price * 100) if cost_price > 0 else 0
        
        # Маржинальность (Margin) - какая доля в выручке является чистой прибылью
        margin = (net_profit / price * 100) if price > 0 else 0
        
        return {
            "net_profit": round(net_profit, 2),
            "roi": round(roi, 1),
            "margin": round(margin, 1),
            "tax": round(tax_amount, 2),
            "marketplace_fees": round(m_commission + m_logistics + m_storage + m_ads, 2),
            "extra_costs": round(extra_costs, 2)
        }

    except Exception as e:
        logger.error(f"Ошибка в расчете unit_economics для {marketplace}: {e}")
        # Возвращаем нулевые показатели в случае ошибки, чтобы бот не «падал»
        return {
            "net_profit": 0, "roi": 0, "margin": 0, "tax": 0, "marketplace_fees": 0, "extra_costs": 0
        }