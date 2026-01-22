import pandas as pd
import io
import logging
from typing import List, Dict, Any, Optional

# Настройка логирования
logger = logging.getLogger(__name__)

async def create_products_template(products_data: list) -> io.BytesIO:
    """
    Генерирует Excel файл в памяти. 
    Принимает список объектов Product из БД.
    """
    data = []
    
    # Формируем список словарей для DataFrame
    for p in products_data:
        data.append({
            "Маркетплейс": str(p.marketplace).upper() if p.marketplace else "WB",
            "Артикул": str(p.article), # Важно: артикул как строка, чтобы не терять ведущие нули
            "Название": p.name or "",
            "Себестоимость": float(p.cost_price or 0.0),
            "Доп. расходы": float(p.extra_costs or 0.0),
            "Налог %": float((p.tax_rate or 0.06) * 100)
        })
    
    # Если данных нет, создаем "рыбу" (примеры)
    if not data:
        data = [
            {"Маркетплейс": "WB", "Артикул": "123456", "Название": "Пример товара 1", "Себестоимость": 500.0, "Доп. расходы": 50.0, "Налог %": 6.0},
            {"Маркетплейс": "OZON", "Артикул": "SKU-999", "Название": "Пример товара 2", "Себестоимость": 300.0, "Доп. расходы": 30.0, "Налог %": 7.0}
        ]
    
    df = pd.DataFrame(data)
    output = io.BytesIO()
    
    try:
        # Используем xlsxwriter для форматирования
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Юнит-Экономика')
            
            workbook = writer.book
            worksheet = writer.sheets['Юнит-Экономика']
            
            # Добавляем стиль для заголовков
            header_format = workbook.add_format({
                'bold': True, 
                'bg_color': '#D7E4BC', 
                'border': 1,
                'align': 'center'
            })
            
            # Перезаписываем заголовки со стилем
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Автоматическая настройка ширины колонок
            for i, col in enumerate(df.columns):
                column_len = max(df[col].astype(str).str.len().max(), len(col)) + 3
                worksheet.set_column(i, i, column_len)
                
    except Exception as e:
        logger.error(f"Критическая ошибка при создании Excel: {e}")
        raise e
    
    output.seek(0)
    return output

async def parse_products_excel(file_content: bytes) -> Optional[List[Dict[str, Any]]]:
    """
    Парсит полученный от пользователя Excel файл.
    Возвращает очищенные данные для bulk_update в БД.
    """
    try:
        # Читаем байты. dtype={'Артикул': str} предотвращает превращение "00123" в 123
        df = pd.read_excel(io.BytesIO(file_content), dtype={'Артикул': str})
        
        # Проверка структуры
        required_cols = ['Маркетплейс', 'Артикул', 'Себестоимость']
        if not all(col in df.columns for col in required_cols):
            logger.warning("Парсинг отменен: неверные заголовки в файле")
            return None

        # 1. Удаляем абсолютно пустые строки и строки без артикула
        df.dropna(how='all', inplace=True)
        df.dropna(subset=['Артикул'], inplace=True)
        
        # 2. Нормализация строковых полей
        df['Маркетплейс'] = df['Маркетплейс'].astype(str).str.strip().lower()
        df['Артикул'] = df['Артикуl'].astype(str).str.strip() if 'Артикуl' in df.columns else df['Артикул'].astype(str).str.strip()
        # Исправим возможную опечатку в кириллице/латинице для Артикул (частая проблема пользователей)
        df.columns = [c.replace('l', 'л') if 'Артику' in c else c for c in df.columns]

        # 3. Приведение числовых данных к безопасному виду
        df['Себестоимость'] = pd.to_numeric(df['Себестоимость'], errors='coerce').fillna(0)
        
        extra_col = 'Доп. расходы'
        df['extra_costs'] = pd.to_numeric(df[extra_col], errors='coerce').fillna(0) if extra_col in df.columns else 0.0
        
        tax_col = 'Налог %'
        if tax_col in df.columns:
            df['tax_rate'] = pd.to_numeric(df[tax_col], errors='coerce').fillna(6) / 100
        else:
            df['tax_rate'] = 0.06

        # Итоговая сборка списка
        result = []
        for _, row in df.iterrows():
            result.append({
                "marketplace": row['Маркетплейс'],
                "article": row['Артикул'],
                "name": str(row.get('Название', '')) if pd.notna(row.get('Название')) else "Товар без названия",
                "cost_price": float(row['Себестоимость']),
                "extra_costs": float(row['extra_costs']),
                "tax_rate": float(row['tax_rate'])
            })
            
        return result

    except Exception as e:
        logger.error(f"Ошибка в модуле parse_products_excel: {e}")
        return None