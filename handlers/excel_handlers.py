import io
import logging
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Excel columns (поддерживаем несколько вариантов названий)
# -----------------------------------------------------------------------------

COL_MP = "Маркетплейс"
COL_ART = "Артикул"
COL_NAME = "Название"
COL_COST = "Себестоимость"

# варианты "доп. расходы"
EXTRA_ALIASES = (
    "Доп_расходы",
    "Доп. расходы",
    "Доп расходы",
    "Допрасходы",
    "ДопРасходы",
)

# варианты "налог"
TAX_ALIASES = (
    "Налог (0.06 = 6%)",
    "Налог %",
    "Налог",
    "Tax",
    "Tax %",
)

REQUIRED_COLS = (COL_MP, COL_ART, COL_COST)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _safe_str(value: Any, max_len: int = 255, default: str = "") -> str:
    s = str(value).strip() if value is not None else default
    if not s:
        s = default
    return s[:max_len]


def _safe_float(value: Any, default: float = 0.0) -> float:
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


def _normalize_column_name(name: str) -> str:
    """
    Нормализуем заголовки:
    - trim
    - приводим латинскую 'l' в слове "Артикуl" к кириллической 'л'
    - убираем двойные пробелы
    """
    s = (name or "").strip()
    if not s:
        return s

    # Частая ошибка пользователей: "Артикуl" (латинская l)
    # Ловим разные регистры/варианты
    s = s.replace("Артикуl", "Артикул").replace("артикуl", "артикул").replace("АРТИКУL", "АРТИКУЛ")

    # Схлопываем пробелы
    while "  " in s:
        s = s.replace("  ", " ")

    return s


def _find_first_existing_column(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    """Ищет первую существующую колонку из списка."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def _normalize_marketplace(value: Any) -> str:
    """
    Нормализуем маркетплейс к 'wb' или 'ozon'.
    """
    s = _safe_str(value, 32, "").lower()
    if s in ("wb", "wildberries", "w", "вайлдберриз", "вайлдберис"):
        return "wb"
    if s in ("ozon", "o3", "o", "озон"):
        return "ozon"
    return s  # если что-то нестандартное — пусть пройдёт дальше, dbf всё равно нормализует


def _normalize_tax_rate(value: Any, default: float = 0.06) -> float:
    """
    Приводит налог к доле:
    - 6      -> 0.06
    - 6.0    -> 0.06
    - 0.06   -> 0.06
    - "6%"   -> 0.06
    """
    if isinstance(value, str) and "%" in value:
        v = value.replace("%", "")
        rate = _safe_float(v, default)
        return max(0.0, rate / 100.0)

    rate = _safe_float(value, default)
    if rate > 1.0:
        rate = rate / 100.0
    if rate < 0:
        rate = 0.0
    return rate


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

async def create_products_template(products_data: list) -> io.BytesIO:
    """
    Генерирует Excel-файл шаблона в памяти.
    Принимает список объектов Product (или похожих объектов) из БД.

    Важно:
    - Артикул сохраняем как строку (чтобы не терять ведущие нули).
    - Налог сохраняем в формате "0.06 = 6%" (как доля), чтобы совпадало с settings.py.
    - Доп. расходы сохраняем в колонку "Доп_расходы".
    """
    rows: List[Dict[str, Any]] = []

    for p in (products_data or []):
        rows.append({
            COL_MP: _safe_str(getattr(p, "marketplace", ""), 32, "").upper() or "WB",
            COL_ART: _safe_str(getattr(p, "article", ""), 128, ""),
            COL_NAME: _safe_str(getattr(p, "name", ""), 255, ""),
            COL_COST: float(getattr(p, "cost_price", 0.0) or 0.0),
            "Налог (0.06 = 6%)": float(getattr(p, "tax_rate", 0.06) or 0.06),
            "Доп_расходы": float(getattr(p, "extra_costs", 0.0) or 0.0),
        })

    # Если данных нет — делаем пример
    if not rows:
        rows = [
            {COL_MP: "WB", COL_ART: "00123", COL_NAME: "Пример товара 1", COL_COST: 500.0, "Налог (0.06 = 6%)": 0.06, "Доп_расходы": 50.0},
            {COL_MP: "OZON", COL_ART: "SKU-999", COL_NAME: "Пример товара 2", COL_COST: 300.0, "Налог (0.06 = 6%)": 0.07, "Доп_расходы": 30.0},
        ]

    df = pd.DataFrame(rows)

    output = io.BytesIO()
    try:
        # Используем openpyxl (обычно уже есть, и он стабильнее в проде, чем xlsxwriter)
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Products")
        output.seek(0)
        return output
    except Exception as e:
        logger.error(f"Критическая ошибка при создании Excel: {e}")
        raise


async def parse_products_excel(file_content: bytes) -> Optional[List[Dict[str, Any]]]:
    """
    Парсит Excel от пользователя и возвращает список словарей
    для dbf.bulk_update_products().

    Поддерживаем 2 формата колонок:
    A) settings.py:
       - Маркетплейс
       - Артикул
       - Название (опционально)
       - Себестоимость
       - Налог (0.06 = 6%) (опционально)
       - Доп_расходы (опционально)

    B) старый шаблон:
       - Маркетплейс
       - Артикул
       - Название (опционально)
       - Себестоимость
       - Налог % (опционально)
       - Доп. расходы (опционально)
    """
    try:
        df = pd.read_excel(io.BytesIO(file_content), dtype={COL_ART: str})

        # Нормализуем названия колонок ДО любых обращений к ним
        df.columns = [_normalize_column_name(str(c)) for c in df.columns]

        # Иногда dtype по Артикулу не сработает, если колонка называлась "Артикуl"
        # После нормализации колонок гарантируем наличие COL_ART в dtype-логике через преобразование ниже.
        missing_required = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing_required:
            logger.warning(f"Парсинг Excel: отсутствуют обязательные колонки: {missing_required}")
            return None

        # Удаляем пустые строки и строки без артикула
        df.dropna(how="all", inplace=True)
        df.dropna(subset=[COL_ART], inplace=True)

        # Нормализация marketplace/article
        df[COL_MP] = df[COL_MP].astype(str).map(_normalize_marketplace)
        df[COL_ART] = df[COL_ART].astype(str).map(lambda x: _safe_str(x, 128, "")).map(lambda x: x.strip())

        # Числовые поля
        df[COL_COST] = pd.to_numeric(df[COL_COST], errors="coerce").fillna(0)

        # optional columns
        extra_col = _find_first_existing_column(df, EXTRA_ALIASES)
        tax_col = _find_first_existing_column(df, TAX_ALIASES)

        if extra_col:
            df["_extra_costs"] = pd.to_numeric(df[extra_col], errors="coerce").fillna(0)
        else:
            df["_extra_costs"] = 0.0

        if tax_col:
            # Может быть либо доля (0.06), либо процент (6), либо строка "6%"
            df["_tax_rate"] = df[tax_col].map(lambda v: _normalize_tax_rate(v, 0.06))
        else:
            df["_tax_rate"] = 0.06

        # name (опционально)
        if COL_NAME in df.columns:
            df["_name"] = df[COL_NAME].astype(str).map(lambda v: _safe_str(v, 255, ""))
        else:
            df["_name"] = ""

        result: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            mp = _safe_str(row.get(COL_MP), 32, "").lower()
            art = _safe_str(row.get(COL_ART), 128, "")
            if not mp or not art:
                continue

            result.append({
                "marketplace": mp,
                "article": art,
                "name": _safe_str(row.get("_name", ""), 255, "") or f"Товар {art}",
                "cost_price": float(_safe_float(row.get(COL_COST), 0.0)),
                "extra_costs": float(_safe_float(row.get("_extra_costs"), 0.0)),
                "tax_rate": float(_safe_float(row.get("_tax_rate"), 0.06)),
            })

        return result if result else None

    except Exception as e:
        logger.error(f"Ошибка parse_products_excel: {e}")
        return None
