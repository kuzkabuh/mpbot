"""
Версия файла: 1.2.0
Описание: Клиент Wildberries API (заказы, продажи, остатки, финансы, карточки, SEO-позиции). Устойчив к ошибкам и лимитам.
Дата изменения: 2026-01-22
Изменения:
- Переработан _make_request: единый AsyncClient, таймауты, ретраи с backoff, обработка 401/403/429/5xx.
- Снижено шумное логирование: сырой ответ баланса логируется только в debug-режиме.
- Нормализованы форматы marketplace и article: marketplace='wb', article=nmID/vendorCode/offerId -> строка, upper/strip где нужно.
- get_all_orders: возвращает dict {'fbs': [...], 'fbo': [...]} с защитой типов.
- get_cards_list: исправлена логика пагинации по cursor (ориентируемся на returned cards и наличие cursor полей), защита от бесконечного цикла.
- get_all_products: связка карточек+остатков с устойчивым подсчётом quantity, marketplace='wb' (критично для БД/аналитики).
- search_product_position: безопасные ретраи и ограничение страниц, защита от пустых ответов.
- Добавлены util-функции _safe_str/_safe_float/_norm_article и параметр debug.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class WildberriesAPI:
    def __init__(
        self,
        token: str,
        *,
        timeout: float = 60.0,
        max_retries: int = 3,
        debug: bool = False,
    ):
        """
        Инициализация клиента Wildberries.
        Удаляем лишние пробелы из токена для предотвращения ошибок авторизации.
        """
        self.token = str(token or "").strip()
        self.timeout = float(timeout)
        self.max_retries = int(max_retries)
        self.debug = bool(debug)

        self.headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Разделение базовых URL по функциональным сегментам API WB
        self.common_url = "https://common-api.wildberries.ru"
        self.finance_url = "https://finance-api.wildberries.ru"
        self.marketplace_url = "https://marketplace-api.wildberries.ru"
        self.statistics_url = "https://statistics-api.wildberries.ru"
        self.content_url = "https://content-api.wildberries.ru"

        # Публичный поиск (SEO)
        self.search_url = "https://search.wb.ru/exactmatch/ru/common/v4/search"

    # -------------------------------------------------------------------------
    # Utils
    # -------------------------------------------------------------------------

    def _safe_str(self, value: Any, max_len: int = 255, default: str = "") -> str:
        s = str(value).strip() if value is not None else default
        if not s:
            s = default
        return s[:max_len]

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
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

    def _norm_article(self, value: Any) -> str:
        # Для WB article часто nmId (int) -> строка
        return self._safe_str(value, max_len=128, default="").strip()

    # -------------------------------------------------------------------------
    # HTTP request
    # -------------------------------------------------------------------------

    async def _make_request(
        self,
        base_url: str,
        endpoint: str,
        *,
        method: str = "GET",
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> Optional[Any]:
        """
        Универсальный метод для выполнения HTTP-запросов с ретраями и обработкой лимитов.
        Возвращает response.json() или None.
        """
        url = f"{base_url}{endpoint}"
        t = float(timeout) if timeout is not None else self.timeout
        base_sleep = 1.0

        limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
        async with httpx.AsyncClient(headers=self.headers, timeout=t, limits=limits) as client:
            for attempt in range(1, self.max_retries + 1):
                try:
                    if method.upper() == "GET":
                        resp = await client.get(url, params=params)
                    else:
                        resp = await client.post(url, json=json_data, params=params)

                    # 200 OK
                    if resp.status_code == 200:
                        try:
                            return resp.json()
                        except Exception as e:
                            logger.error(f"WB JSON decode error {endpoint}: {e}")
                            if self.debug:
                                logger.info(f"WB raw body: {resp.text[:500]}")
                            return None

                    # Auth errors
                    if resp.status_code in (401, 403):
                        logger.error(f"WB auth error {endpoint}: {resp.status_code} - {resp.text[:300]}")
                        return None

                    # Rate limit / transient
                    if resp.status_code in (429, 500, 502, 503, 504):
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            try:
                                sleep_s = max(1.0, float(retry_after))
                            except Exception:
                                sleep_s = base_sleep * (2 ** (attempt - 1))
                        else:
                            # WB часто жёстко режет — делаем более длинный backoff
                            sleep_s = min(30.0, (10.0 * attempt))

                        sleep_s = min(sleep_s, 60.0)
                        logger.warning(
                            f"WB transient error {endpoint}: {resp.status_code}, attempt {attempt}/{self.max_retries}, sleep {sleep_s}s"
                        )
                        await asyncio.sleep(sleep_s)
                        continue

                    # Other errors
                    logger.error(f"WB API error {resp.status_code} {endpoint}: {resp.text[:500]}")
                    return None

                except (httpx.TimeoutException, httpx.NetworkError) as e:
                    sleep_s = min(base_sleep * (2 ** (attempt - 1)), 10.0)
                    logger.warning(
                        f"WB connection/timeout {endpoint}: {e} (attempt {attempt}/{self.max_retries}), sleep {sleep_s}s"
                    )
                    await asyncio.sleep(sleep_s)
                    continue
                except Exception as e:
                    logger.error(f"WB unexpected error {endpoint}: {e}")
                    return None

        return None

    # -------------------------------------------------------------------------
    # Finance / token
    # -------------------------------------------------------------------------

    async def get_balance(self) -> float:
        """
        Получение баланса (доступно к выводу или текущий баланс).
        """
        result = await self._make_request(self.finance_url, "/api/v1/account/balance")
        if not isinstance(result, dict):
            return 0.0

        if self.debug:
            logger.info(f"WB Balance Raw Response (dict keys): {list(result.keys())}")

        try:
            for_withdraw = self._safe_float(result.get("for_withdraw"), 0.0)
            current = self._safe_float(result.get("current"), 0.0)

            # Приоритет: к выводу, если 0 — текущий
            final_balance = for_withdraw if for_withdraw > 0 else current

            logger.info(f"WB Balance calculated: {final_balance} (Withdraw={for_withdraw}, Current={current})")
            return float(final_balance)
        except Exception:
            return 0.0

    async def validate_token(self) -> bool:
        """
        Проверка работоспособности токена через /ping.
        """
        result = await self._make_request(self.common_url, "/ping")
        return result is not None

    # -------------------------------------------------------------------------
    # Orders / sales / stocks
    # -------------------------------------------------------------------------

    async def get_all_orders(self, days: int = 1) -> Dict[str, List[Dict[str, Any]]]:
        """
        Сбор заказов по схемам FBS и FBO за указанный период.
        Возвращает:
        {
          "fbs": [...],
          "fbo": [...]
        }
        """
        data: Dict[str, List[Dict[str, Any]]] = {"fbs": [], "fbo": []}

        # 1) FBS - новые сборочные задания
        fbs_res = await self._make_request(self.marketplace_url, "/api/v3/orders/new")
        if isinstance(fbs_res, dict):
            orders = fbs_res.get("orders", [])
            if isinstance(orders, list):
                data["fbs"] = [o for o in orders if isinstance(o, dict)]

        # 2) FBO - статистические заказы за период
        date_from = (datetime.now() - timedelta(days=int(days))).strftime("%Y-%m-%dT00:00:00Z")
        fbo_res = await self._make_request(
            self.statistics_url,
            "/api/v1/supplier/orders",
            params={"dateFrom": date_from},
        )
        if isinstance(fbo_res, list):
            data["fbo"] = [o for o in fbo_res if isinstance(o, dict)]

        return data

    async def get_stock_info(self) -> List[Dict[str, Any]]:
        """
        Получение актуальных остатков товаров на складах WB.
        """
        date_from = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        result = await self._make_request(
            self.statistics_url,
            "/api/v1/supplier/stocks",
            params={"dateFrom": date_from},
            timeout=90.0,
        )
        return result if isinstance(result, list) else []

    async def get_report_detail(self, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """
        Получение детализированного финансового отчета по продажам за период.
        """
        params = {"dateFrom": date_from, "dateTo": date_to}
        result = await self._make_request(
            self.statistics_url,
            "/api/v1/supplier/reportDetailByPeriod",
            params=params,
            timeout=120.0,
        )
        return result if isinstance(result, list) else []

    async def get_sales_info(self, days: int = 1) -> List[Dict[str, Any]]:
        """
        Получение данных о продажах (выкупах) за период.
        """
        date_from = (datetime.now() - timedelta(days=int(days))).strftime("%Y-%m-%dT00:00:00Z")
        result = await self._make_request(
            self.statistics_url,
            "/api/v1/supplier/sales",
            params={"dateFrom": date_from},
            timeout=90.0,
        )
        return result if isinstance(result, list) else []

    async def get_sales_report(self, days: int = 1) -> List[Dict[str, Any]]:
        """
        Совместимый с send_morning_report метод получения продаж.
        """
        return await self.get_sales_info(days)

    # -------------------------------------------------------------------------
    # SEO search
    # -------------------------------------------------------------------------

    async def search_product_position(self, keyword: str, target_article: str) -> int:
        """
        Поиск позиции товара в выдаче WB по ключевому слову.
        В target_article обычно nmId (число строкой).
        """
        keyword = self._safe_str(keyword, max_len=200, default="")
        target_article = self._safe_str(target_article, max_len=64, default="").strip()

        if not keyword or not target_article:
            return 0

        logger.info(f"WB SEO: Поиск артикула {target_article} по запросу '{keyword}'")

        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
        async with httpx.AsyncClient(timeout=20.0, limits=limits) as client:
            # WB: до 100 товаров на страницу, максимум 10 страниц = топ-1000
            for page in range(1, 11):
                params = {
                    "appType": 1,
                    "curr": "rub",
                    "dest": -1257744,  # Москва
                    "query": keyword,
                    "resultset": "catalog",
                    "sort": "popular",
                    "page": page,
                }

                try:
                    resp = await client.get(self.search_url, params=params)
                    if resp.status_code != 200:
                        # 429/5xx — подождём и попробуем продолжить
                        if resp.status_code in (429, 500, 502, 503, 504):
                            sleep_s = min(2.0 * page, 10.0)
                            logger.warning(f"WB SEO transient {resp.status_code}, sleep {sleep_s}s (page={page})")
                            await asyncio.sleep(sleep_s)
                            continue

                        logger.error(f"WB Search Error: {resp.status_code} - {resp.text[:200]}")
                        break

                    data = resp.json()
                    products = data.get("data", {}).get("products", [])
                    if not isinstance(products, list) or not products:
                        break

                    for index, product in enumerate(products):
                        if not isinstance(product, dict):
                            continue
                        if str(product.get("id")) == target_article:
                            position = ((page - 1) * 100) + index + 1
                            logger.info(f"WB SEO: Товар {target_article} найден на {position} месте")
                            return position

                except Exception as e:
                    logger.error(f"Ошибка WB SEO (page={page}): {e}")
                    break

                await asyncio.sleep(0.3)

        logger.info(f"WB SEO: Товар {target_article} не найден в топ-1000")
        return 0

    # -------------------------------------------------------------------------
    # Content API v2 (cards)
    # -------------------------------------------------------------------------

    async def get_cards_list(self) -> List[Dict[str, Any]]:
        """
        Получение списка карточек товаров через Content API v2.
        Пагинация через cursor (updatedAt, nmID).
        """
        endpoint = "/content/v2/get/cards/list"

        payload: Dict[str, Any] = {
            "settings": {
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1},
            }
        }

        all_cards: List[Dict[str, Any]] = []
        seen_pages = 0
        max_pages = 200  # защита от бесконечного цикла

        while True:
            seen_pages += 1
            if seen_pages > max_pages:
                logger.error("WB get_cards_list: достигнут лимит страниц, прерываем (защита от бесконечного цикла).")
                break

            result = await self._make_request(
                self.content_url,
                endpoint,
                method="POST",
                json_data=payload,
                timeout=90.0,
            )

            if not isinstance(result, dict):
                logger.error("WB get_cards_list: ответ не dict или пустой.")
                break

            cards = result.get("cards", [])
            if not isinstance(cards, list):
                cards = []

            if not cards:
                # пустая страница — конец
                break

            all_cards.extend([c for c in cards if isinstance(c, dict)])

            cursor = result.get("cursor", {})
            if not isinstance(cursor, dict):
                break

            # Документация: курсор должен содержать nmID и updatedAt для следующей страницы
            nm_id = cursor.get("nmID")
            updated_at = cursor.get("updatedAt")

            if nm_id is None or updated_at is None:
                # если курсора нет — это последняя страница
                break

            payload["settings"]["cursor"]["nmID"] = nm_id
            payload["settings"]["cursor"]["updatedAt"] = updated_at

            # Лимиты API (примерно 100 запросов/мин) — выдержим паузу
            await asyncio.sleep(0.7)

        return all_cards

    async def get_all_products(self) -> List[Dict[str, Any]]:
        """
        Подготовка данных о товарах для сохранения в БД.
        Связывает карточки (Content API) с актуальными остатками (Statistics API).
        Возвращает список словарей в формате проекта (для db_functions.bulk_update_products).
        """
        cards = await self.get_cards_list()
        stocks = await self.get_stock_info()

        # В stocks структура может отличаться; чаще есть nmId и quantity
        stock_map: Dict[str, int] = {}
        for s in stocks:
            if not isinstance(s, dict):
                continue
            nm_id = s.get("nmId")
            qty = s.get("quantity")
            if nm_id is None:
                continue
            try:
                stock_map[str(nm_id)] = int(qty or 0)
            except Exception:
                stock_map[str(nm_id)] = 0

        products: List[Dict[str, Any]] = []
        for card in cards:
            if not isinstance(card, dict):
                continue

            nm_id = card.get("nmID") or card.get("nmId") or card.get("nm_id")
            vendor_code = card.get("vendorCode") or card.get("vendor_code")

            if not nm_id:
                continue

            title = card.get("title") or card.get("name") or f"WB: {vendor_code or nm_id}"

            products.append(
                {
                    "marketplace": "wb",  # КРИТИЧНО: единый идентификатор для БД и аналитики
                    "article": str(nm_id).strip().upper(),
                    "name": self._safe_str(title, max_len=255, default=f"WB Product {nm_id}"),
                    "cost_price": 0.0,
                    "extra_costs": 0.0,
                    "tax_rate": 0.06,
                    "stock": int(stock_map.get(str(nm_id), 0)),
                }
            )

        logger.info(f"WB API: подготовлено {len(products)} товаров.")
        return products

    # -------------------------------------------------------------------------
    # Prices / warehouses
    # -------------------------------------------------------------------------

    async def get_product_prices(self) -> List[Dict[str, Any]]:
        """
        Получение информации о ценах, скидках и промокодах.
        """
        result = await self._make_request(
            self.marketplace_url,
            "/api/v2/list/goods/filter",
            params={"limit": 1000},
            timeout=90.0,
        )
        if isinstance(result, dict) and "data" in result and isinstance(result["data"], dict):
            items = result["data"].get("listGoods", [])
            return items if isinstance(items, list) else []
        return []

    async def get_warehouses(self) -> List[Dict[str, Any]]:
        """
        Получение списка складов продавца (FBS).
        """
        result = await self._make_request(self.marketplace_url, "/api/v3/warehouses")
        return result if isinstance(result, list) else []
