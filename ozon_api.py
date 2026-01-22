"""
Версия файла: 1.2.0
Описание: Клиент Ozon Seller API (заказы, остатки, товары, финансы, позиции). Устойчив к ошибкам сети/лимитам.
Дата изменения: 2026-01-22
Изменения:
- Переработан _make_request: единый AsyncClient, таймауты, ретраи с backoff, обработка 401/403/429/5xx.
- Добавлены безопасные парсеры и нормализация данных (offer_id/article/price/date).
- get_all_orders и get_daily_stats возвращают консистентный формат {'fbs': [...], 'fbo': [...]}.
- Улучшен get_stock_info: пагинация /v3/product/list, батчи /v3/product/info/list, умеренное логирование (без огромных payload в логах).
- get_all_products: корректный подсчёт остатков по структурам stocks.
- Добавлены параметры debug и лимиты на объём логов.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger(__name__)


class OzonAPI:
    """
    Ozon Seller API client.
    Важно:
    - API требует заголовки Client-Id и Api-Key
    - многие методы возвращают result в разной структуре (dict/list) — нормализуем
    """

    BASE_URL = "https://api-seller.ozon.ru"

    def __init__(
        self,
        client_id: str,
        api_key: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
        debug: bool = False,
    ):
        self.client_id = str(client_id).strip()
        self.api_key = str(api_key).strip()
        self.timeout = float(timeout)
        self.max_retries = int(max_retries)
        self.debug = bool(debug)

        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }
        self.base_url = self.BASE_URL

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
        return self._safe_str(value, max_len=128, default="").upper()

    def _fmt_dt_range(self, date_from: datetime, date_to: datetime) -> Tuple[str, str]:
        """
        Ozon ожидает ISO с Z.
        """
        return (
            date_from.strftime("%Y-%m-%dT00:00:00Z"),
            date_to.strftime("%Y-%m-%dT23:59:59Z"),
        )

    # -------------------------------------------------------------------------
    # HTTP request
    # -------------------------------------------------------------------------

    async def _make_request(
        self,
        endpoint: str,
        payload: dict,
        *,
        method: str = "POST",
        timeout: Optional[float] = None,
    ) -> Optional[dict]:
        """
        Унифицированный запрос к Ozon API с ретраями.
        Возвращает dict (response.json) или None при ошибке.
        """
        url = f"{self.base_url}{endpoint}"
        t = float(timeout) if timeout is not None else self.timeout

        # Backoff: 1s, 2s, 4s ... (с потолком)
        base_sleep = 1.0

        limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
        async with httpx.AsyncClient(headers=self.headers, timeout=t, limits=limits) as client:
            for attempt in range(1, self.max_retries + 1):
                try:
                    resp = await client.request(method, url, json=payload)

                    # 200 OK
                    if resp.status_code == 200:
                        try:
                            return resp.json()
                        except Exception as e:
                            logger.error(f"Ozon JSON decode error {endpoint}: {e}")
                            return None

                    # Auth errors - ретраить бессмысленно
                    if resp.status_code in (401, 403):
                        logger.error(
                            f"Ozon auth error {endpoint}: {resp.status_code} - {resp.text[:300]}"
                        )
                        return None

                    # Rate limit / transient server errors
                    if resp.status_code in (429, 500, 502, 503, 504):
                        # Если Ozon отдаёт Retry-After — учитываем
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            try:
                                sleep_s = max(1.0, float(retry_after))
                            except Exception:
                                sleep_s = base_sleep * (2 ** (attempt - 1))
                        else:
                            sleep_s = base_sleep * (2 ** (attempt - 1))

                        sleep_s = min(sleep_s, 20.0)

                        logger.warning(
                            f"Ozon transient error {endpoint}: {resp.status_code}, attempt {attempt}/{self.max_retries}, sleep {sleep_s}s"
                        )
                        await asyncio.sleep(sleep_s)
                        continue

                    # Остальные коды — считаем ошибкой
                    logger.error(
                        f"Ozon API error {endpoint}: {resp.status_code} - {resp.text[:500]}"
                    )
                    return None

                except (httpx.TimeoutException, httpx.NetworkError) as e:
                    sleep_s = min(base_sleep * (2 ** (attempt - 1)), 10.0)
                    logger.warning(
                        f"Ozon connection/timeout {endpoint}: {e} (attempt {attempt}/{self.max_retries}), sleep {sleep_s}s"
                    )
                    await asyncio.sleep(sleep_s)
                    continue
                except Exception as e:
                    logger.error(f"Ozon unexpected error {endpoint}: {e}")
                    return None

        return None

    # -------------------------------------------------------------------------
    # Public methods
    # -------------------------------------------------------------------------

    async def get_balance(self) -> float:
        """
        Баланс: /v1/finance/balance
        Возвращает closing_balance.value или 0.0
        """
        now = datetime.now()
        payload = {
            "date_from": (now - timedelta(days=2)).strftime("%Y-%m-%d"),
            "date_to": now.strftime("%Y-%m-%d"),
        }
        result = await self._make_request("/v1/finance/balance", payload)
        if not result:
            return 0.0

        try:
            data_root = result.get("result", result)
            total_data = data_root.get("total", {})
            return float(total_data.get("closing_balance", {}).get("value", 0.0))
        except Exception:
            return 0.0

    async def check_connection(self) -> Tuple[bool, float]:
        """
        Быстрая проверка работоспособности ключей.
        """
        wh_check = await self._make_request("/v1/warehouse/list", {})
        if wh_check and ("result" in wh_check or isinstance(wh_check, list)):
            balance = await self.get_balance()
            return True, balance
        return False, 0.0

    async def get_all_orders(self, days: int = 1) -> Dict[str, List[Dict[str, Any]]]:
        """
        Возвращает заказы за период (days) в едином формате:
        {
          "fbs": [{"posting_number":..., "products":[...], ...}, ...],
          "fbo": [{"posting_number":..., "products":[...], ...}, ...]
        }

        ВАЖНО:
        - scheduler_tasks.py ожидает 'posting_number' и 'products' для fbs.
        """
        now = datetime.now()
        date_from = now - timedelta(days=int(days))
        date_to = now

        since, to = self._fmt_dt_range(date_from, date_to)

        payload = {
            "dir": "desc",
            "filter": {"since": since, "to": to},
            "limit": 100,
            "with": {"financial_data": True},
        }

        results: Dict[str, List[Dict[str, Any]]] = {"fbs": [], "fbo": []}

        # FBS
        fbs_res = await self._make_request("/v3/posting/fbs/list", payload)
        if fbs_res:
            postings = fbs_res.get("result", {}).get("postings", [])
            if isinstance(postings, list):
                for p in postings:
                    if not isinstance(p, dict):
                        continue
                    results["fbs"].append(p)

        # FBO
        fbo_res = await self._make_request("/v2/posting/fbo/list", payload)
        if fbo_res:
            # В некоторых методах result может быть списком
            data = fbo_res.get("result", fbo_res)
            if isinstance(data, dict):
                postings = data.get("postings") or data.get("result") or []
            else:
                postings = data
            if isinstance(postings, list):
                for p in postings:
                    if not isinstance(p, dict):
                        continue
                    results["fbo"].append(p)

        return results

    async def get_daily_stats(self, date_str: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Возвращает FBS и FBO заказы за одну дату (date_str: YYYY-MM-DD).
        Формат такой же, как у get_all_orders.
        """
        try:
            day = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
        except Exception:
            # если передали неверно — используем вчера
            day = datetime.now() - timedelta(days=1)

        since, to = self._fmt_dt_range(day, day)

        payload = {
            "dir": "desc",
            "filter": {"since": since, "to": to},
            "limit": 100,
            "with": {"financial_data": True},
        }

        results: Dict[str, List[Dict[str, Any]]] = {"fbs": [], "fbo": []}

        fbs_res = await self._make_request("/v3/posting/fbs/list", payload)
        if fbs_res:
            postings = fbs_res.get("result", {}).get("postings", [])
            if isinstance(postings, list):
                results["fbs"] = [p for p in postings if isinstance(p, dict)]

        fbo_res = await self._make_request("/v2/posting/fbo/list", payload)
        if fbo_res:
            data = fbo_res.get("result", fbo_res)
            if isinstance(data, dict):
                postings = data.get("postings") or data.get("result") or []
            else:
                postings = data
            if isinstance(postings, list):
                results["fbo"] = [p for p in postings if isinstance(p, dict)]

        return results

    async def get_stock_info(self) -> List[Dict[str, Any]]:
        """
        Получение детальной информации по товарам через:
        1) /v3/product/list (получаем product_id)
        2) /v3/product/info/list (получаем stocks и прочее)

        Возвращает список items (детальные структуры Ozon).
        """
        all_product_ids: List[int] = []
        last_id: str = ""

        # 1) Сбор product_id
        while True:
            payload: Dict[str, Any] = {"filter": {"visibility": "ALL"}, "limit": 1000}
            if last_id:
                payload["last_id"] = last_id

            list_res = await self._make_request("/v3/product/list", payload)
            if not list_res:
                logger.error("Ozon get_stock_info: /v3/product/list вернул None")
                break

            items = list_res.get("result", {}).get("items", [])
            if not isinstance(items, list):
                items = []

            if self.debug:
                logger.info(f"Ozon DEBUG: /v3/product/list items={len(items)} last_id={last_id}")

            for p in items:
                if not isinstance(p, dict):
                    continue
                pid = p.get("product_id")
                if pid is not None:
                    try:
                        all_product_ids.append(int(pid))
                    except Exception:
                        continue

            last_id = list_res.get("result", {}).get("last_id", "")
            if not last_id or len(items) < 1000:
                break

            await asyncio.sleep(0.1)

        if not all_product_ids:
            logger.warning("Ozon get_stock_info: product_id список пуст.")
            return []

        # 2) Сбор деталей по батчам
        all_details: List[Dict[str, Any]] = []
        for i in range(0, len(all_product_ids), 100):
            chunk = all_product_ids[i : i + 100]
            info_payload = {"product_id": chunk}

            if self.debug:
                # ограничим размер лога
                logger.info(f"Ozon DEBUG: /v3/product/info/list batch size={len(chunk)}")

            info_res = await self._make_request("/v3/product/info/list", info_payload)
            if not info_res:
                logger.error("Ozon get_stock_info: /v3/product/info/list вернул None")
                await asyncio.sleep(0.2)
                continue

            res_data = info_res.get("result")
            chunk_items: List[Dict[str, Any]] = []

            if isinstance(res_data, dict):
                items = res_data.get("items", [])
                if isinstance(items, list):
                    chunk_items = [x for x in items if isinstance(x, dict)]
            elif isinstance(res_data, list):
                chunk_items = [x for x in res_data if isinstance(x, dict)]
            else:
                # иногда API отдаёт result в неожиданном виде
                if self.debug:
                    logger.info(f"Ozon DEBUG: unexpected result type in info/list: {type(res_data)}")

            all_details.extend(chunk_items)
            await asyncio.sleep(0.2)

        return all_details

    async def get_all_products(self) -> List[Dict[str, Any]]:
        """
        Возвращает список товаров в формате проекта для синхронизации в БД:
        [
          {
            "marketplace":"ozon",
            "article":"OFFER_ID",
            "name":"Название",
            "cost_price":0.0,
            "extra_costs":0.0,
            "tax_rate":0.06,
            "stock": 10
          }, ...
        ]
        """
        details = await self.get_stock_info()
        if not details:
            logger.error("Ozon get_all_products: детальная информация не получена.")
            return []

        products: List[Dict[str, Any]] = []
        for item in details:
            if not isinstance(item, dict):
                continue

            offer_id = item.get("offer_id") or item.get("id") or item.get("product_id")
            if not offer_id:
                continue

            # Подсчёт остатков: Ozon может отдавать по разным структурам
            total_stock = 0

            stocks = item.get("stocks")
            if isinstance(stocks, dict):
                inner = stocks.get("stocks")
                if isinstance(inner, list):
                    # склады могут иметь present/reserved
                    total_stock = sum(int(s.get("present", 0) or 0) for s in inner if isinstance(s, dict))
                else:
                    total_stock = int(stocks.get("present", 0) or 0)

            # fallback
            if total_stock == 0:
                total_stock = int(item.get("fbs_stocks", 0) or 0) + int(item.get("fbo_stocks", 0) or 0)

            products.append(
                {
                    "marketplace": "ozon",
                    "article": self._norm_article(offer_id),
                    "name": self._safe_str(item.get("name"), max_len=255, default=f"Ozon Product {offer_id}"),
                    "cost_price": 0.0,
                    "extra_costs": 0.0,
                    "tax_rate": 0.06,
                    "stock": int(total_stock),
                }
            )

        logger.info(f"Ozon API: Итого подготовлено {len(products)} товаров.")
        return products

    # -------------------------------------------------------------------------
    # Other methods (kept, but with minor hardening)
    # -------------------------------------------------------------------------

    async def search_product_position(self, keyword: str, target_article: str) -> int:
        endpoint = "/v1/product/search/list"
        keyword = self._safe_str(keyword, max_len=200, default="")
        target_article = self._norm_article(target_article)
        if not keyword or not target_article:
            return 0

        for page in range(5):
            payload = {"text": keyword, "limit": 100, "offset": page * 100}
            res = await self._make_request(endpoint, payload)
            if not res or "result" not in res:
                break
            items = res["result"].get("items", [])
            if not isinstance(items, list):
                break
            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                if self._norm_article(item.get("offer_id", "")) == target_article:
                    return (page * 100) + index + 1
            await asyncio.sleep(0.5)

        return 0

    async def get_transaction_report(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        endpoint = "/v3/finance/transaction/list"
        all_operations: List[Dict[str, Any]] = []
        page = 1

        while True:
            from_s, to_s = self._fmt_dt_range(date_from, date_to)
            payload = {
                "filter": {
                    "date": {"from": from_s, "to": to_s},
                    "transaction_type": "all",
                },
                "page": page,
                "page_size": 1000,
            }
            result = await self._make_request(endpoint, payload)
            if not result or "result" not in result:
                break

            ops = result["result"].get("operations", [])
            if not isinstance(ops, list) or not ops:
                break

            all_operations.extend([o for o in ops if isinstance(o, dict)])

            page_count = int(result["result"].get("page_count", 1) or 1)
            if page >= page_count:
                break

            page += 1
            await asyncio.sleep(0.2)

        return all_operations

    async def get_product_info(self, offer_id: str) -> Optional[Dict[str, Any]]:
        offer_id = self._safe_str(offer_id, max_len=128, default="")
        if not offer_id:
            return None
        result = await self._make_request("/v2/product/info", {"offer_id": offer_id})
        return result.get("result") if result else None

    async def get_fbo_inventory(self) -> List[Dict[str, Any]]:
        result = await self._make_request("/v2/analytics/stock_on_warehouses", {"limit": 1000, "offset": 0})
        if not result:
            return []
        rows = result.get("result", {}).get("rows", [])
        return rows if isinstance(rows, list) else []

    async def get_product_prices(self, offer_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        offer_ids = offer_ids or []
        offer_ids = [self._safe_str(x, max_len=128, default="") for x in offer_ids if x]
        payload = {"filter": {"offer_id": offer_ids, "visibility": "ALL"}, "limit": 1000}
        result = await self._make_request("/v4/product/info/prices", payload)
        if not result:
            return []
        items = result.get("result", {}).get("items", [])
        return items if isinstance(items, list) else []
