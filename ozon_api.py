import httpx
import logging
import asyncio
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

# Настройка логирования
logger = logging.getLogger(__name__)

class OzonAPI:
    def __init__(self, client_id: str, api_key: str):
        self.client_id = str(client_id).strip()
        self.api_key = str(api_key).strip()
        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
        self.base_url = "https://api-seller.ozon.ru"

    async def _make_request(self, endpoint: str, payload: dict, method: str = "POST", timeout: float = 30.0):
        """ Универсальный метод для запросов к API Ozon """
        url = f"{self.base_url}{endpoint}"
        async with httpx.AsyncClient(headers=self.headers, timeout=timeout) as client:
            for attempt in range(3):
                try:
                    response = await client.request(method, url, json=payload)
                    if response.status_code == 200:
                        return response.json()
                    
                    logger.error(f"Ozon API Error {endpoint}: {response.status_code} - {response.text}")
                    if response.status_code in [429, 500, 502, 503, 504]:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    return None
                except Exception as e:
                    logger.error(f"Ozon Connection Error ({endpoint}): {e}")
                    await asyncio.sleep(1)
            return None

    async def get_stock_info(self) -> List[Dict]:
        """
        Получает детальную информацию о товарах.
        Исправлено: корректная обработка пагинации и вложенности 'items'.
        """
        all_product_ids = []
        last_id = ""
        
        # Шаг 1: Получаем список всех идентификаторов товаров
        while True:
            payload = {"filter": {"visibility": "ALL"}, "limit": 1000}
            if last_id:
                payload["last_id"] = last_id
            
            list_res = await self._make_request("/v3/product/list", payload)
            if not list_res or "result" not in list_res:
                break
                
            items = list_res.get("result", {}).get("items", [])
            for p in items:
                pid = p.get("product_id")
                if pid:
                    all_product_ids.append(int(pid))
            
            last_id = list_res.get("result", {}).get("last_id", "")
            if not last_id or len(items) < 1000:
                break
        
        if not all_product_ids:
            logger.warning("Ozon API: Товари не найдены в личном кабинете.")
            return []

        # Шаг 2: Получаем детали для каждого товара пачками по 100 штук
        all_details = []
        for i in range(0, len(all_product_ids), 100):
            chunk = all_product_ids[i:i + 100]
            # Запрос v3 требует ключ 'product_id' со списком ID
            info_payload = {"product_id": chunk}
            info_res = await self._make_request("/v3/product/info/list", info_payload)
            
            if info_res and "result" in info_res:
                res_data = info_res["result"]
                # Согласно дампу, товары могут быть в ключе 'items' внутри 'result'
                if isinstance(res_data, dict):
                    chunk_items = res_data.get("items", [])
                    all_details.extend(chunk_items)
                elif isinstance(res_data, list):
                    all_details.extend(res_data)
            
            await asyncio.sleep(0.2) # Небольшая пауза для соблюдения лимитов
                
        return all_details

    async def get_all_products(self) -> List[Dict]:
        """
        Преобразует сырые данные Ozon в стандартный формат бота.
        Исправлено: парсинг остатков (stocks) на основе присланных логов.
        """
        details = await self.get_stock_info()
        products = []
        
        for item in details:
            # offer_id - это артикул продавца
            offer_id = item.get("offer_id")
            if not offer_id:
                continue

            # Расчет общего остатка (сумма FBO и FBS)
            total_stock = 0
            stocks_data = item.get("stocks", {})
            
            # В предоставленном дампе структура: 'stocks': {'has_stock': True, 'stocks': [{'present': 1, ...}]}
            if isinstance(stocks_data, dict):
                inner_stocks_list = stocks_data.get("stocks", [])
                if isinstance(inner_stocks_list, list):
                    for s in inner_stocks_list:
                        total_stock += s.get("present", 0)
            
            # Если в объекте stocks пусто, проверяем альтернативные поля
            if total_stock == 0:
                total_stock = int(item.get("fbs_stocks", 0)) + int(item.get("fbo_stocks", 0))

            products.append({
                "marketplace": "ozon",
                "article": str(offer_id).strip().upper(),
                "name": item.get("name", f"Ozon Product {offer_id}"),
                "cost_price": 0.0,
                "extra_costs": 0.0,
                "tax_rate": 0.06,
                "stock": total_stock
            })
        
        logger.info(f"Ozon API: Синхронизировано {len(products)} товаров.")
        return products

    # --- Остальные методы для работы бота ---

    async def check_connection(self) -> Tuple[bool, float]:
        """ Проверка ключей и получение баланса """
        wh_check = await self._make_request("/v1/warehouse/list", {})
        if wh_check and ("result" in wh_check or isinstance(wh_check, list)):
            balance = await self.get_balance()
            return True, balance
        return False, 0.0

    async def get_balance(self) -> float:
        """ Получение текущего баланса из финансового отчета """
        now = datetime.now()
        payload = {
            "date_from": (now - timedelta(days=2)).strftime('%Y-%m-%d'),
            "date_to": now.strftime('%Y-%m-%d')
        }
        result = await self._make_request("/v1/finance/balance", payload)
        if not result: return 0.0
        try:
            return float(result.get("result", {}).get("total", {}).get("closing_balance", {}).get("value", 0.0))
        except: return 0.0

    async def get_all_orders(self, days: int = 1) -> Dict[str, List]:
        """ Сбор заказов FBS и FBO """
        now = datetime.now()
        date_from = (now - timedelta(days=days)).strftime('%Y-%m-%dT00:00:00Z')
        date_to = now.strftime('%Y-%m-%dT23:59:59Z')
        payload = {
            "dir": "desc",
            "filter": {"since": date_from, "to": date_to},
            "limit": 100,
            "with": {"financial_data": True}
        }
        results = {"fbs": [], "fbo": []}
        
        # Заказы FBS
        fbs_res = await self._make_request("/v3/posting/fbs/list", payload)
        if fbs_res:
            for p in fbs_res.get("result", {}).get("postings", []):
                for item in p.get("products", []):
                    results["fbs"].append({
                        "order_id": str(p.get("posting_number")),
                        "article": str(item.get("offer_id")).strip().upper(),
                        "name": item.get("name"),
                        "price": float(item.get("price", 0)), 
                        "date": p.get("in_process_at") or p.get("created_at")
                    })

        # Заказы FBO
        fbo_res = await self._make_request("/v2/posting/fbo/list", payload)
        if fbo_res:
            for p in fbo_res.get("result", []):
                for item in p.get("products", []):
                    results["fbo"].append({
                        "order_id": str(p.get("posting_number")),
                        "article": str(item.get("offer_id")).strip().upper(),
                        "name": item.get("name"),
                        "price": float(item.get("price", 0)),
                        "date": p.get("created_at")
                    })
        return results