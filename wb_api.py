import httpx
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Настройка логирования для отслеживания взаимодействия с API Wildberries
logger = logging.getLogger(__name__)

class WildberriesAPI:
    def __init__(self, token: str):
        """
        Инициализация клиента Wildberries. 
        Удаляем лишние пробелы из токена для предотвращения ошибок авторизации.
        """
        self.token = token.strip()
        self.headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Разделение базовых URL по функциональным сегментам API WB
        self.common_url = "https://common-api.wildberries.ru"
        self.finance_url = "https://finance-api.wildberries.ru"
        self.marketplace_url = "https://marketplace-api.wildberries.ru"
        self.statistics_url = "https://statistics-api.wildberries.ru"
        self.content_url = "https://content-api.wildberries.ru"
        
        # URL для поисковых запросов (публичный API для SEO мониторинга)
        self.search_url = "https://search.wb.ru/exactmatch/ru/common/v4/search"

    async def _make_request(self, base_url: str, endpoint: str, method: str = "GET", 
                            params: dict = None, json_data: dict = None, timeout: float = 60.0):
        """
        Универсальный метод для выполнения HTTP-запросов с логированием и обработкой лимитов.
        """
        url = f"{base_url}{endpoint}"
        
        async with httpx.AsyncClient(headers=self.headers, timeout=timeout) as client:
            for attempt in range(3):
                try:
                    if method.upper() == "GET":
                        response = await client.get(url, params=params)
                    else:
                        response = await client.post(url, json=json_data, params=params)
                    
                    # Логируем сырой ответ для отладки финансовых данных
                    if "balance" in endpoint:
                        logger.info(f"WB Balance Raw Response: {response.status_code} - {response.text}")

                    # Обработка успешного выполнения
                    if response.status_code == 200:
                        return response.json()
                    
                    # Обработка превышения лимитов запросов (429 Too Many Requests)
                    if response.status_code == 429:
                        wait_time = 30 * (attempt + 1)
                        logger.warning(f"WB 429: Лимит запросов. Ждем {wait_time}с...")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    # Логирование прочих ошибок HTTP
                    logger.error(f"WB API Error {response.status_code} на {endpoint}: {response.text}")
                    return None
                    
                except Exception as e:
                    logger.error(f"Исключение при запросе к WB ({endpoint}): {e}")
                    await asyncio.sleep(2)
            return None

    async def get_balance(self) -> float:
        """
        Получение баланса (доступно к выводу или текущий баланс).
        """
        result = await self._make_request(self.finance_url, "/api/v1/account/balance")
        if result and isinstance(result, dict):
            for_withdraw = result.get("for_withdraw", 0.0)
            current = result.get("current", 0.0)
            
            # Приоритет сумме к выводу, если 0 — берем текущий баланс
            final_balance = for_withdraw if for_withdraw > 0 else current
            
            logger.info(f"WB Balance calculated: {final_balance} (Withdraw: {for_withdraw}, Current: {current})")
            return float(final_balance)
                
        return 0.0

    async def validate_token(self) -> bool:
        """
        Проверка работоспособности токена через системный метод /ping.
        """
        result = await self._make_request(self.common_url, "/ping")
        return result is not None

    async def get_all_orders(self, days: int = 1) -> Dict[str, List]:
        """
        Сбор заказов по схемам FBS (маркетплейс) и FBO (склад WB) за указанный период.
        """
        data = {"fbs": [], "fbo": []}
        
        # 1. Загрузка новых сборочных заданий (FBS)
        fbs_res = await self._make_request(self.marketplace_url, "/api/v3/orders/new")
        if fbs_res and isinstance(fbs_res, dict) and "orders" in fbs_res:
            data["fbs"] = fbs_res["orders"]

        # 2. Загрузка заказов со склада WB (FBO) за период
        date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%dT00:00:00Z')
        fbo_res = await self._make_request(self.statistics_url, "/api/v1/supplier/orders", 
                                            params={"dateFrom": date_from})
        if isinstance(fbo_res, list):
            data["fbo"] = fbo_res
            
        return data

    async def get_stock_info(self) -> List[Dict]:
        """
        Получение актуальных остатков товаров на складах Wildberries.
        """
        date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        result = await self._make_request(self.statistics_url, "/api/v1/supplier/stocks", 
                                          params={"dateFrom": date_from})
        return result if isinstance(result, list) else []

    async def get_report_detail(self, date_from: str, date_to: str) -> List[Dict]:
        """
        Получение детализированного финансового отчета по продажам за период.
        """
        params = {"dateFrom": date_from, "dateTo": date_to}
        result = await self._make_request(self.statistics_url, "/api/v1/supplier/reportDetailByPeriod", 
                                          params=params, timeout=120.0)
        return result if isinstance(result, list) else []

    async def search_product_position(self, keyword: str, target_article: str) -> int:
        """
        Поиск позиции товара в выдаче WB по ключевому слову.
        """
        logger.info(f"WB SEO: Поиск артикула {target_article} по запросу '{keyword}'")
        
        target_article = str(target_article).strip()
        
        async with httpx.AsyncClient(timeout=20.0) as client:
            for page in range(1, 11):
                params = {
                    "appType": 1,
                    "curr": "rub",
                    "dest": -1257744, # Москва
                    "query": keyword,
                    "resultset": "catalog",
                    "sort": "popular",
                    "page": page
                }
                
                try:
                    response = await client.get(self.search_url, params=params)
                    if response.status_code != 200:
                        logger.error(f"WB Search Error: {response.status_code}")
                        break
                        
                    data = response.json()
                    products = data.get("data", {}).get("products", [])
                    
                    if not products:
                        break
                        
                    for index, product in enumerate(products):
                        if str(product.get("id")) == target_article:
                            position = ((page - 1) * 100) + index + 1
                            logger.info(f"WB SEO: Товар {target_article} найден на {position} месте")
                            return position
                            
                except Exception as e:
                    logger.error(f"Ошибка при парсинге WB SEO (стр {page}): {e}")
                    break
                
                await asyncio.sleep(0.3)
            
        logger.info(f"WB SEO: Товар {target_article} не найден в топ-1000")
        return 0

    # --- ОБНОВЛЕННЫЕ МЕТОДЫ CONTENT API V2 ---

    async def get_cards_list(self) -> List[Dict]:
        """
        Получение списка карточек товаров через Content API v2.
        Реализована пагинация согласно документации через cursor (updatedAt, nmID).
        """
        # Новый путь согласно актуальной документации WB v2
        endpoint = "/content/v2/get/cards/list"
        
        payload = {
            "settings": {
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1}
            }
        }
        
        all_cards = []
        while True:
            # Используем POST для получения списка карточек
            result = await self._make_request(self.content_url, endpoint, 
                                              method="POST", json_data=payload)
            
            if not result or "cards" not in result:
                logger.error("Не удалось получить карточки товаров или пустой ответ.")
                break
                
            cards = result.get("cards", [])
            all_cards.extend(cards)
            
            # Извлекаем курсор для следующей страницы
            cursor = result.get("cursor", {})
            total = cursor.get("total", 0)
            limit = payload["settings"]["cursor"]["limit"]
            
            # Условие выхода: если количество вернувшихся карточек меньше лимита, мы дошли до конца
            if total < limit:
                break
                
            # Обновляем курсор в payload для следующего запроса
            if "nmID" in cursor and "updatedAt" in cursor:
                payload["settings"]["cursor"]["nmID"] = cursor["nmID"]
                payload["settings"]["cursor"]["updatedAt"] = cursor["updatedAt"]
            else:
                # Если total >= limit, но курсор неполный — выходим во избежание бесконечного цикла
                break
            
            # Пауза согласно лимитам API (100 запросов в минуту, ~600мс интервал)
            await asyncio.sleep(0.7)

        return all_cards

    async def get_all_products(self) -> List[Dict]:
        """
        Подготовка данных о товарах для сохранения в БД.
        Связывает данные из карточек (Content API) с актуальными остатками (Statistics API).
        """
        # Загружаем все карточки продавца
        cards = await self.get_cards_list()
        
        # Загружаем текущие остатки (чтобы заполнить поле stock сразу)
        stocks = await self.get_stock_info()
        # Группируем остатки по nmId (артикул WB)
        stock_map = {str(s.get("nmId")): s.get("quantity", 0) for s in stocks}

        products = []
        for card in cards:
            nm_id = card.get("nmID")
            vendor_code = card.get("vendorCode")
            
            if not nm_id:
                continue
            
            # Название товара: приоритет полю 'title', если пусто — формируем из артикула
            title = card.get("title") or f"WB: {vendor_code}"
            
            products.append({
                "marketplace": "wildberries",
                "article": str(nm_id),          # В качестве уникального ID для WB используем nmID
                "name": title,
                "cost_price": 0.0,              # Значения по умолчанию для БД
                "extra_costs": 0.0,
                "tax_rate": 0.06,               # Ставка налога по умолчанию (УСН 6%)
                "stock": stock_map.get(str(nm_id), 0)
            })
            
        logger.info(f"WB API: Синхронизировано {len(products)} товаров.")
        return products

    async def get_sales_info(self, days: int = 1) -> List[Dict]:
        """
        Получение данных о продажах (выкупах) за период из статистики.
        """
        date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%dT00:00:00Z')
        result = await self._make_request(self.statistics_url, "/api/v1/supplier/sales", 
                                          params={"dateFrom": date_from})
        return result if isinstance(result, list) else []

    async def get_product_prices(self) -> List[Dict]:
        """
        Получение информации о ценах, скидках и промокодах.
        """
        result = await self._make_request(self.marketplace_url, "/api/v2/list/goods/filter", 
                                          params={"limit": 1000})
        if result and "data" in result:
            return result["data"].get("listGoods", [])
        return []

    async def get_warehouses(self) -> List[Dict]:
        """
        Получение списка складов продавца (FBS).
        """
        result = await self._make_request(self.marketplace_url, "/api/v3/warehouses")
        return result if isinstance(result, list) else []