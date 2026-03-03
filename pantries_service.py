from collections import OrderedDict
import re
import time
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Callable, Dict, List, Optional, Tuple

from firebase_admin import firestore
import requests
from google.api_core import exceptions as gcloud_exceptions


SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP
MAX_TRANSACTION_RETRIES = 3
DEFAULT_SEARCH_LIMIT = 15
MAX_SEARCH_LIMIT = 50
MIN_SEARCH_QUERY_LENGTH = 2
OFF_MAX_RETRIES = 2
OFF_RETRY_BACKOFF_SECONDS = 0.2
OFF_RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
OFF_SEARCH_TIMEOUT: Tuple[float, float] = (2.0, 8.0)
OFF_LEGACY_SEARCH_TIMEOUT: Tuple[float, float] = (3.05, 15.0)
OFF_BARCODE_TIMEOUT: Tuple[float, float] = (3.05, 8.0)
OFF_SEARCH_MAX_RETRIES = 1
OFF_BARCODE_MAX_RETRIES = OFF_MAX_RETRIES
OFF_SEARCH_URL = "https://search.openfoodfacts.org/search"
OFF_LEGACY_SEARCH_URL = "https://world.openfoodfacts.org/cgi/search.pl"
OFF_DEFAULT_HEADERS = {
    "User-Agent": "SmartPantryBackend/1.0 (smartpantry@localhost)",
    "Accept": "application/json",
}
FALLBACK_FIRESTORE_SCAN_LIMIT = 800
SEARCH_CACHE_MAX_ENTRIES = 3000
SEARCH_CACHE_TTL_SECONDS = 60 * 60 * 24


class PantriesError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class PantriesService:
    def __init__(self, db: Any):
        self._db = db
        # Volatile LRU+TTL cache for recent product lookups.
        self._search_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    def search_products(
        self,
        query: Any,
        similar: bool = False,
        limit: Any = DEFAULT_SEARCH_LIMIT,
        lang: Any = "it",
    ) -> Dict[str, Any]:
        validated_query = self._validate_search_query(query)
        validated_limit = self._validate_search_limit(limit)
        validated_lang = self._validate_search_lang(lang)
        off_exception: Optional[PantriesError] = None
        off_products: List[Dict[str, Any]] = []
        try:
            off_products = self.search_open_food_facts(
                query=validated_query,
                limit=validated_limit,
                lang=validated_lang,
            )
        except PantriesError as exc:
            if exc.status_code == 502:
                off_exception = exc
            else:
                raise

        fallback_products = self.search_firestore_products(
            query=validated_query,
            limit=max(validated_limit * 2, 20),
        )
        products = self._merge_search_products(off_products, fallback_products)

        if not products and off_exception is not None:
            raise off_exception
        recommended: List[Dict[str, Any]] = []

        if similar:
            recommended = self._build_recommended_products(
                query=validated_query,
                products=products,
                limit=validated_limit,
            )

        return {
            "query": validated_query,
            "products": products,
            "recommended": recommended,
        }

    def search_open_food_facts(
        self,
        query: str,
        limit: int = DEFAULT_SEARCH_LIMIT,
        lang: str = "it",
    ) -> List[Dict[str, Any]]:
        page_size = min(max(limit, 20), MAX_SEARCH_LIMIT)
        search_fields = ",".join(
            [
                "code",
                "product_name",
                f"product_name_{lang}",
                "generic_name",
                f"generic_name_{lang}",
                "brands",
                "brands_tags",
            ]
        )
        search_backends = [
            {
                "url": OFF_SEARCH_URL,
                "params": {
                    "q": query,
                    "page_size": page_size,
                    "fields": search_fields,
                },
                "timeout": OFF_SEARCH_TIMEOUT,
            },
            {
                "url": OFF_LEGACY_SEARCH_URL,
                "params": {
                    "search_terms": query,
                    "search_simple": 1,
                    "action": "process",
                    "json": 1,
                    "page_size": page_size,
                    "lc": lang,
                },
                "timeout": OFF_LEGACY_SEARCH_TIMEOUT,
            },
        ]

        last_exception: Optional[PantriesError] = None
        for backend in search_backends:
            try:
                data = self._off_get_json(
                    url=backend["url"],
                    params=backend["params"],
                    timeout=backend["timeout"],
                    max_retries=OFF_SEARCH_MAX_RETRIES,
                )
            except PantriesError as exc:
                last_exception = exc
                continue

            products = self._extract_off_search_products(data)
            if not products:
                continue

            normalized_products = [
                self._map_off_search_product(product=p, preferred_lang=lang)
                for p in products
            ]
            self._cache_off_search_products(products, preferred_lang=lang)
            return normalized_products

        if last_exception is not None:
            raise last_exception
        return []

    def get_open_food_facts_product(self, barcode: str) -> Dict[str, Any]:
        normalized_barcode = self._normalize_barcode(barcode)

        url = f"https://world.openfoodfacts.org/api/v0/product/{normalized_barcode}.json"

        data = self._off_get_json(
            url=url,
            params=None,
            timeout=OFF_BARCODE_TIMEOUT,
            max_retries=OFF_BARCODE_MAX_RETRIES,
        )
        if data.get("status") != 1:
            raise PantriesError("Prodotto non trovato su OpenFoodFacts", 404)

        product = data.get("product", {})
        mapped_product = self._map_off_product(product)
        self._upsert_search_cache_entry(
            open_food_facts_id=mapped_product.get("openFoodFactsId", ""),
            product_name=mapped_product.get("productName", ""),
            brands=mapped_product.get("brands", ""),
            nutrients=mapped_product.get("nutrients") or {},
        )
        return mapped_product

    def add_or_upsert_item(
        self,
        uid: Any,
        open_food_facts_id: Any,
        quantity_delta: Any,
        product_name: Any = None,
        nutrients: Any = None,
    ) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_id = self._validate_open_food_facts_id(open_food_facts_id)
        validated_delta = self._validate_positive_int(quantity_delta, "quantityDelta")
        validated_product_name = self._validate_product_name(product_name)
        validated_nutrients = self._validate_nutrients(nutrients)
        should_update_product_name = bool(validated_product_name)
        should_update_nutrients = bool(validated_nutrients)
        cached_entry = self._get_cached_product_entry(validated_id)
        cached_name = self._normalize_text(cached_entry.get("product_name"))
        cached_nutrients = self._validate_nutrients(cached_entry.get("nutrients"))

        # Riferimento: users/{uid}/pantry/{itemId}
        item_ref = self._get_pantry_collection(validated_uid).document(validated_id)

        def _tx(transaction: Any) -> Dict[str, Any]:
            snapshot = item_ref.get(transaction=transaction)
            created = not snapshot.exists

            if created:
                new_quantity = validated_delta
                final_product_name = validated_product_name or cached_name
                final_nutrients = validated_nutrients or cached_nutrients
                create_payload = {
                    "openFoodFactsId": validated_id,
                    "productName": final_product_name or "Prodotto sconosciuto",
                    "quantity": new_quantity,
                    "lastUpdated": SERVER_TIMESTAMP,
                }
                if final_nutrients:
                    create_payload["nutrients"] = final_nutrients
                transaction.set(
                    item_ref,
                    create_payload,
                )
            else:
                current_quantity = self._parse_stored_quantity(
                    snapshot.get("quantity"), validated_id
                )
                current_product_name = self._normalize_stored_product_name(
                    snapshot.get("productName")
                )
                new_quantity = current_quantity + validated_delta
                update_payload = {
                    "quantity": new_quantity,
                    "lastUpdated": SERVER_TIMESTAMP,
                }
                final_product_name = current_product_name
                current_nutrients = self._validate_nutrients(snapshot.get("nutrients"))
                final_nutrients = current_nutrients
                if should_update_product_name:
                    update_payload["productName"] = validated_product_name
                    final_product_name = validated_product_name
                if should_update_nutrients:
                    update_payload["nutrients"] = validated_nutrients
                    final_nutrients = validated_nutrients
                transaction.update(
                    item_ref,
                    update_payload,
                )

            return {
                "openFoodFactsId": validated_id,
                "productName": final_product_name,
                "quantity": new_quantity,
                "nutrients": final_nutrients,
                "created": created,
            }

        result = self._run_transaction(_tx)
        self._upsert_search_cache_entry(
            open_food_facts_id=result["openFoodFactsId"],
            product_name=result["productName"],
            nutrients=result.get("nutrients") or validated_nutrients or cached_nutrients,
        )
        return result

    def set_item_quantity(
        self,
        uid: Any,
        open_food_facts_id: Any,
        quantity: Any,
        product_name: Any = None,
        nutrients: Any = None,
        allow_zero: bool = True,
    ) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_id = self._validate_open_food_facts_id(open_food_facts_id)
        validated_quantity = self._validate_non_negative_int(quantity, "quantity")
        if not allow_zero and validated_quantity == 0:
            raise PantriesError("quantity deve essere un intero >= 1", status_code=400)
        validated_product_name = self._validate_product_name(product_name)
        validated_nutrients = self._validate_nutrients(nutrients)
        should_update_product_name = bool(validated_product_name)
        should_update_nutrients = bool(validated_nutrients)
        cached_entry = self._get_cached_product_entry(validated_id)
        cached_name = self._normalize_text(cached_entry.get("product_name"))
        cached_nutrients = self._validate_nutrients(cached_entry.get("nutrients"))

        item_ref = self._get_pantry_collection(validated_uid).document(validated_id)

        def _tx(transaction: Any) -> Dict[str, Any]:
            snapshot = item_ref.get(transaction=transaction)
            exists = snapshot.exists
            current_product_name = self._normalize_stored_product_name(
                snapshot.get("productName")
            )
            current_nutrients = self._validate_nutrients(snapshot.get("nutrients"))

            final_product_name = (
                validated_product_name
                or current_product_name
                or cached_name
                or "Prodotto sconosciuto"
            )
            final_nutrients = validated_nutrients or current_nutrients or cached_nutrients

            if validated_quantity == 0:
                if exists:
                    transaction.delete(item_ref)
                return {
                    "openFoodFactsId": validated_id,
                    "productName": final_product_name,
                    "quantity": 0,
                    "nutrients": final_nutrients,
                    "created": False,
                    "deleted": exists,
                }

            if not exists:
                create_payload = {
                    "openFoodFactsId": validated_id,
                    "productName": final_product_name,
                    "quantity": validated_quantity,
                    "lastUpdated": SERVER_TIMESTAMP,
                }
                if final_nutrients:
                    create_payload["nutrients"] = final_nutrients
                transaction.set(item_ref, create_payload)
                return {
                    "openFoodFactsId": validated_id,
                    "productName": final_product_name,
                    "quantity": validated_quantity,
                    "nutrients": final_nutrients,
                    "created": True,
                    "deleted": False,
                }

            update_payload = {
                "quantity": validated_quantity,
                "lastUpdated": SERVER_TIMESTAMP,
            }
            if should_update_product_name:
                update_payload["productName"] = validated_product_name
                final_product_name = validated_product_name
            if should_update_nutrients:
                update_payload["nutrients"] = validated_nutrients
                final_nutrients = validated_nutrients
            transaction.update(item_ref, update_payload)
            return {
                "openFoodFactsId": validated_id,
                "productName": final_product_name,
                "quantity": validated_quantity,
                "nutrients": final_nutrients,
                "created": False,
                "deleted": False,
            }

        result = self._run_transaction(_tx)
        if result.get("quantity", 0) > 0:
            self._upsert_search_cache_entry(
                open_food_facts_id=result["openFoodFactsId"],
                product_name=result["productName"],
                nutrients=result.get("nutrients") or validated_nutrients or cached_nutrients,
            )
        return result

    def decrement_item(self, uid: Any, open_food_facts_id: Any) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_id = self._validate_open_food_facts_id(open_food_facts_id)

        item_ref = self._get_pantry_collection(validated_uid).document(validated_id)

        def _tx(transaction: Any) -> Dict[str, Any]:
            snapshot = item_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise PantriesError(
                    "Item non trovato nella dispensa", status_code=404
                )

            current_quantity = self._parse_stored_quantity(
                snapshot.get("quantity"), validated_id
            )
            current_product_name = self._normalize_stored_product_name(
                snapshot.get("productName")
            )
            current_nutrients = self._validate_nutrients(snapshot.get("nutrients"))

            if current_quantity == 1:
                transaction.delete(item_ref)
                new_quantity = 0
                deleted = True
            else:
                new_quantity = current_quantity - 1
                deleted = False
                transaction.update(
                    item_ref,
                    {"quantity": new_quantity, "lastUpdated": SERVER_TIMESTAMP},
                )

            return {
                "openFoodFactsId": validated_id,
                "productName": current_product_name,
                "quantity": new_quantity,
                "nutrients": current_nutrients,
                "deleted": deleted,
            }

        return self._run_transaction(_tx)

    def delete_item(self, uid: Any, open_food_facts_id: Any) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_id = self._validate_open_food_facts_id(open_food_facts_id)

        item_ref = self._get_pantry_collection(validated_uid).document(validated_id)

        def _tx(transaction: Any) -> Dict[str, Any]:
            snapshot = item_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise PantriesError(
                    "Item non trovato nella dispensa", status_code=404
                )

            transaction.delete(item_ref)
            return {"openFoodFactsId": validated_id}

        return self._run_transaction(_tx)

    def list_items(self, uid: Any) -> List[Dict[str, Any]]:
        validated_uid = self._validate_uid(uid)
        items_ref = self._get_pantry_collection(validated_uid)
        docs = items_ref.stream() # Recupera tutti i documenti

        items: List[Dict[str, Any]] = []
        for doc in docs:
            data = doc.to_dict() or {}
            open_food_facts_id = data.get("openFoodFactsId") or getattr(doc, "id", None)
            if not open_food_facts_id:
                continue

            quantity = self._parse_stored_quantity(
                data.get("quantity"), str(open_food_facts_id)
            )
            product_name = self._normalize_stored_product_name(data.get("productName"))
            item_payload = {
                "openFoodFactsId": str(open_food_facts_id),
                "productName": product_name,
                "quantity": quantity,
            }
            nutrients = self._validate_nutrients(data.get("nutrients"))
            if nutrients:
                item_payload["nutrients"] = nutrients
            items.append(item_payload)

        items.sort(key=lambda x: x["productName"].lower())
        return items

    def _run_transaction(self, callback: Callable[[Any], Dict[str, Any]]) -> Dict[str, Any]:
        for attempt in range(MAX_TRANSACTION_RETRIES):
            transaction = self._db.transaction()
            try:
                use_firestore_transaction = callable(
                    getattr(firestore, "transactional", None)
                ) and hasattr(transaction, "_id")

                if use_firestore_transaction:
                    result = firestore.transactional(callback)(transaction)
                else:
                    result = callback(transaction)
                    if hasattr(transaction, "commit"):
                        transaction.commit()
                return result
            except PantriesError:
                self._rollback_quietly(transaction)
                raise
            except gcloud_exceptions.Aborted as exc:
                self._rollback_quietly(transaction)
                if attempt == MAX_TRANSACTION_RETRIES - 1:
                    raise PantriesError(
                        "Transazione Firestore fallita, riprova",
                        status_code=409,
                    ) from exc
            except Exception:
                self._rollback_quietly(transaction)
                raise

        raise PantriesError("Transazione Firestore fallita", status_code=409)

    @staticmethod
    def _rollback_quietly(transaction: Any) -> None:
        if hasattr(transaction, "rollback"):
            try:
                transaction.rollback()
            except Exception:
                return

    def _get_pantry_collection(self, uid: str) -> Any:
        # Struttura richiesta: users/{uid}/pantry
        return self._db.collection("users").document(uid).collection("pantry")

    def search_firestore_products(self, query: str, limit: int) -> List[Dict[str, Any]]:
        by_code: Dict[str, Dict[str, Any]] = {}

        # 1) Search recent OFF lookups cached in memory (no Firestore persistence).
        for code, data in self._iter_cached_search_entries():
            by_code[code] = {
                "code": code,
                "product_name": self._normalize_text(data.get("product_name")),
                "brands": self._normalize_text(data.get("brands")),
            }

        # 2) Fallback to live pantry documents.
        collection_group = getattr(self._db, "collection_group", None)
        if callable(collection_group):
            try:
                docs = (
                    collection_group("pantry")
                    .limit(FALLBACK_FIRESTORE_SCAN_LIMIT)
                    .stream()
                )
                for doc in docs:
                    data = doc.to_dict() or {}
                    code = self._normalize_text(data.get("openFoodFactsId")) or self._normalize_text(
                        getattr(doc, "id", "")
                    )
                    if not code:
                        continue

                    product_name = self._normalize_text(data.get("productName"))
                    if code not in by_code or (
                        product_name and not by_code[code].get("product_name")
                    ):
                        by_code[code] = {
                            "code": code,
                            "product_name": product_name,
                            "brands": "",
                        }
            except Exception:
                pass

        products = list(by_code.values())
        if not products:
            return []

        ranked = self._build_recommended_products(
            query=query,
            products=products,
            limit=min(limit, len(products)),
        )
        return ranked

    @staticmethod
    def _merge_search_products(
        primary_products: List[Dict[str, Any]],
        secondary_products: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen_codes: set[str] = set()

        for product in primary_products + secondary_products:
            code = PantriesService._normalize_text(product.get("code"))
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            merged.append(product)

        return merged

    def _cache_off_search_products(
        self, raw_products: List[Dict[str, Any]], preferred_lang: str
    ) -> None:
        for product in raw_products:
            code = self._normalize_text(product.get("code"))
            if not code:
                continue

            self._upsert_search_cache_entry(
                open_food_facts_id=code,
                product_name=self._extract_off_product_name(
                    product, preferred_lang=preferred_lang, fallback=""
                ),
                brands=self._extract_off_brands(product),
                nutrients=self._extract_off_nutrients(product),
            )

    @staticmethod
    def _extract_off_search_products(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        for key in ("hits", "products"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    def _upsert_search_cache_entry(
        self,
        open_food_facts_id: str,
        product_name: str,
        brands: str = "",
        nutrients: Optional[Dict[str, float]] = None,
    ) -> None:
        normalized_code = self._normalize_text(open_food_facts_id)
        if not normalized_code:
            return

        try:
            normalized_nutrients = self._validate_nutrients(nutrients)
        except PantriesError:
            normalized_nutrients = {}

        self._cleanup_expired_search_cache()
        if normalized_code in self._search_cache:
            self._search_cache.pop(normalized_code, None)

        payload: Dict[str, Any] = {
            "product_name": self._normalize_text(product_name),
            "brands": self._normalize_text(brands),
            "expires_at": time.time() + SEARCH_CACHE_TTL_SECONDS,
        }
        if normalized_nutrients:
            payload["nutrients"] = normalized_nutrients

        self._search_cache[normalized_code] = payload
        try:
            while len(self._search_cache) > SEARCH_CACHE_MAX_ENTRIES:
                self._search_cache.popitem(last=False)
        except Exception:
            return

    def _iter_cached_search_entries(self) -> List[Tuple[str, Dict[str, Any]]]:
        self._cleanup_expired_search_cache()
        entries: List[Tuple[str, Dict[str, Any]]] = []
        for code, data in self._search_cache.items():
            entries.append((code, dict(data)))
        return entries

    def _cleanup_expired_search_cache(self) -> None:
        if not self._search_cache:
            return
        now = time.time()
        expired_codes = [
            code
            for code, data in self._search_cache.items()
            if float(data.get("expires_at", 0)) <= now
        ]
        for code in expired_codes:
            self._search_cache.pop(code, None)

    def _build_recommended_products(
        self, query: str, products: List[Dict[str, Any]], limit: int
    ) -> List[Dict[str, Any]]:
        scored: List[tuple[float, int, Dict[str, Any]]] = []
        for index, product in enumerate(products):
            searchable_text = " ".join(
                [
                    str(product.get("product_name", "")),
                    str(product.get("brands", "")),
                ]
            ).strip()
            score = self._compute_similarity_score(query, searchable_text)
            scored.append((score, index, product))

        scored.sort(key=lambda item: (-item[0], item[1]))
        return [item[2] for item in scored[:limit]]

    @staticmethod
    def _compute_similarity_score(query: str, target: str) -> float:
        normalized_query = PantriesService._normalize_search_text(query)
        normalized_target = PantriesService._normalize_search_text(target)
        if not normalized_target:
            return 0.0

        query_tokens = PantriesService._tokenize_text(normalized_query)
        target_tokens = PantriesService._tokenize_text(normalized_target)

        if query_tokens:
            overlap_ratio = len(set(query_tokens) & set(target_tokens)) / len(
                set(query_tokens)
            )
        else:
            overlap_ratio = 0.0

        contains_bonus = 1.0 if normalized_query in normalized_target else 0.0
        prefix_bonus = (
            1.0
            if any(token.startswith(normalized_query) for token in target_tokens)
            else 0.0
        )

        full_ratio = SequenceMatcher(None, normalized_query, normalized_target).ratio()
        token_ratio = max(
            (
                SequenceMatcher(None, normalized_query, token).ratio()
                for token in target_tokens
            ),
            default=0.0,
        )
        distance_component = max(full_ratio, token_ratio)

        return (
            (overlap_ratio * 0.45)
            + (contains_bonus * 0.25)
            + (prefix_bonus * 0.10)
            + (distance_component * 0.20)
        )

    @staticmethod
    def _tokenize_text(value: str) -> List[str]:
        normalized = PantriesService._normalize_search_text(value)
        return [token for token in re.split(r"[^a-z0-9]+", normalized) if token]

    @staticmethod
    def _normalize_search_text(value: Any) -> str:
        raw = PantriesService._normalize_text(value).lower()
        normalized = unicodedata.normalize("NFKD", raw)
        ascii_like = normalized.encode("ascii", "ignore").decode("ascii")
        return ascii_like.strip()

    def _off_get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]],
        timeout: Tuple[float, float],
        max_retries: int = OFF_MAX_RETRIES,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        last_exception: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                request_headers = dict(OFF_DEFAULT_HEADERS)
                if headers:
                    request_headers.update(headers)
                response = requests.get(
                    url,
                    params=params,
                    timeout=timeout,
                    headers=request_headers,
                )
                if response.status_code in OFF_RETRYABLE_HTTP_STATUS:
                    raise requests.HTTPError(
                        f"OFF temporary HTTP {response.status_code}",
                        response=response,
                    )

                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict):
                    return payload
                raise requests.RequestException("Invalid OFF JSON payload")
            except requests.RequestException as exc:
                last_exception = exc
                should_retry = (
                    attempt < max_retries
                    and self._is_retryable_off_exception(exc)
                )
                if should_retry:
                    time.sleep(OFF_RETRY_BACKOFF_SECONDS * (attempt + 1))
                    continue
                break

        raise PantriesError("OpenFoodFacts unavailable", 502) from last_exception

    @staticmethod
    def _is_retryable_off_exception(exc: requests.RequestException) -> bool:
        if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
            return True

        if isinstance(exc, requests.HTTPError):
            response = getattr(exc, "response", None)
            if response is not None:
                return response.status_code in OFF_RETRYABLE_HTTP_STATUS

        return False

    @staticmethod
    def _map_off_product(product: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "openFoodFactsId": PantriesService._normalize_text(product.get("code")),
            "productName": PantriesService._extract_off_product_name(
                product,
                preferred_lang="it",
                fallback="Prodotto sconosciuto",
            ),
            "brands": PantriesService._extract_off_brands(product),
            "imageUrl": PantriesService._extract_off_image_url(product),
            "nutrients": PantriesService._extract_off_nutrients(product),
        }

    @staticmethod
    def _map_off_search_product(
        product: Dict[str, Any], preferred_lang: str = "it"
    ) -> Dict[str, Any]:
        return {
            "code": PantriesService._normalize_text(product.get("code")),
            "product_name": PantriesService._extract_off_product_name(
                product, preferred_lang=preferred_lang, fallback=""
            ),
            "brands": PantriesService._extract_off_brands(product),
        }

    @staticmethod
    def _extract_off_product_name(
        product: Dict[str, Any],
        preferred_lang: str = "it",
        fallback: str = "",
    ) -> str:
        candidates = [
            f"product_name_{preferred_lang}",
            "product_name",
            f"generic_name_{preferred_lang}",
            "generic_name",
            "abbreviated_product_name",
            "product_name_en",
        ]
        for field in candidates:
            value = PantriesService._normalize_text(product.get(field))
            if value:
                return value

        return fallback

    @staticmethod
    def _extract_off_brands(product: Dict[str, Any]) -> str:
        brands = product.get("brands")
        if isinstance(brands, list):
            cleaned = [PantriesService._normalize_text(brand) for brand in brands]
            normalized = [value for value in cleaned if value]
            if normalized:
                return ", ".join(normalized)

        direct = PantriesService._normalize_text(product.get("brands"))
        if direct:
            return direct

        tags = product.get("brands_tags")
        if isinstance(tags, list):
            cleaned_tags = [
                PantriesService._normalize_text(str(tag).split(":")[-1])
                for tag in tags
                if PantriesService._normalize_text(tag)
            ]
            if cleaned_tags:
                return ", ".join(cleaned_tags)

        return ""

    @staticmethod
    def _extract_off_image_url(product: Dict[str, Any]) -> str:
        image_fields = [
            "image_front_small_url",
            "image_front_url",
            "image_url",
        ]
        for field in image_fields:
            value = PantriesService._normalize_text(product.get(field))
            if value:
                return value
        return ""

    @staticmethod
    def _extract_off_nutrients(product: Dict[str, Any]) -> Dict[str, float]:
        nutriments = product.get("nutriments")
        if not isinstance(nutriments, dict):
            return {}

        def _read_float(*keys: str) -> Optional[float]:
            for key in keys:
                value = nutriments.get(key)
                try:
                    parsed = float(value)
                    if parsed >= 0:
                        return parsed
                except (TypeError, ValueError):
                    continue
            return None

        nutrients: Dict[str, float] = {}
        kcal = _read_float("energy-kcal_100g", "energy-kcal")
        carbs = _read_float("carbohydrates_100g", "carbohydrates")
        protein = _read_float("proteins_100g", "proteins")
        fat = _read_float("fat_100g", "fat")

        if kcal is not None:
            nutrients["kcal"] = kcal
        if carbs is not None:
            nutrients["carbs"] = carbs
        if protein is not None:
            nutrients["protein"] = protein
        if fat is not None:
            nutrients["fat"] = fat
        return nutrients

    def _get_cached_product_entry(self, open_food_facts_id: str) -> Dict[str, Any]:
        normalized_code = self._normalize_text(open_food_facts_id)
        if not normalized_code:
            return {}

        self._cleanup_expired_search_cache()
        entry = self._search_cache.get(normalized_code)
        if not isinstance(entry, dict):
            return {}

        if float(entry.get("expires_at", 0)) <= time.time():
            self._search_cache.pop(normalized_code, None)
            return {}

        self._search_cache.move_to_end(normalized_code)
        payload = {
            "code": normalized_code,
            "product_name": self._normalize_text(entry.get("product_name")),
            "brands": self._normalize_text(entry.get("brands")),
        }
        nutrients = entry.get("nutrients")
        if isinstance(nutrients, dict) and nutrients:
            payload["nutrients"] = dict(nutrients)
        return payload

    @staticmethod
    def _normalize_barcode(barcode: Any) -> str:
        if not isinstance(barcode, str):
            raise PantriesError("Barcode obbligatorio", 400)

        normalized = re.sub(r"[\s-]+", "", barcode.strip())
        if not normalized:
            raise PantriesError("Barcode obbligatorio", 400)
        if not normalized.isdigit():
            raise PantriesError("Barcode non valido", 400)
        if len(normalized) < 6 or len(normalized) > 20:
            raise PantriesError("Barcode non valido", 400)
        return normalized

    @staticmethod
    def _normalize_text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _validate_uid(uid: Any) -> str:
        if not isinstance(uid, str) or not uid.strip():
            raise PantriesError("uid obbligatorio", status_code=400)
        return uid.strip()

    @staticmethod
    def _validate_open_food_facts_id(open_food_facts_id: Any) -> str:
        if not isinstance(open_food_facts_id, str) or not open_food_facts_id.strip():
            raise PantriesError("openFoodFactsId obbligatorio", status_code=400)
        return open_food_facts_id.strip()

    @staticmethod
    def _validate_positive_int(value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise PantriesError(
                f"{field_name} deve essere un intero >= 1", status_code=400
            )

        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise PantriesError(
                f"{field_name} deve essere un intero >= 1", status_code=400
            ) from exc

        if parsed < 1:
            raise PantriesError(
                f"{field_name} deve essere un intero >= 1", status_code=400
            )

        return parsed

    @staticmethod
    def _validate_non_negative_int(value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise PantriesError(
                f"{field_name} deve essere un intero >= 0", status_code=400
            )

        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise PantriesError(
                f"{field_name} deve essere un intero >= 0", status_code=400
            ) from exc

        if parsed < 0:
            raise PantriesError(
                f"{field_name} deve essere un intero >= 0", status_code=400
            )

        return parsed

    @staticmethod
    def _validate_product_name(product_name: Any) -> str:
        if product_name is None:
            return ""
        if not isinstance(product_name, str):
            raise PantriesError("productName deve essere una stringa", status_code=400)
        return product_name.strip()

    @staticmethod
    def _validate_nutrients(nutrients: Any) -> Dict[str, float]:
        if nutrients is None:
            return {}
        if not isinstance(nutrients, dict):
            raise PantriesError("nutrients deve essere un oggetto", status_code=400)

        aliases = {
            "kcal": "kcal",
            "carbs": "carbs",
            "protein": "protein",
            "prot": "protein",
            "fat": "fat",
        }

        parsed: Dict[str, float] = {}
        for raw_key, raw_value in nutrients.items():
            key = aliases.get(str(raw_key).strip().lower())
            if key is None:
                continue

            try:
                value = float(raw_value)
            except (TypeError, ValueError) as exc:
                raise PantriesError(
                    f"nutrients.{key} deve essere un numero >= 0",
                    status_code=400,
                ) from exc

            if value < 0:
                raise PantriesError(
                    f"nutrients.{key} deve essere un numero >= 0",
                    status_code=400,
                )
            parsed[key] = value

        return parsed

    @staticmethod
    def _validate_search_query(query: Any) -> str:
        if not isinstance(query, str):
            raise PantriesError("q obbligatorio (minimo 2 caratteri)", status_code=400)
        normalized = query.strip()
        if len(normalized) < MIN_SEARCH_QUERY_LENGTH:
            raise PantriesError("q obbligatorio (minimo 2 caratteri)", status_code=400)
        return normalized

    @staticmethod
    def _validate_search_limit(limit: Any) -> int:
        if limit is None:
            return DEFAULT_SEARCH_LIMIT
        if isinstance(limit, bool):
            raise PantriesError("limit deve essere un intero tra 1 e 50", status_code=400)
        try:
            parsed = int(limit)
        except (TypeError, ValueError) as exc:
            raise PantriesError(
                "limit deve essere un intero tra 1 e 50", status_code=400
            ) from exc
        if parsed < 1 or parsed > MAX_SEARCH_LIMIT:
            raise PantriesError("limit deve essere un intero tra 1 e 50", status_code=400)
        return parsed

    @staticmethod
    def _validate_search_lang(lang: Any) -> str:
        if lang is None:
            return "it"
        if not isinstance(lang, str):
            raise PantriesError("lang deve essere una stringa valida", status_code=400)
        normalized = lang.strip().lower()
        if not normalized or not re.fullmatch(r"[a-z]{2,8}", normalized):
            raise PantriesError("lang deve essere una stringa valida", status_code=400)
        return normalized

    @staticmethod
    def _normalize_stored_product_name(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _parse_stored_quantity(value: Any, open_food_facts_id: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise PantriesError(
                f"Quantita non valida su Firestore per item {open_food_facts_id}",
                status_code=500,
            ) from exc

        if parsed < 1:
            raise PantriesError(
                f"Quantita non valida su Firestore per item {open_food_facts_id}",
                status_code=500,
            )

        return parsed
