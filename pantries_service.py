from collections import OrderedDict
import math
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
OFF_SEARCH_SORT_BY = "-completeness"
OFF_SEARCH_QUALITY_CONSTRAINTS: Tuple[str, ...] = (
    'states_tags:"en:nutrition-facts-completed"',
    "nutriments.energy-kcal_100g:[1 TO *]",
    "nutriments.carbohydrates_100g:[0.1 TO *]",
    "nutriments.proteins_100g:[0.1 TO *]",
    "nutriments.fat_100g:[0.1 TO *]",
)
# Producer/source indicators used to infer OFF data trustworthiness.
OFF_PRODUCER_SOURCE_KEYWORDS: Tuple[str, ...] = (
    "producer",
    "producers",
    "producer-",
    "manufacturer",
    "manufacturers",
    "brand-owner",
    "brand_owner",
)
OFF_REVIEWED_STATES = {"en:checked", "en:complete"}
OFF_CERTIFIED_MIN_COMPLETENESS = 0.9
OFF_SEARCH_RESPONSE_FIELDS: Tuple[str, ...] = (
    "code",
    "product_name",
    # Language fallback fields for display quality.
    "generic_name",
    "brands",
    "brands_tags",
    # Producer/source metadata used for "certified" signal.
    "owner",
    "brand_owner",
    "brand_owner_imported",
    "owners_tags",
    "data_sources_tags",
    # Package weight metadata for frontend display.
    "quantity",
    "product_quantity",
    "product_quantity_unit",
    # Lightweight macro fields (avoid full nutriments payload in search).
    "nutriments",
    "energy-kcal_100g",
    "carbohydrates_100g",
    "proteins_100g",
    "fat_100g",
    # OFF quality metadata used for ranking.
    "states_tags",
    "completeness",
)
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

        # Search responses are intentionally sourced only from OpenFoodFacts.
        query_tokens = self._tokenize_text(validated_query)
        products = []
        for p in off_products:
            searchable_text = self._normalize_search_text(
                f"{p.get('product_name', '')} {p.get('brands', '')} {p.get('code', '')}"
            )
            if all(token in searchable_text for token in query_tokens):
                products.append(p)

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
        filtered_query = self._build_off_quality_search_query(query)
        langs = self._build_off_search_langs(lang)
        legacy_langs = self._build_off_legacy_search_langs(lang)
        search_fields = ",".join(
            [
                *OFF_SEARCH_RESPONSE_FIELDS,
                f"product_name_{lang}",
                f"generic_name_{lang}",
                "product_name_en",
                "generic_name_en",
            ]
        )
        search_backends = [
            {
                "url": OFF_SEARCH_URL,
                "params": {
                    "q": filtered_query,
                    "page_size": page_size,
                    "fields": search_fields,
                    "langs": langs,
                    "sort_by": OFF_SEARCH_SORT_BY,
                },
                "timeout": OFF_SEARCH_TIMEOUT,
            }
        ]
        for legacy_lang in legacy_langs:
            search_backends.append(
                {
                    "url": OFF_LEGACY_SEARCH_URL,
                    "params": {
                        "search_terms": query,
                        "search_simple": 1,
                        "action": "process",
                        "json": 1,
                        "page_size": page_size,
                        "lc": legacy_lang,
                        "fields": search_fields,
                    },
                    "timeout": OFF_LEGACY_SEARCH_TIMEOUT,
                }
            )

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
            normalized_products = self._sort_search_products_for_certification(
                normalized_products
            )
            self._cache_off_search_products(products, preferred_lang=lang)
            return normalized_products

        if last_exception is not None:
            raise last_exception
        return []

    @staticmethod
    def _build_off_quality_search_query(query: str) -> str:
        normalized_query = PantriesService._normalize_text(query)
        if not normalized_query:
            return ""
        return " AND ".join(
            [normalized_query, *OFF_SEARCH_QUALITY_CONSTRAINTS]
        )

    @staticmethod
    def _build_off_search_langs(lang: str) -> str:
        preferred = PantriesService._normalize_text(lang).lower() or "en"
        ordered_langs = [preferred]
        if preferred != "en":
            ordered_langs.append("en")
        return ",".join(ordered_langs)

    @staticmethod
    def _build_off_legacy_search_langs(lang: str) -> List[str]:
        preferred = PantriesService._normalize_text(lang).lower() or "en"
        ordered_langs = [preferred]
        if preferred != "en":
            ordered_langs.append("en")
        return ordered_langs

    @staticmethod
    def _sort_search_products_for_certification(
        products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        indexed_products = list(enumerate(products))

        def _sort_key(item: Tuple[int, Dict[str, Any]]) -> Tuple[int, float, float, int]:
            original_index, product = item
            certified = 1 if bool(product.get("certified")) else 0
            certification = (
                product.get("certification")
                if isinstance(product.get("certification"), dict)
                else {}
            )
            certification_score = PantriesService._parse_non_negative_float(
                certification.get("score")
            )
            completeness = PantriesService._parse_non_negative_float(
                product.get("completeness")
            )
            score = certification_score if certification_score is not None else 0.0
            completeness_score = completeness if completeness is not None else 0.0
            return (-certified, -score, -completeness_score, original_index)

        indexed_products.sort(key=_sort_key)
        return [product for _, product in indexed_products]

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
            package_weight_grams=mapped_product.get("packageWeightGrams"),
        )
        return mapped_product

    def set_item_quantity(
        self,
        uid: Any,
        open_food_facts_id: Any,
        quantity: Any,
        product_name: Any = None,
        nutrients: Any = None,
        package_weight_grams: Any = None,
        allow_zero: bool = True,
    ) -> Dict[str, Any]:
        validated_quantity = self._validate_non_negative_int(quantity, "quantity")
        if not allow_zero and validated_quantity == 0:
            raise PantriesError("quantity deve essere un intero >= 1", status_code=400)
        validated_package_weight_grams = self._validate_required_package_weight_grams(
            package_weight_grams
        )
        if validated_package_weight_grams <= 0:
            raise PantriesError(
                "packageWeightGrams deve essere un numero > 0", status_code=400
            )
        grams_delta = round(validated_quantity * validated_package_weight_grams, 3)
        if not allow_zero and grams_delta <= 0:
            raise PantriesError(
                "quantity e packageWeightGrams devono produrre grams > 0",
                status_code=400,
            )
        return self._mutate_item_grams(
            uid=uid,
            open_food_facts_id=open_food_facts_id,
            product_name=product_name,
            nutrients=nutrients,
            grams=grams_delta,
            mode="increment",
            require_existing=False,
            allow_zero=allow_zero,
        )

    def set_item_grams(
        self,
        uid: Any,
        open_food_facts_id: Any,
        grams: Any,
        product_name: Any = None,
        nutrients: Any = None,
        require_existing: bool = True,
        allow_zero: bool = True,
    ) -> Dict[str, Any]:
        validated_grams = self._validate_grams(grams)
        if not allow_zero and validated_grams == 0:
            raise PantriesError("grams deve essere un numero > 0", status_code=400)
        return self._mutate_item_grams(
            uid=uid,
            open_food_facts_id=open_food_facts_id,
            product_name=product_name,
            nutrients=nutrients,
            grams=validated_grams,
            mode="set",
            require_existing=require_existing,
            allow_zero=allow_zero,
        )

    def _mutate_item_grams(
        self,
        uid: Any,
        open_food_facts_id: Any,
        product_name: Any,
        nutrients: Any,
        grams: float,
        mode: str,
        require_existing: bool,
        allow_zero: bool,
    ) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_product_name = self._validate_product_name(product_name)
        validated_id, item_source = self._resolve_item_identity(
            open_food_facts_id=open_food_facts_id,
            product_name=validated_product_name,
        )
        validated_nutrients = self._validate_nutrients(nutrients)
        should_update_product_name = bool(validated_product_name)
        should_update_nutrients = bool(validated_nutrients)
        cached_entry = self._get_cached_product_entry(validated_id)
        cached_name = self._normalize_text(cached_entry.get("product_name"))
        cached_nutrients = self._to_complete_nutrients(
            self._validate_nutrients(cached_entry.get("nutrients"))
        )

        item_ref = self._get_pantry_collection(validated_uid).document(validated_id)

        def _tx(transaction: Any) -> Dict[str, Any]:
            snapshot = item_ref.get(transaction=transaction)
            exists = snapshot.exists
            if require_existing and not exists:
                raise PantriesError("Item non trovato in dispensa", status_code=404)
            snapshot_data = snapshot.to_dict() or {}
            current_product_name = self._normalize_stored_product_name(
                snapshot.get("productName")
            )
            current_source = self._normalize_stored_source(
                snapshot_data.get("source"),
                validated_id,
            )
            current_nutrients = self._extract_stored_nutrients(snapshot_data)
            current_grams = self._extract_stored_grams(snapshot_data, validated_id)

            final_product_name = (
                validated_product_name
                or current_product_name
                or cached_name
                or "Prodotto sconosciuto"
            )
            final_nutrients = self._to_complete_nutrients(
                validated_nutrients or current_nutrients or cached_nutrients
            )
            final_source = current_source if exists else item_source
            if mode == "increment":
                final_grams = round((current_grams if exists else 0.0) + grams, 3)
            else:
                final_grams = round(grams, 3)

            if final_grams <= 0:
                if not allow_zero:
                    raise PantriesError("grams deve essere un numero > 0", status_code=400)
                if exists:
                    transaction.delete(item_ref)
                deleted_payload = {
                    "openFoodFactsId": validated_id,
                    "productName": final_product_name,
                    "grams": 0.0,
                    "nutrients": final_nutrients,
                    "created": False,
                    "deleted": exists,
                }
                return deleted_payload

            if not exists:
                create_payload = {
                    "openFoodFactsId": validated_id,
                    "source": final_source,
                    "productName": final_product_name,
                    "grams": final_grams,
                }
                create_payload.update(self._build_nutrients_storage(final_nutrients))
                transaction.set(item_ref, create_payload)
                created_payload = {
                    "openFoodFactsId": validated_id,
                    "productName": final_product_name,
                    "grams": final_grams,
                    "nutrients": final_nutrients,
                    "created": True,
                    "deleted": False,
                }
                return created_payload

            update_payload = {
                "grams": final_grams,
                "source": final_source,
            }
            if should_update_product_name:
                update_payload["productName"] = validated_product_name
                final_product_name = validated_product_name
            if should_update_nutrients:
                final_nutrients = self._to_complete_nutrients(validated_nutrients)
            update_payload.update(self._build_nutrients_storage(final_nutrients))
            update_payload.update(self._build_legacy_nutrients_cleanup())
            transaction.update(item_ref, update_payload)
            updated_payload = {
                "openFoodFactsId": validated_id,
                "productName": final_product_name,
                "grams": final_grams,
                "nutrients": final_nutrients,
                "created": False,
                "deleted": False,
            }
            return updated_payload

        result = self._run_transaction(_tx)
        if result.get("grams", 0) > 0:
            self._upsert_search_cache_entry(
                open_food_facts_id=result["openFoodFactsId"],
                product_name=result["productName"],
                nutrients=result.get("nutrients") or validated_nutrients or cached_nutrients,
            )
        return result

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

            grams = self._extract_stored_grams(data, str(open_food_facts_id))
            product_name = self._normalize_stored_product_name(data.get("productName"))
            item_payload = {
                "openFoodFactsId": str(open_food_facts_id),
                "productName": product_name,
                "grams": grams,
            }
            nutrients = self._extract_stored_nutrients(data)
            if self._has_non_zero_nutrients(nutrients):
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
                cached_package_weight = self._validate_package_weight_grams(
                    data.get("package_weight_grams")
                )
                if cached_package_weight is not None:
                    by_code[code]["packageWeightGrams"] = cached_package_weight

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
                    package_weight = self._extract_stored_package_weight_grams(data)
                    if code not in by_code or (
                        product_name and not by_code[code].get("product_name")
                    ):
                        by_code[code] = {
                            "code": code,
                            "product_name": product_name,
                            "brands": "",
                        }
                    if (
                        package_weight is not None
                        and by_code[code].get("packageWeightGrams") is None
                    ):
                        by_code[code]["packageWeightGrams"] = package_weight
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
                package_weight_grams=self._extract_off_package_weight_grams(product),
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
        package_weight_grams: Optional[float] = None,
    ) -> None:
        normalized_code = self._normalize_text(open_food_facts_id)
        if not normalized_code:
            return

        try:
            normalized_nutrients = self._validate_nutrients(nutrients)
        except PantriesError:
            normalized_nutrients = {}
        validated_package_weight_grams = self._validate_package_weight_grams(
            package_weight_grams
        )

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
        if validated_package_weight_grams is not None:
            payload["package_weight_grams"] = validated_package_weight_grams

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
    def _extract_off_tags(value: Any) -> List[str]:
        if isinstance(value, list):
            normalized = [
                PantriesService._normalize_text(item)
                for item in value
                if PantriesService._normalize_text(item)
            ]
            if normalized:
                return normalized

        single_value = PantriesService._normalize_text(value)
        if not single_value:
            return []
        if "," in single_value:
            return [
                token.strip()
                for token in single_value.split(",")
                if PantriesService._normalize_text(token)
            ]
        return [single_value]

    @staticmethod
    def _extract_off_owner(product: Dict[str, Any]) -> str:
        direct_owner_fields = (
            "owner",
            "brand_owner",
            "brand_owner_imported",
        )
        for field in direct_owner_fields:
            direct_owner = PantriesService._normalize_text(product.get(field))
            if direct_owner:
                return direct_owner

        owner_tags = PantriesService._extract_off_owners_tags(product)
        if owner_tags:
            return owner_tags[0]
        return ""

    @staticmethod
    def _extract_off_owners_tags(product: Dict[str, Any]) -> List[str]:
        return PantriesService._extract_off_tags(product.get("owners_tags"))

    @staticmethod
    def _extract_off_data_sources(product: Dict[str, Any]) -> str:
        return PantriesService._normalize_text(product.get("data_sources"))

    @staticmethod
    def _extract_off_data_sources_tags(product: Dict[str, Any]) -> List[str]:
        return PantriesService._extract_off_tags(product.get("data_sources_tags"))

    @staticmethod
    def _build_off_certification_payload(
        brands: str,
        brands_tags: List[str],
        owner: str,
        owners_tags: List[str],
        data_sources_tags: List[str],
        states_tags: List[str],
        completeness: Optional[float],
        via_barcode: bool,
    ) -> Dict[str, Any]:
        normalized_brands = PantriesService._normalize_search_text(
            " ".join([brands, *brands_tags])
        )
        normalized_owner = PantriesService._normalize_search_text(
            " ".join([owner, *owners_tags])
        )
        normalized_sources = [
            PantriesService._normalize_search_text(tag)
            for tag in data_sources_tags
        ]
        normalized_states = [
            PantriesService._normalize_search_text(tag) for tag in states_tags
        ]
        # Legacy key kept for backward compatibility with existing frontend checks.
        # It now represents "brand metadata available", not a brand-name whitelist match.
        brand_matched = bool(normalized_brands)
        # Any explicit owner/owner-tag counts as producer identity signal.
        owner_matched = bool(normalized_owner)
        producer_source_matched = any(
            any(keyword in source_tag for keyword in OFF_PRODUCER_SOURCE_KEYWORDS)
            for source_tag in normalized_sources
        )
        state_reviewed_matched = any(
            state_tag in OFF_REVIEWED_STATES for state_tag in normalized_states
        )
        completeness_matched = (
            completeness is not None
            and completeness >= OFF_CERTIFIED_MIN_COMPLETENESS
        )
        producer_identity_matched = owner_matched or producer_source_matched
        quality_matched = (
            state_reviewed_matched
            or completeness_matched
            or (not normalized_states and completeness is None)
        )
        is_certified = bool(
            producer_identity_matched and (quality_matched or via_barcode)
        )
        score = 0
        if brand_matched:
            score += 5
        if owner_matched:
            score += 35
        if producer_source_matched:
            score += 35
        if state_reviewed_matched:
            score += 10
        if completeness_matched:
            score += 10
        if via_barcode:
            score += 10
        certification_score = float(min(score, 100))

        reasons: List[str] = []
        if brand_matched:
            reasons.append("brand_match")
        if owner_matched:
            reasons.append("owner_match")
        if producer_source_matched:
            reasons.append("producer_source_match")
        if state_reviewed_matched:
            reasons.append("state_reviewed_match")
        if completeness_matched:
            reasons.append("completeness_match")
        if via_barcode:
            reasons.append("barcode_lookup")

        return {
            "isCertified": is_certified,
            "likelyOriginal": is_certified,
            "brandMatched": brand_matched,
            "ownerMatched": owner_matched,
            "producerSourceMatched": producer_source_matched,
            "stateReviewedMatched": state_reviewed_matched,
            "completenessMatched": completeness_matched,
            "score": certification_score,
            "viaBarcodeLookup": bool(via_barcode),
            "confidence": "high" if is_certified else "none",
            "reasons": reasons,
        }

    @staticmethod
    def _apply_off_certification_metadata(
        mapped_product: Dict[str, Any],
        raw_product: Dict[str, Any],
        via_barcode: bool = False,
    ) -> None:
        brands_tags = PantriesService._extract_off_tags(raw_product.get("brands_tags"))
        owner = PantriesService._extract_off_owner(raw_product)
        owners_tags = PantriesService._extract_off_owners_tags(raw_product)
        data_sources = PantriesService._extract_off_data_sources(raw_product)
        data_sources_tags = PantriesService._extract_off_data_sources_tags(raw_product)

        if brands_tags:
            mapped_product["brands_tags"] = brands_tags
        if owner:
            mapped_product["owner"] = owner
        if owners_tags:
            mapped_product["owners_tags"] = owners_tags
        if data_sources:
            mapped_product["data_sources"] = data_sources
        if data_sources_tags:
            mapped_product["data_sources_tags"] = data_sources_tags
        states_tags = PantriesService._extract_off_tags(raw_product.get("states_tags"))
        completeness = PantriesService._parse_non_negative_float(
            raw_product.get("completeness")
        )

        certification = PantriesService._build_off_certification_payload(
            brands=PantriesService._normalize_text(mapped_product.get("brands")),
            brands_tags=brands_tags,
            owner=owner,
            owners_tags=owners_tags,
            data_sources_tags=data_sources_tags,
            states_tags=states_tags,
            completeness=completeness,
            via_barcode=via_barcode,
        )
        mapped_product["certification"] = certification
        mapped_product["certified"] = certification["isCertified"]
        mapped_product["likelyOriginal"] = certification["likelyOriginal"]

    @staticmethod
    def _map_off_product(product: Dict[str, Any]) -> Dict[str, Any]:
        mapped = {
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
        package_weight_grams = PantriesService._extract_off_package_weight_grams(product)
        if package_weight_grams is not None:
            mapped["packageWeightGrams"] = package_weight_grams
        PantriesService._apply_off_certification_metadata(
            mapped_product=mapped,
            raw_product=product,
            via_barcode=True,
        )
        return mapped

    @staticmethod
    def _map_off_search_product(
        product: Dict[str, Any], preferred_lang: str = "it"
    ) -> Dict[str, Any]:
        nutrients = PantriesService._extract_off_nutrients(product)
        mapped = {
            "code": PantriesService._normalize_text(product.get("code")),
            "product_name": PantriesService._extract_off_product_name(
                product, preferred_lang=preferred_lang, fallback=""
            ),
            "brands": PantriesService._extract_off_brands(product),
        }
        completeness = PantriesService._parse_non_negative_float(
            product.get("completeness")
        )
        if completeness is not None:
            mapped["completeness"] = completeness
        states_tags = product.get("states_tags")
        if isinstance(states_tags, list):
            normalized_states = [
                PantriesService._normalize_text(state) for state in states_tags
            ]
            mapped["states_tags"] = [state for state in normalized_states if state]
        PantriesService._apply_off_certification_metadata(
            mapped_product=mapped,
            raw_product=product,
            via_barcode=False,
        )

        package_weight_grams = PantriesService._extract_off_package_weight_grams(product)
        if package_weight_grams is not None:
            mapped["packageWeightGrams"] = package_weight_grams

        if nutrients:
            protein = nutrients.get("protein")
            kcal = nutrients.get("kcal")
            carbs = nutrients.get("carbs")
            fat = nutrients.get("fat")
            mapped["kcal"] = kcal
            mapped["carbs"] = carbs
            mapped["fat"] = fat
            mapped["prot"] = protein
            mapped["protein"] = protein
            nutriments_payload: Dict[str, Any] = {}
            if kcal is not None:
                nutriments_payload["energy-kcal_100g"] = kcal
            if carbs is not None:
                nutriments_payload["carbohydrates_100g"] = carbs
            if fat is not None:
                nutriments_payload["fat_100g"] = fat
            if protein is not None:
                nutriments_payload["proteins_100g"] = protein
            if nutriments_payload:
                mapped["nutriments"] = nutriments_payload
            mapped["nutrients"] = nutrients

        return mapped

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
        nutriments_payload = nutriments if isinstance(nutriments, dict) else {}

        def _read_float_from_dict(payload: Dict[str, Any], *keys: str) -> Optional[float]:
            for key in keys:
                value = payload.get(key)
                try:
                    parsed = float(value)
                    if parsed >= 0:
                        return parsed
                except (TypeError, ValueError):
                    continue
            return None

        def _read_float(*keys: str) -> Optional[float]:
            nested_value = _read_float_from_dict(nutriments_payload, *keys)
            if nested_value is not None:
                return nested_value
            return _read_float_from_dict(product, *keys)

        nutrients: Dict[str, float] = {}
        kcal = _read_float("energy-kcal_100g", "energy-kcal", "kcal", "energy_kcal_100g")
        carbs = _read_float("carbohydrates_100g", "carbohydrates")
        protein = _read_float("proteins_100g", "proteins", "prot", "protein")
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

    @staticmethod
    def _extract_off_package_weight_grams(product: Dict[str, Any]) -> Optional[float]:
        product_quantity = PantriesService._parse_non_negative_float(
            product.get("product_quantity")
        )
        product_quantity_unit = PantriesService._normalize_text(
            product.get("product_quantity_unit")
        ).lower()
        converted = PantriesService._convert_weight_to_grams(
            product_quantity, product_quantity_unit
        )
        if converted is not None:
            return converted

        quantity_label = PantriesService._normalize_text(product.get("quantity")).lower()
        if not quantity_label:
            return None

        multipack_match = re.search(
            r"(\d+(?:[.,]\d+)?)\s*[xX]\s*(\d+(?:[.,]\d+)?)\s*(kg|g|mg)\b",
            quantity_label,
        )
        if multipack_match:
            packs = PantriesService._parse_non_negative_float(multipack_match.group(1))
            per_pack = PantriesService._parse_non_negative_float(multipack_match.group(2))
            unit = multipack_match.group(3)
            if packs is not None:
                per_pack_grams = PantriesService._convert_weight_to_grams(per_pack, unit)
                if per_pack_grams is not None:
                    return round(packs * per_pack_grams, 3)

        single_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(kg|g|mg)\b", quantity_label)
        if single_match:
            amount = PantriesService._parse_non_negative_float(single_match.group(1))
            unit = single_match.group(2)
            return PantriesService._convert_weight_to_grams(amount, unit)

        return None

    @staticmethod
    def _convert_weight_to_grams(
        quantity: Optional[float], unit: str
    ) -> Optional[float]:
        if quantity is None:
            return None
        normalized_unit = PantriesService._normalize_text(unit).lower()
        if normalized_unit in {"g", "gram", "grams"}:
            return round(quantity, 3)
        if normalized_unit in {"kg", "kilogram", "kilograms"}:
            return round(quantity * 1000.0, 3)
        if normalized_unit in {"mg", "milligram", "milligrams"}:
            return round(quantity / 1000.0, 3)
        return None

    @staticmethod
    def _parse_non_negative_float(value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            parsed = float(str(value).strip().replace(",", "."))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed) or parsed < 0:
            return None
        return parsed

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
        package_weight_grams = self._validate_package_weight_grams(
            entry.get("package_weight_grams")
        )
        if package_weight_grams is not None:
            payload["package_weight_grams"] = package_weight_grams
        return payload

    def _resolve_item_identity(
        self, open_food_facts_id: Any, product_name: str
    ) -> Tuple[str, str]:
        normalized_id = self._normalize_text(open_food_facts_id)
        normalized_product_name = self._normalize_text(product_name)
        if normalized_id:
            source = "manual" if self._is_manual_item_id(normalized_id) else "openfoodfacts"
            return normalized_id, source

        if normalized_product_name:
            manual_id = self._build_manual_item_id(normalized_product_name)
            return manual_id, "manual"

        raise PantriesError("openFoodFactsId o productName obbligatorio", status_code=400)

    @staticmethod
    def _is_manual_item_id(open_food_facts_id: str) -> bool:
        return open_food_facts_id.lower().startswith("manual:")

    @staticmethod
    def _build_manual_item_id(product_name: str) -> str:
        normalized = PantriesService._normalize_search_text(product_name)
        slug = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
        if not slug:
            slug = "custom-food"
        if len(slug) > 64:
            slug = slug[:64].rstrip("-")
        return f"manual:{slug}"

    @staticmethod
    def _normalize_stored_source(stored_source: Any, open_food_facts_id: str) -> str:
        normalized = PantriesService._normalize_text(stored_source).lower()
        if normalized in {"openfoodfacts", "manual"}:
            return normalized
        if PantriesService._is_manual_item_id(open_food_facts_id):
            return "manual"
        return "openfoodfacts"

    @staticmethod
    def _to_complete_nutrients(nutrients: Dict[str, float]) -> Dict[str, float]:
        return {
            "kcal": float(nutrients.get("kcal", 0.0)),
            "carbs": float(nutrients.get("carbs", 0.0)),
            "protein": float(nutrients.get("protein", 0.0)),
            "fat": float(nutrients.get("fat", 0.0)),
        }

    @staticmethod
    def _build_nutrients_storage(nutrients: Dict[str, float]) -> Dict[str, Any]:
        complete = PantriesService._to_complete_nutrients(nutrients)
        return {
            "nutrients": complete,
        }

    @staticmethod
    def _build_legacy_nutrients_cleanup() -> Dict[str, Any]:
        return {
            "foodName": firestore.DELETE_FIELD,
            "kcal": firestore.DELETE_FIELD,
            "carbs": firestore.DELETE_FIELD,
            "fat": firestore.DELETE_FIELD,
            "prot": firestore.DELETE_FIELD,
            "protein": firestore.DELETE_FIELD,
            "quantity": firestore.DELETE_FIELD,
            "packageWeightGrams": firestore.DELETE_FIELD,
            "lastUpdated": firestore.DELETE_FIELD,
        }

    def _extract_stored_nutrients(self, data: Dict[str, Any]) -> Dict[str, float]:
        parsed = self._validate_nutrients(data.get("nutrients"))
        if not parsed:
            top_level_payload: Dict[str, Any] = {}
            for key in ("kcal", "carbs", "fat", "prot", "protein"):
                value = data.get(key)
                if value is not None:
                    top_level_payload[key] = value
            parsed = self._validate_nutrients(
                top_level_payload
            )
        return self._to_complete_nutrients(parsed)

    def _extract_stored_package_weight_grams(
        self, data: Dict[str, Any]
    ) -> Optional[float]:
        try:
            return self._validate_package_weight_grams(data.get("packageWeightGrams"))
        except PantriesError:
            return None

    def _extract_stored_grams(self, data: Dict[str, Any], open_food_facts_id: str) -> float:
        if "grams" in data:
            return self._parse_stored_grams(data.get("grams"), open_food_facts_id)

        legacy_quantity = data.get("quantity")
        if legacy_quantity is None:
            return 0.0
        parsed_quantity = self._parse_stored_legacy_quantity(
            legacy_quantity, open_food_facts_id
        )
        package_weight = self._extract_stored_package_weight_grams(data)
        if package_weight is not None:
            return round(parsed_quantity * package_weight, 3)
        return round(parsed_quantity, 3)

    @staticmethod
    def _has_non_zero_nutrients(nutrients: Dict[str, float]) -> bool:
        return any(float(value) > 0 for value in nutrients.values())

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
    def _validate_grams(value: Any) -> float:
        if value is None or isinstance(value, bool):
            raise PantriesError("grams deve essere un numero >= 0", status_code=400)
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise PantriesError("grams deve essere un numero >= 0", status_code=400) from exc
        if not math.isfinite(parsed) or parsed < 0:
            raise PantriesError("grams deve essere un numero >= 0", status_code=400)
        return round(parsed, 3)

    @staticmethod
    def _validate_required_package_weight_grams(value: Any) -> float:
        parsed = PantriesService._validate_package_weight_grams(value)
        if parsed is None:
            raise PantriesError(
                "packageWeightGrams obbligatorio", status_code=400
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
    def _validate_package_weight_grams(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            raise PantriesError(
                "packageWeightGrams deve essere un numero >= 0", status_code=400
            )
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise PantriesError(
                "packageWeightGrams deve essere un numero >= 0", status_code=400
            ) from exc
        if not math.isfinite(parsed) or parsed < 0:
            raise PantriesError(
                "packageWeightGrams deve essere un numero >= 0", status_code=400
            )
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
    def _parse_stored_grams(value: Any, open_food_facts_id: str) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise PantriesError(
                f"Grammi non validi su Firestore per item {open_food_facts_id}",
                status_code=500,
            ) from exc

        if not math.isfinite(parsed) or parsed < 0:
            raise PantriesError(
                f"Grammi non validi su Firestore per item {open_food_facts_id}",
                status_code=500,
            )

        return round(parsed, 3)

    @staticmethod
    def _parse_stored_legacy_quantity(value: Any, open_food_facts_id: str) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise PantriesError(
                f"Quantita legacy non valida su Firestore per item {open_food_facts_id}",
                status_code=500,
            ) from exc

        if not math.isfinite(parsed) or parsed < 0:
            raise PantriesError(
                f"Quantita legacy non valida su Firestore per item {open_food_facts_id}",
                status_code=500,
            )

        return parsed
