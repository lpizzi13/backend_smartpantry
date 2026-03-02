import copy
import unittest
from typing import Any, Dict, Tuple
from unittest.mock import patch

from flask import Flask
import requests

import pantries_service
from pantries_routes import create_pantries_blueprint


class FakeDocumentSnapshot:
    def __init__(self, doc_id: str, data: Dict[str, Any] | None):
        self.id = doc_id
        self._data = copy.deepcopy(data) if data is not None else None

    @property
    def exists(self) -> bool:
        return self._data is not None

    def to_dict(self) -> Dict[str, Any] | None:
        if self._data is None:
            return None
        return copy.deepcopy(self._data)

    def get(self, key: str, default: Any = None) -> Any:
        if self._data is None:
            return default
        return self._data.get(key, default)


class FakeCollectionReference:
    def __init__(self, db: "FakeFirestoreClient", path: Tuple[str, ...]):
        self._db = db
        self._path = path

    def document(self, doc_id: str) -> "FakeDocumentReference":
        return FakeDocumentReference(self._db, self._path + (doc_id,))

    def stream(self):
        docs = []
        expected_len = len(self._path) + 1
        for path, data in self._db._documents.items():
            if len(path) == expected_len and path[:-1] == self._path:
                docs.append(FakeDocumentSnapshot(path[-1], data))
        return docs


class FakeCollectionGroupQuery:
    def __init__(self, db: "FakeFirestoreClient", collection_name: str):
        self._db = db
        self._collection_name = collection_name
        self._limit: int | None = None

    def limit(self, value: int) -> "FakeCollectionGroupQuery":
        self._limit = value
        return self

    def stream(self):
        docs = []
        for path, data in self._db._documents.items():
            if len(path) >= 2 and path[-2] == self._collection_name:
                docs.append(FakeDocumentSnapshot(path[-1], data))

        if self._limit is not None:
            return docs[: self._limit]
        return docs


class FakeDocumentReference:
    def __init__(self, db: "FakeFirestoreClient", path: Tuple[str, ...]):
        self._db = db
        self._path = path
        self.id = path[-1]

    def collection(self, name: str) -> FakeCollectionReference:
        return FakeCollectionReference(self._db, self._path + (name,))

    def get(self, transaction: Any = None) -> FakeDocumentSnapshot:
        return FakeDocumentSnapshot(self.id, self._db._documents.get(self._path))

    def set(self, data: Dict[str, Any], merge: bool = False) -> None:
        resolved = self._db._resolve_value(copy.deepcopy(data))
        if merge and self._path in self._db._documents:
            merged = copy.deepcopy(self._db._documents[self._path])
            merged.update(resolved)
            self._db._documents[self._path] = merged
        else:
            self._db._documents[self._path] = resolved

    def update(self, data: Dict[str, Any]) -> None:
        if self._path not in self._db._documents:
            raise KeyError("Document does not exist")
        resolved = self._db._resolve_value(copy.deepcopy(data))
        current = copy.deepcopy(self._db._documents[self._path])
        current.update(resolved)
        self._db._documents[self._path] = current

    def delete(self) -> None:
        self._db._documents.pop(self._path, None)


class FakeTransaction:
    def set(
        self, doc_ref: FakeDocumentReference, data: Dict[str, Any], merge: bool = False
    ) -> None:
        doc_ref.set(data, merge=merge)

    def update(self, doc_ref: FakeDocumentReference, data: Dict[str, Any]) -> None:
        doc_ref.update(data)

    def delete(self, doc_ref: FakeDocumentReference) -> None:
        doc_ref.delete()

    def commit(self) -> None:
        return

    def rollback(self) -> None:
        return


class FakeFirestoreClient:
    def __init__(self):
        self._documents: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        self._timestamp_counter = 0

    def collection(self, name: str) -> FakeCollectionReference:
        return FakeCollectionReference(self, (name,))

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    def collection_group(self, name: str) -> FakeCollectionGroupQuery:
        return FakeCollectionGroupQuery(self, name)

    def get_document(self, *path: str) -> Dict[str, Any] | None:
        data = self._documents.get(tuple(path))
        return copy.deepcopy(data) if data is not None else None

    def _resolve_value(self, value: Any) -> Any:
        if value is pantries_service.SERVER_TIMESTAMP:
            self._timestamp_counter += 1
            return self._timestamp_counter
        if isinstance(value, dict):
            return {k: self._resolve_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_value(v) for v in value]
        return value


class FakeOffResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> Dict[str, Any]:
        return self._payload


class PantriesRoutesTests(unittest.TestCase):
    def setUp(self):
        self.db = FakeFirestoreClient()
        app = Flask(__name__)
        app.register_blueprint(create_pantries_blueprint(self.db))
        self.client = app.test_client()

    def _add(
        self,
        uid: str,
        open_food_facts_id: str,
        quantity_delta: int,
        product_name: str | None = None,
    ):
        payload = {
            "uid": uid,
            "openFoodFactsId": open_food_facts_id,
            "quantityDelta": quantity_delta,
        }
        if product_name is not None:
            payload["productName"] = product_name
        return self.client.post("/pantry/add", json=payload)

    @staticmethod
    def _off_products_payload() -> Dict[str, Any]:
        return {
            "products": [
                {
                    "code": "111",
                    "product_name": "Pasta Integrale",
                    "brands": "De Cecco",
                },
                {
                    "code": "222",
                    "product_name": "Riso Arborio",
                    "brands": "Scotti",
                },
                {
                    "code": "333",
                    "product_name": "Pastina",
                    "brands": "Buitoni",
                },
            ]
        }

    def test_add_and_increment_item(self):
        response = self._add("uid-1", "off-001", 2, "Pasta")
        self.assertEqual(response.status_code, 201)
        self.assertEqual(
            response.get_json(),
            {
                "status": "success",
                "item": {
                    "openFoodFactsId": "off-001",
                    "productName": "Pasta",
                    "quantity": 2,
                },
            },
        )

        item_doc = self.db.get_document("users", "uid-1", "pantry", "off-001")
        self.assertIsNotNone(item_doc)
        self.assertEqual(item_doc["openFoodFactsId"], "off-001")
        self.assertEqual(item_doc["productName"], "Pasta")
        self.assertEqual(item_doc["quantity"], 2)
        self.assertIn("lastUpdated", item_doc)

        increment = self._add("uid-1", "off-001", 3)
        self.assertEqual(increment.status_code, 200)
        self.assertEqual(
            increment.get_json(),
            {
                "status": "success",
                "item": {
                    "openFoodFactsId": "off-001",
                    "productName": "Pasta",
                    "quantity": 5,
                },
            },
        )

    def test_decrement_and_delete(self):
        self._add("uid-2", "off-009", 2, "Latte")

        first_decrement = self.client.patch(
            "/pantry/decrement",
            json={"uid": "uid-2", "openFoodFactsId": "off-009"},
        )
        self.assertEqual(first_decrement.status_code, 200)
        self.assertEqual(
            first_decrement.get_json(),
            {
                "status": "success",
                "item": {
                    "openFoodFactsId": "off-009",
                    "productName": "Latte",
                    "quantity": 1,
                    "deleted": False,
                },
            },
        )

        second_decrement = self.client.patch(
            "/pantry/decrement",
            json={"uid": "uid-2", "openFoodFactsId": "off-009"},
        )
        self.assertEqual(second_decrement.status_code, 200)
        self.assertEqual(
            second_decrement.get_json(),
            {
                "status": "success",
                "item": {
                    "openFoodFactsId": "off-009",
                    "productName": "Latte",
                    "quantity": 0,
                    "deleted": True,
                },
            },
        )

        self.assertIsNone(self.db.get_document("users", "uid-2", "pantry", "off-009"))

    def test_delete_item(self):
        self._add("uid-3", "manual_item", 1, "Elemento manuale")

        response = self.client.delete(
            "/pantry/item",
            json={"uid": "uid-3", "openFoodFactsId": "manual_item"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {"status": "success", "deleted": "manual_item"},
        )
        self.assertIsNone(
            self.db.get_document("users", "uid-3", "pantry", "manual_item")
        )

    def test_list_items_sorted_by_product_name(self):
        self._add("uid-4", "id-z", 1, "Zucchero")
        self._add("uid-4", "id-a", 1, "Acqua")
        self._add("uid-4", "id-b", 1, "Banana")

        response = self.client.get("/pantry/uid-4")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "status": "success",
                "items": [
                    {
                        "openFoodFactsId": "id-a",
                        "productName": "Acqua",
                        "quantity": 1,
                    },
                    {
                        "openFoodFactsId": "id-b",
                        "productName": "Banana",
                        "quantity": 1,
                    },
                    {
                        "openFoodFactsId": "id-z",
                        "productName": "Zucchero",
                        "quantity": 1,
                    },
                ],
            },
        )

    def test_validation_errors(self):
        missing_uid = self.client.post(
            "/pantry/add",
            json={"openFoodFactsId": "off-001", "quantityDelta": 1},
        )
        self.assertEqual(missing_uid.status_code, 400)
        self.assertEqual(
            missing_uid.get_json(),
            {"status": "error", "error": "uid obbligatorio"},
        )

        invalid_quantity = self.client.post(
            "/pantry/add",
            json={"uid": "uid-5", "openFoodFactsId": "off-001", "quantityDelta": 0},
        )
        self.assertEqual(invalid_quantity.status_code, 400)
        self.assertEqual(
            invalid_quantity.get_json(),
            {
                "status": "error",
                "error": "quantityDelta deve essere un intero >= 1",
            },
        )

    def test_search_validation_errors(self):
        missing_query = self.client.get("/pantry/search")
        self.assertEqual(missing_query.status_code, 400)
        self.assertEqual(
            missing_query.get_json(),
            {"status": "error", "error": "q obbligatorio (minimo 2 caratteri)"},
        )

        short_query = self.client.get("/pantry/search?q=a")
        self.assertEqual(short_query.status_code, 400)
        self.assertEqual(
            short_query.get_json(),
            {"status": "error", "error": "q obbligatorio (minimo 2 caratteri)"},
        )

        invalid_limit = self.client.get("/pantry/search?q=pasta&limit=100")
        self.assertEqual(invalid_limit.status_code, 400)
        self.assertEqual(
            invalid_limit.get_json(),
            {"status": "error", "error": "limit deve essere un intero tra 1 e 50"},
        )

        invalid_similar = self.client.get("/pantry/search?q=pasta&similar=forse")
        self.assertEqual(invalid_similar.status_code, 400)
        self.assertEqual(
            invalid_similar.get_json(),
            {"status": "error", "error": "similar deve essere true o false"},
        )

    @patch("pantries_service.requests.get")
    def test_search_without_similar_returns_products_and_empty_recommended(
        self, mock_get
    ):
        mock_get.return_value = FakeOffResponse(self._off_products_payload())
        response = self.client.get("/pantry/search?q=pasta&lang=it")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()

        self.assertEqual(payload["query"], "pasta")
        self.assertEqual(payload["recommended"], [])
        self.assertEqual(
            payload["products"],
            [
                {"code": "111", "product_name": "Pasta Integrale", "brands": "De Cecco"},
                {"code": "222", "product_name": "Riso Arborio", "brands": "Scotti"},
                {"code": "333", "product_name": "Pastina", "brands": "Buitoni"},
            ],
        )
        self.assertEqual(payload["results"], payload["products"])

    @patch("pantries_service.requests.get")
    def test_search_with_similar_returns_ranked_recommended(self, mock_get):
        mock_get.return_value = FakeOffResponse(self._off_products_payload())
        response = self.client.get("/pantry/search?q=pasta&similar=true&limit=2")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()

        self.assertEqual(payload["query"], "pasta")
        self.assertEqual(len(payload["recommended"]), 2)
        self.assertEqual(payload["recommended"][0]["code"], "111")
        self.assertEqual(payload["recommended"][1]["code"], "333")

    @patch("pantries_service.requests.get")
    def test_search_upstream_error_returns_502(self, mock_get):
        mock_get.side_effect = requests.Timeout("timeout")
        response = self.client.get("/pantry/search?q=pasta")
        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.get_json(),
            {"status": "error", "error": "OpenFoodFacts unavailable"},
        )

    @patch("pantries_service.requests.get")
    def test_search_upstream_error_uses_firestore_fallback(self, mock_get):
        self._add("uid-9", "off-latte-1", 1, "Latte Intero")
        self._add("uid-9", "off-caffe-1", 1, "Caffe Macinato")
        mock_get.side_effect = requests.Timeout("timeout")

        response = self.client.get("/pantry/search?q=caffè&similar=true&limit=5&lang=it")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()

        self.assertEqual(payload["query"], "caffè")
        self.assertTrue(len(payload["products"]) >= 1)
        self.assertEqual(payload["products"][0]["code"], "off-caffe-1")
        self.assertEqual(payload["products"][0]["product_name"], "Caffe Macinato")
        self.assertEqual(payload["recommended"][0]["code"], "off-caffe-1")

    @patch("pantries_service.requests.get")
    def test_barcode_response_is_backward_compatible(self, mock_get):
        mock_get.return_value = FakeOffResponse(
            {
                "status": 1,
                "product": {
                    "code": "8002470000476",
                    "product_name": "Pasta di Semola",
                    "brands": "Barilla",
                },
            }
        )
        response = self.client.get("/pantry/barcode/8002470000476")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()

        self.assertEqual(payload["status"], 1)
        self.assertEqual(payload["statusText"], "success")
        self.assertEqual(payload["product"]["code"], "8002470000476")
        self.assertEqual(payload["product"]["product_name"], "Pasta di Semola")
        self.assertEqual(payload["product"]["openFoodFactsId"], "8002470000476")
        self.assertEqual(payload["product"]["productName"], "Pasta di Semola")


if __name__ == "__main__":
    unittest.main()
