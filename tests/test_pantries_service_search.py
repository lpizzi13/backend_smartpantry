import unittest
from unittest.mock import patch

import requests

from pantries_service import PantriesError, PantriesService


class FakeSnapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data

    def to_dict(self):
        return dict(self._data)


class FakeCollectionGroupQuery:
    def __init__(self, docs):
        self._docs = docs
        self._limit = None

    def limit(self, value: int):
        self._limit = value
        return self

    def stream(self):
        if self._limit is None:
            return self._docs
        return self._docs[: self._limit]


class FakeDbWithCollectionGroup:
    def __init__(self, docs):
        self._docs = docs

    def collection_group(self, name: str):
        if name != "pantry":
            return FakeCollectionGroupQuery([])
        return FakeCollectionGroupQuery(self._docs)


class FakeOffResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class PantriesServiceSearchTests(unittest.TestCase):
    def setUp(self):
        self.service = PantriesService(db=None)
        self.payload = {
            "products": [
                {"code": "a1", "product_name": "Pasta Integrale", "brands": "De Cecco"},
                {"code": "b2", "product_name": "Riso Arborio", "brands": "Scotti"},
                {"code": "c3", "product_name": "Pastina", "brands": "Buitoni"},
            ]
        }

    @patch("pantries_service.requests.get")
    def test_search_products_without_similar_returns_empty_recommended(self, mock_get):
        mock_get.return_value = FakeOffResponse(self.payload)
        result = self.service.search_products(
            query="pasta",
            similar=False,
            limit=15,
            lang="it",
        )

        self.assertEqual(result["query"], "pasta")
        self.assertEqual(result["recommended"], [])
        self.assertEqual(len(result["products"]), 3)

    @patch("pantries_service.requests.get")
    def test_search_products_with_similar_true_returns_ranked_recommended(self, mock_get):
        mock_get.return_value = FakeOffResponse(self.payload)
        result = self.service.search_products(
            query="pasta",
            similar=True,
            limit=2,
            lang="it",
        )

        self.assertEqual([p["code"] for p in result["recommended"]], ["a1", "c3"])

    def test_search_products_invalid_query_raises_400(self):
        with self.assertRaises(PantriesError) as ctx:
            self.service.search_products(query="a", similar=False, limit=15, lang="it")

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.message, "q obbligatorio (minimo 2 caratteri)")

    @patch("pantries_service.requests.get")
    def test_search_products_upstream_error_raises_502(self, mock_get):
        mock_get.side_effect = requests.Timeout("timeout")

        with self.assertRaises(PantriesError) as ctx:
            self.service.search_products(query="pasta", similar=False, limit=15, lang="it")

        self.assertEqual(ctx.exception.status_code, 502)
        self.assertEqual(ctx.exception.message, "OpenFoodFacts unavailable")

    @patch("pantries_service.time.sleep", return_value=None)
    @patch("pantries_service.requests.get")
    def test_search_products_retries_then_succeeds(self, mock_get, _mock_sleep):
        mock_get.side_effect = [
            requests.Timeout("timeout"),
            FakeOffResponse(self.payload),
        ]

        result = self.service.search_products(
            query="pasta",
            similar=False,
            limit=15,
            lang="it",
        )

        self.assertEqual(len(result["products"]), 3)
        self.assertEqual(mock_get.call_count, 2)

    @patch("pantries_service.requests.get")
    def test_get_barcode_product_uses_fallback_name_fields(self, mock_get):
        mock_get.return_value = FakeOffResponse(
            {
                "status": 1,
                "product": {
                    "code": "0123456789012",
                    "product_name": "",
                    "generic_name_it": "Biscotti al cacao",
                    "brands_tags": ["it:Mulino Bianco"],
                },
            }
        )

        result = self.service.get_open_food_facts_product("0123456789012")
        self.assertEqual(result["openFoodFactsId"], "0123456789012")
        self.assertEqual(result["productName"], "Biscotti al cacao")
        self.assertEqual(result["brands"], "Mulino Bianco")

    def test_get_barcode_product_rejects_invalid_barcode(self):
        with self.assertRaises(PantriesError) as ctx:
            self.service.get_open_food_facts_product("12-ABC")

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.message, "Barcode non valido")

    @patch("pantries_service.requests.get")
    def test_search_products_uses_firestore_fallback_when_off_unavailable(self, mock_get):
        mock_get.side_effect = requests.Timeout("timeout")
        db = FakeDbWithCollectionGroup(
            docs=[
                FakeSnapshot(
                    "off-1",
                    {"openFoodFactsId": "off-1", "productName": "Caffe Macinato"},
                ),
                FakeSnapshot(
                    "off-2",
                    {"openFoodFactsId": "off-2", "productName": "Pasta Integrale"},
                ),
            ]
        )
        service = PantriesService(db=db)

        result = service.search_products(
            query="caffè",
            similar=True,
            limit=5,
            lang="it",
        )

        self.assertTrue(len(result["products"]) >= 1)
        self.assertEqual(result["products"][0]["code"], "off-1")
        self.assertEqual(result["recommended"][0]["code"], "off-1")


if __name__ == "__main__":
    unittest.main()
