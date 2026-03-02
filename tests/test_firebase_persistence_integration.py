import os
import time
import unittest
import uuid

from app import app, db


RUN_FIREBASE_INTEGRATION = os.getenv("RUN_FIREBASE_INTEGRATION") == "1"


@unittest.skipUnless(
    RUN_FIREBASE_INTEGRATION,
    "Set RUN_FIREBASE_INTEGRATION=1 to run Firebase integration tests.",
)
class FirebasePersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = app.test_client()

    def setUp(self):
        nonce = uuid.uuid4().hex[:8]
        self.uid = f"itest_{int(time.time())}_{nonce}"
        self.email = f"{self.uid}@example.com"
        self.item_id = f"itest_item_{nonce}"
        self.user_ref = db.collection("users").document(self.uid)
        self.item_ref = self.user_ref.collection("pantry").document(self.item_id)

    def tearDown(self):
        try:
            self.item_ref.delete()
        except Exception:
            pass
        try:
            self.user_ref.delete()
        except Exception:
            pass

    def test_user_and_pantry_are_persisted_on_firestore(self):
        user_payload = {
            "uid": self.uid,
            "email": self.email,
            "name": "Integration Tester",
        }
        user_response = self.client.post("/get-user-data", json=user_payload)
        self.assertIn(user_response.status_code, (200, 201))

        user_doc = self.user_ref.get()
        self.assertTrue(user_doc.exists)
        user_data = user_doc.to_dict() or {}
        self.assertEqual(user_data.get("uid"), self.uid)
        self.assertEqual(user_data.get("email"), self.email)

        pantry_payload = {
            "uid": self.uid,
            "openFoodFactsId": self.item_id,
            "quantityDelta": 2,
            "productName": "Integration Test Pasta",
        }
        pantry_response = self.client.post("/pantry/add", json=pantry_payload)
        self.assertIn(pantry_response.status_code, (200, 201))

        pantry_doc = self.item_ref.get()
        self.assertTrue(pantry_doc.exists)
        pantry_data = pantry_doc.to_dict() or {}
        self.assertEqual(pantry_data.get("openFoodFactsId"), self.item_id)
        self.assertEqual(pantry_data.get("productName"), "Integration Test Pasta")
        self.assertEqual(pantry_data.get("quantity"), 2)

        list_response = self.client.get(f"/pantry/{self.uid}")
        self.assertEqual(list_response.status_code, 200)
        list_payload = list_response.get_json() or {}
        items = list_payload.get("items", [])
        self.assertTrue(
            any(
                item.get("openFoodFactsId") == self.item_id
                and item.get("quantity") == 2
                for item in items
            )
        )


if __name__ == "__main__":
    unittest.main()
