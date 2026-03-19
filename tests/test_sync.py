import main


# Stop scheduler thread started at import to avoid side effects during tests.
try:
    main.scheduler.shutdown(wait=False)
except Exception:
    pass


class DummyJob:
    def __init__(self):
        self.rescheduled = False

    def reschedule(self, trigger):
        self.rescheduled = True
        self.trigger = trigger


class DummyScheduler:
    def __init__(self, job=None):
        self.job = job
        self.added = False

    def get_job(self, _job_id):
        return self.job

    def add_job(self, **kwargs):
        self.added = True
        self.job = DummyJob()


class DummyResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def test_run_sync_returns_409_when_lock_is_busy(monkeypatch):
    monkeypatch.setattr(main, "acquire_sync_lock", lambda: None)

    with main.app.test_client() as client:
        response = client.post("/run_sync")

    assert response.status_code == 409
    data = response.get_json()
    assert data["ok"] is False


def test_run_sync_success_path(monkeypatch):
    dummy_lock = object()
    dummy_job = DummyJob()

    monkeypatch.setattr(main, "acquire_sync_lock", lambda: dummy_lock)
    monkeypatch.setattr(main, "release_sync_lock", lambda lock: None)
    monkeypatch.setattr(main, "sync_products", lambda: {"status": "finished"})
    monkeypatch.setattr(main, "scheduler", DummyScheduler(job=dummy_job))

    with main.app.test_client() as client:
        response = client.post("/run_sync")

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert dummy_job.rescheduled is True


def test_send_to_shopify_updates_existing_product_by_normalized_sku(monkeypatch):
    updated = {}

    def fake_update(variant_id, inventory_item_id, new_price, new_quantity):
        updated["variant_id"] = variant_id
        updated["inventory_item_id"] = inventory_item_id
        updated["new_price"] = new_price
        updated["new_quantity"] = new_quantity

    def should_not_post(*args, **kwargs):
        raise AssertionError("POST create must not be called when SKU exists")

    monkeypatch.setattr(main, "update_shopify_variant", fake_update)
    monkeypatch.setattr(main.requests, "post", should_not_post)

    existing_products = [
        {
            "id": 101,
            "handle": "demo-product",
            "variants": [{"id": 201, "inventory_item_id": 301, "sku": "000000029"}],
        }
    ]

    all_skus = {"000000029"}
    all_handles = {"demo-product"}

    shopify_product = {
        "product": {
            "handle": "Demo-Product",
            "variants": [{"sku": " 000000029 ", "price": "100.00", "inventory_quantity": 5}],
        }
    }

    main.send_to_shopify(shopify_product, existing_products, all_skus, all_handles)

    assert updated["variant_id"] == 201
    assert updated["inventory_item_id"] == 301
    assert updated["new_price"] == "100.00"
    assert updated["new_quantity"] == 5


def test_send_to_shopify_422_conflict_falls_back_to_update(monkeypatch):
    updated = {}

    def fake_post(*args, **kwargs):
        return DummyResponse(422, {"errors": {"handle": ["has already been taken"]}})

    def fake_fetch_all_products():
        return [
            {
                "id": 111,
                "handle": "lemo-mango-marakujya",
                "variants": [{"id": 211, "inventory_item_id": 311, "sku": "000000029"}],
            }
        ]

    def fake_update(variant_id, inventory_item_id, new_price, new_quantity):
        updated["variant_id"] = variant_id
        updated["inventory_item_id"] = inventory_item_id
        updated["new_price"] = new_price
        updated["new_quantity"] = new_quantity

    monkeypatch.setattr(main.requests, "post", fake_post)
    monkeypatch.setattr(main, "fetch_all_shopify_products", fake_fetch_all_products)
    monkeypatch.setattr(main, "update_shopify_variant", fake_update)

    shopify_product = {
        "product": {
            "handle": "LEMO-Манго-Маракуйя",
            "variants": [{"sku": "000000029", "price": "111.11", "inventory_quantity": 7}],
        }
    }

    main.send_to_shopify(shopify_product, existing_products=[], all_skus=set(), all_handles=set())

    assert updated["variant_id"] == 211
    assert updated["inventory_item_id"] == 311
    assert updated["new_price"] == "111.11"
    assert updated["new_quantity"] == 7


def test_sync_products_endpoint_does_not_create_when_sku_already_exists(monkeypatch):
    # 1C returns one product with existing SKU and valid TOV price.
    monkeypatch.setattr(
        main,
        "fetch_products",
        lambda: [
            {
                "id": "000000029",
                "name": "LEMO Манго-Маракуйя",
                "quantity": "12",
                "price": [{"type_price": "ТОВ", "amount": "100,0"}],
            }
        ],
    )

    # Shopify already has this SKU.
    monkeypatch.setattr(
        main,
        "fetch_all_shopify_products",
        lambda: [
            {
                "id": 999,
                "handle": "lemo-манго-маракуйя",
                "variants": [{"id": 111, "inventory_item_id": 222, "sku": "000000029"}],
            }
        ],
    )

    created = {"called": False}
    updated = {"called": False}

    def should_not_create(*args, **kwargs):
        created["called"] = True
        raise AssertionError("Create API must not be called for existing SKU")

    def fake_update(variant_id, inventory_item_id, new_price, new_quantity):
        updated["called"] = True
        updated["variant_id"] = variant_id
        updated["inventory_item_id"] = inventory_item_id
        updated["new_price"] = new_price
        updated["new_quantity"] = new_quantity

    monkeypatch.setattr(main.requests, "post", should_not_create)
    monkeypatch.setattr(main, "update_shopify_variant", fake_update)

    with main.app.test_client() as client:
        response = client.get("/sync_products")

    assert response.status_code == 200
    assert response.get_json()["status"] == "finished"
    assert created["called"] is False
    assert updated["called"] is True
    assert updated["variant_id"] == 111
    assert updated["inventory_item_id"] == 222


def test_sync_products_aborts_when_shopify_catalog_fetch_failed(monkeypatch):
    monkeypatch.setattr(
        main,
        "fetch_products",
        lambda: [
            {
                "id": "000000029",
                "name": "LEMO Манго-Маракуйя",
                "quantity": "1",
                "price": [{"type_price": "ТОВ", "amount": "100,0"}],
            }
        ],
    )
    monkeypatch.setattr(main, "fetch_all_shopify_products", lambda: None)

    called = {"send_to_shopify": False}

    def should_not_send(*args, **kwargs):
        called["send_to_shopify"] = True
        raise AssertionError("send_to_shopify must not be called when catalog fetch failed")

    monkeypatch.setattr(main, "send_to_shopify", should_not_send)

    with main.app.test_client() as client:
        response = client.get("/sync_products")

    assert response.status_code == 503
    assert response.get_json()["status"] == "Shopify catalog fetch failed. Sync aborted."
    assert called["send_to_shopify"] is False
