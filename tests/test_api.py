def test_openapi(client):
    res = client.get("/openapi.json")
    assert res.status_code == 200
    data = res.json()
    assert "/api/v1/dashboard/monthly" in data["paths"]
    assert "/api/v1/monthly" in data["paths"]
    assert "/api/v1/snapshots/copy-latest" in data["paths"]
    assert "/api/v1/trades" in data["paths"]


def test_account_asset_snapshot_crud_and_dashboard(client):
    account = client.post(
        "/api/v1/accounts",
        json={"name": "楽天証券", "display_order": 1, "is_active": True},
    )
    assert account.status_code == 201
    account_id = account.json()["id"]

    asset = client.post(
        "/api/v1/assets",
        json={
            "account_id": account_id,
            "name": "全世界株",
            "asset_type": "fund",
            "currency": "JPY",
            "display_order": 1,
            "is_active": True,
        },
    )
    assert asset.status_code == 201
    asset_id = asset.json()["id"]

    snap = client.post(
        "/api/v1/snapshots",
        json={"month": "2026-01", "asset_id": asset_id, "value_jpy": 1000000, "memo": "m1"},
    )
    assert snap.status_code == 201
    body = snap.json()
    assert body["account_id"] == account_id

    latest = client.get("/api/v1/dashboard/latest")
    assert latest.status_code == 200
    latest_data = latest.json()
    assert latest_data["month"] == "2026-01"
    assert latest_data["total_jpy"] == 1000000
    assert latest_data["by_asset_type"][0]["asset_type"] == "fund"

    monthly = client.get("/api/v1/dashboard/monthly", params={"from": "2026-01", "to": "2026-01"})
    assert monthly.status_code == 200
    point = monthly.json()["points"][0]
    assert point["total_jpy"] == 1000000
    assert point["by_asset_type"]["fund"] == 1000000


def test_unique_and_integrity_conflict_returns_409(client):
    r1 = client.post("/api/v1/accounts", json={"name": "銀行A"})
    assert r1.status_code == 201

    r2 = client.post("/api/v1/accounts", json={"name": "銀行A"})
    assert r2.status_code == 409

    bad_asset = client.post(
        "/api/v1/assets",
        json={"account_id": 999, "name": "不正", "asset_type": "cash", "currency": "JPY"},
    )
    assert bad_asset.status_code == 409


def test_delete_referenced_account_returns_409(client):
    acc = client.post("/api/v1/accounts", json={"name": "証券口座"}).json()
    asset = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "現金", "asset_type": "cash", "currency": "JPY"},
    )
    assert asset.status_code == 201

    delete_res = client.delete(f"/api/v1/accounts/{acc['id']}")
    assert delete_res.status_code == 409


def test_snapshot_duplicate_returns_409(client):
    acc = client.post("/api/v1/accounts", json={"name": "口座X"}).json()
    asset = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "投信A", "asset_type": "fund", "currency": "JPY"},
    ).json()

    s1 = client.post(
        "/api/v1/snapshots",
        json={"month": "2026-02", "asset_id": asset["id"], "value_jpy": 100},
    )
    assert s1.status_code == 201

    s2 = client.post(
        "/api/v1/snapshots",
        json={"month": "2026-02", "asset_id": asset["id"], "value_jpy": 200},
    )
    assert s2.status_code == 409


def test_monthly_tree_api(client):
    acc_active = client.post("/api/v1/accounts", json={"name": "A1", "is_active": True}).json()
    acc_inactive = client.post("/api/v1/accounts", json={"name": "A2", "is_active": False}).json()

    active_asset = client.post(
        "/api/v1/assets",
        json={"account_id": acc_active["id"], "name": "FundA", "asset_type": "fund", "currency": "JPY", "is_active": True},
    ).json()
    client.post(
        "/api/v1/assets",
        json={"account_id": acc_active["id"], "name": "CashB", "asset_type": "cash", "currency": "JPY", "is_active": True},
    )
    client.post(
        "/api/v1/assets",
        json={"account_id": acc_active["id"], "name": "Hidden", "asset_type": "cash", "currency": "JPY", "is_active": False},
    )
    client.post(
        "/api/v1/assets",
        json={"account_id": acc_inactive["id"], "name": "Nope", "asset_type": "cash", "currency": "JPY", "is_active": True},
    )

    client.post(
        "/api/v1/snapshots",
        json={"month": "2026-02", "asset_id": active_asset["id"], "value_jpy": 123456},
    )

    res = client.get("/api/v1/monthly", params={"month": "2026-02"})
    assert res.status_code == 200
    data = res.json()
    assert data["month"] == "2026-02"
    assert len(data["accounts"]) == 1
    assert data["accounts"][0]["account_id"] == acc_active["id"]
    assert len(data["accounts"][0]["assets"]) == 2
    assert data["summary"]["filled"] == 1
    assert data["summary"]["missing"] == 1


def test_copy_latest_skips_existing(client):
    acc = client.post("/api/v1/accounts", json={"name": "CopyAcc"}).json()
    a1 = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "F1", "asset_type": "fund", "currency": "JPY", "is_active": True},
    ).json()
    a2 = client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "C1", "asset_type": "cash", "currency": "JPY", "is_active": True},
    ).json()
    client.post(
        "/api/v1/assets",
        json={"account_id": acc["id"], "name": "X1", "asset_type": "other", "currency": "JPY", "is_active": False},
    )

    client.post("/api/v1/snapshots", json={"month": "2026-01", "asset_id": a1["id"], "value_jpy": 1000})
    client.post("/api/v1/snapshots", json={"month": "2026-01", "asset_id": a2["id"], "value_jpy": 2000})
    client.post("/api/v1/snapshots", json={"month": "2026-02", "asset_id": a1["id"], "value_jpy": 3000})

    copy = client.post("/api/v1/snapshots/copy-latest", json={"to_month": "2026-02"})
    assert copy.status_code == 200
    payload = copy.json()
    assert payload["from_month"] == "2026-02"
    assert payload["to_month"] == "2026-02"
    assert payload["created"] == 1
    assert payload["skipped"] == 1


def test_trades_crud_flow(client):
    payload = {
        "market": "JP",
        "symbol": "7203",
        "name": "Toyota",
        "notes_buy": "breakout",
        "notes_sell": "target hit",
        "notes_review": "good",
        "rating": 4,
        "tags": "swing,auto",
        "chart_image_url": "https://example.com/c.png",
        "fills": [
            {"side": "buy", "date": "2026-01-10", "price": 1000, "qty": 10, "fee": 100},
            {"side": "sell", "date": "2026-01-15", "price": 1200, "qty": 10, "fee": 100},
        ],
    }
    created = client.post("/api/v1/trades", json=payload)
    assert created.status_code == 201
    trade = created.json()
    trade_id = trade["id"]
    assert trade["opened_at"] == "2026-01-10"
    assert trade["closed_at"] == "2026-01-15"
    assert trade["profit_jpy"] == 1800
    assert trade["profit_usd"] is None
    assert trade["profit_currency"] == "JPY"
    assert trade["holding_days"] == 5
    assert trade["review_done"] is False
    assert trade["reviewed_at"] is None

    listed = client.get("/api/v1/trades", params={"market": "JP", "symbol": "720"})
    assert listed.status_code == 200
    list_body = listed.json()
    assert list_body["total"] == 1
    assert list_body["limit"] == 20
    assert list_body["offset"] == 0
    assert "stats" in list_body
    assert "pending_review_count" in list_body["stats"]
    assert list_body["items"][0]["id"] == trade_id

    detail = client.get(f"/api/v1/trades/{trade_id}")
    assert detail.status_code == 200
    assert len(detail.json()["fills"]) == 2

    updated = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={
            "rating": 5,
            "fills": [
                {"side": "buy", "date": "2026-01-10", "price": 1000, "qty": 10, "fee": 50},
                {"side": "sell", "date": "2026-01-20", "price": 1300, "qty": 10, "fee": 50},
            ],
        },
    )
    assert updated.status_code == 200
    updated_body = updated.json()
    assert updated_body["rating"] == 5
    assert updated_body["closed_at"] == "2026-01-20"
    assert updated_body["profit_jpy"] == 2900
    assert updated_body["profit_usd"] is None
    assert updated_body["profit_currency"] == "JPY"
    assert updated_body["holding_days"] == 10

    deleted = client.delete(f"/api/v1/trades/{trade_id}")
    assert deleted.status_code == 204
    assert client.get(f"/api/v1/trades/{trade_id}").status_code == 404


def test_trades_validation_422(client):
    bad = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "9984",
            "fills": [
                {"side": "buy", "date": "2026-01-10", "price": 1000, "qty": 10, "fee": 0},
                {"side": "sell", "date": "2026-01-09", "price": 900, "qty": 10, "fee": 0},
            ],
        },
    )
    assert bad.status_code == 422


def test_trades_create_open_position_buy_only(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "AAPL",
            "fills": [
                {"side": "buy", "date": "2026-02-10", "price": 200, "qty": 5, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    body = created.json()
    assert body["is_open"] is True
    assert body["closed_at"] is None
    assert body["profit_jpy"] is None
    assert body["profit_usd"] is None
    assert body["profit_currency"] == "USD"
    assert body["holding_days"] is None
    assert len(body["fills"]) == 1


def test_trades_us_closed_returns_profit_usd(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "US",
            "symbol": "MSFT",
            "fills": [
                {"side": "buy", "date": "2026-02-10", "price": 100, "qty": 3, "fee": 1},
                {"side": "sell", "date": "2026-02-12", "price": 120, "qty": 3, "fee": 1},
            ],
        },
    )
    assert created.status_code == 201
    body = created.json()
    assert body["profit_jpy"] is None
    assert body["profit_usd"] == 58
    assert body["profit_currency"] == "USD"


def test_trades_list_pagination_limit_offset(client):
    for symbol in ["AAA", "BBB", "CCC"]:
        created = client.post(
            "/api/v1/trades",
            json={
                "market": "JP",
                "symbol": symbol,
                "fills": [
                    {"side": "buy", "date": "2026-02-01", "price": 100, "qty": 1, "fee": 0},
                    {"side": "sell", "date": "2026-02-02", "price": 110, "qty": 1, "fee": 0},
                ],
            },
        )
        assert created.status_code == 201

    listed = client.get(
        "/api/v1/trades",
        params={"limit": 1, "offset": 1, "sort": "buy_date", "sort_dir": "asc"},
    )
    assert listed.status_code == 200
    body = listed.json()
    assert body["limit"] == 1
    assert body["offset"] == 1
    assert body["total"] == 3
    assert len(body["items"]) == 1


def test_trades_review_filter_and_update(client):
    created = client.post(
        "/api/v1/trades",
        json={
            "market": "JP",
            "symbol": "R1",
            "fills": [
                {"side": "buy", "date": "2026-03-01", "price": 100, "qty": 1, "fee": 0},
                {"side": "sell", "date": "2026-03-02", "price": 110, "qty": 1, "fee": 0},
            ],
        },
    )
    assert created.status_code == 201
    trade_id = created.json()["id"]

    pending = client.get("/api/v1/trades", params={"review": "pending"})
    assert pending.status_code == 200
    pending_body = pending.json()
    assert pending_body["total"] == 1
    assert pending_body["stats"]["pending_review_count"] == 1

    updated = client.patch(
        f"/api/v1/trades/{trade_id}",
        json={"review_done": True, "reviewed_at": "2026-03-05"},
    )
    assert updated.status_code == 200
    assert updated.json()["review_done"] is True
    assert updated.json()["reviewed_at"] == "2026-03-05"

    done = client.get("/api/v1/trades", params={"review": "done"})
    assert done.status_code == 200
    done_body = done.json()
    assert done_body["total"] == 1
    assert done_body["items"][0]["review_done"] is True
    assert done_body["stats"]["pending_review_count"] == 0
