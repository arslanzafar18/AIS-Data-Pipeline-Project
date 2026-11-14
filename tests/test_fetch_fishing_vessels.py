"""Fishing vessel register download: paging, owner removal, CSV rows (no network)."""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("ffv", Path(__file__).parent.parent / "reference" / "fetch_fishing_vessels.py")
F = importlib.util.module_from_spec(spec)
spec.loader.exec_module(F)


def vessel(i, cs="LK%04d"):
    return {"id": str(i), "radioCallSign": cs % i if cs else None, "name": f"BOAT {i} ", "registrationMark": "N 1",
            "length": 12.5, "buildYear": 2001, "owners": [{"entityType": "PERSON", "name": "Private Person"}]}


def test_pages_until_total_and_owners_dropped():
    data = [vessel(i) for i in range(1, 8)]
    calls = []

    def fake_get(url):
        calls.append(url)
        page = int(url.split("page=")[1].split("&")[0])
        return data[(page - 1) * 3: page * 3], 7

    pages, total = F.fetch_pages(get=fake_get, per_page=3)
    assert total == 7 and [len(p) for p in pages] == [3, 3, 1] and len(calls) == 3
    items = [it for p in pages for it in F.strip_personal(p)]
    assert all("owners" not in it for it in items)
    rows = F.to_rows(items, "src", "now")
    assert rows[0]["call_sign"] == "LK0001" and rows[0]["name"] == "BOAT 1" and rows[0]["length_m"] == 12.5


def test_stops_on_empty_page_without_total():
    pages, total = F.fetch_pages(get=lambda url: ([vessel(1)] if "page=1&" in url else [], None), per_page=1)
    assert len(pages) == 1 and total is None


def test_missing_call_sign_is_empty_and_field_check():
    rows = F.to_rows([vessel(5, cs=None)], "src", "now")
    assert rows[0]["call_sign"] == ""
    try:
        F.check_fields([{"id": "1", "name": "X"}])
        raise AssertionError("should refuse")
    except SystemExit as e:
        assert "NOT replaced" in str(e)


def test_page_size_falls_back_when_the_api_says_too_large():
    import urllib.error
    data = [vessel(i) for i in range(1, 6)]

    def fake_get(url):
        size = int(url.split("per_page=")[1])
        if size > 100:
            raise urllib.error.HTTPError(url, 400, "Bad Request", {}, None)
        page = int(url.split("page=")[1].split("&")[0])
        return data[(page - 1) * size: page * size], 5

    assert F.choose_page_size(fake_get) == 100
    pages, total = F.fetch_pages(get=fake_get)
    assert total == 5 and sum(len(p) for p in pages) == 5
