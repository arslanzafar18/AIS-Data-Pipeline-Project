"""Collector retries: doubling waits up to 60 s; rejected credentials wait 10 minutes."""
import requests
from collector import collector as C


def http_error(status, url="https://live.ais.barentswatch.no/v1/combined"):
    r = requests.Response()
    r.status_code, r.url = status, url
    return requests.HTTPError(f"{status}", response=r)


def test_network_and_server_errors_double_up_to_the_cap():
    waits, b = [], 1
    for _ in range(9):
        w, b = C.retry_wait(ConnectionError("reset"), b)
        waits.append(w)
    assert waits == [1, 2, 4, 8, 16, 32, 60, 60, 60]
    assert C.retry_wait(http_error(503), 4) == (4, 8)


def test_rejected_credentials_wait_ten_minutes_and_keep_the_backoff():
    assert C.retry_wait(http_error(401), 4) == (600, 4)
    assert C.retry_wait(http_error(403), 1) == (600, 1)
    assert C.retry_wait(http_error(400, C.TOKEN_URL), 1) == (600, 1)     # invalid_client from the token server
    assert C.retry_wait(http_error(400), 1) == (1, 2)                     # a 400 from the stream is not about us
    assert not C.is_auth_failure(ConnectionError("x"))
