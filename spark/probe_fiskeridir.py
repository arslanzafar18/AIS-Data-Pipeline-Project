"""One-off check: which documentation and vessel endpoints the Fiskeridirektoratet vessel API answers on.
Read-only: it only sends GET requests and prints the first part of each answer. Stdlib only.
  python3 probe_fiskeridir.py
"""
import json, urllib.request

BASE = "https://api.fiskeridir.no/vessel-api"
PATHS = ["/", "/swagger/v1/swagger.json", "/swagger/index.html", "/openapi.json", "/v3/api-docs", "/api-docs",
         "/swagger-ui/index.html", "/api/v1/vessels?callsign=LCJM", "/api/vessels?callsign=LCJM", "/vessels?callsign=LCJM"]

for p in PATHS:
    url = BASE + p
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=20) as r:
            body = r.read(2_000_000).decode("utf-8", "replace")
            print(f"== {r.status} {url}  ({r.headers.get('Content-Type')})")
            try:                                   # an OpenAPI document: print its endpoint paths
                doc = json.loads(body)
                if "paths" in doc:
                    print("   endpoints:", ", ".join(doc["paths"]))
                    continue
            except ValueError:
                pass
            print("  ", body[:600].replace("\n", " "))
    except Exception as e:
        print(f"== FAIL {url}: {e}")
