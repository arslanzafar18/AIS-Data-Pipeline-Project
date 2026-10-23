"""
Bronze for reference data: Sodir FactPages shapefiles (NLOD licence, attribution required).
No pandas/geopandas: pyshp reads the shapefile, pyproj reprojects to WGS84 if the .prj is projected.
Output: CSV with all attributes + lat/lon (point, or bounding-box centre for polygons) + lineage columns.
"""
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
import csv, datetime as dt, io, tempfile, zipfile
from pathlib import Path
import requests, shapefile
from pyproj import CRS, Transformer
import pipeline_config

SRC = {"facilities": "https://factpages.sodir.no/downloads/shape/fclPoint.zip",
       "fields": "https://factpages.sodir.no/downloads/shape/fldArea.zip"}  # confirm filename on first run
OUT = Path(pipeline_config.path(pipeline_config.load(), "bronze/reference")); OUT.mkdir(parents=True, exist_ok=True)

for name, url in SRC.items():
    r = requests.get(url, timeout=120); r.raise_for_status()
    with tempfile.TemporaryDirectory() as td:
        zipfile.ZipFile(io.BytesIO(r.content)).extractall(td)
        shp = next(Path(td).glob("*.shp"))
        prj = shp.with_suffix(".prj")
        tr = None
        if prj.exists():
            crs = CRS.from_wkt(prj.read_text())
            if not crs.is_geographic or crs.to_epsg() != 4326:
                tr = Transformer.from_crs(crs, 4326, always_xy=True)
        sf = shapefile.Reader(str(shp), encoding="utf-8")
        fields = [f[0] for f in sf.fields[1:]]
        with open(OUT / f"{name}.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(fields + ["centroid_lon", "centroid_lat", "_source", "_ingest_ts"])
            for sr in sf.shapeRecords():
                x0, y0, x1, y1 = sr.shape.bbox if sr.shape.shapeType not in (1, 11, 21) else (*sr.shape.points[0],) * 2
                lon, lat = (x0 + x1) / 2, (y0 + y1) / 2
                if tr: lon, lat = tr.transform(lon, lat)
                w.writerow(list(sr.record) + [lon, lat, url, dt.datetime.utcnow().isoformat()])
    print(name, "written; columns:", fields)
