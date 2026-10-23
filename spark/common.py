import math
from pyspark.sql import functions as F


def haversine_m(lat1, lon1, lat2, lon2):
    """Spark column expression: great-circle distance in metres."""
    r = 6371000.0
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = p2 - p1
    dl = F.radians(lon2) - F.radians(lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dl / 2) ** 2
    return F.lit(2 * r) * F.asin(F.sqrt(a))


def haversine_py(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def load_facilities(spark, cfg, path):
    """Sodir installation points (CSV from reference/load_sodir.py) -> inst_name, inst_kind, i_lat, i_lon."""
    from pyspark.sql import functions as F
    df = spark.read.option("header", True).csv(path)
    kind = cfg.get("facility_kind_col", "fclKind")
    return (df.select(F.col(cfg.get("facility_name_col", "fclName")).alias("inst_name"),
                      (F.col(kind) if kind in df.columns else F.lit(None)).alias("inst_kind"),
                      F.col("centroid_lat").cast("double").alias("i_lat"),
                      F.col("centroid_lon").cast("double").alias("i_lon"))
              .where("i_lat is not null and i_lon is not null"))
