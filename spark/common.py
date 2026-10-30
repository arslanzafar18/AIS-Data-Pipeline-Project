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


def filter_facilities(df, flt, kind_col):
    """Keep only the Sodir facilities the analysis is about (config facility_filter).
    No filter configured -> unchanged (dev synthetic data). Missing columns fail loudly."""
    if not flt:
        return df
    cond = F.lit(True)
    for col, allowed in flt.get("include", {}).items():
        cond = cond & F.col(col).isin(allowed)
    if flt.get("exclude_kinds"):
        cond = cond & ~F.col(kind_col).isin(flt["exclude_kinds"])
    if flt.get("fixed_col"):
        fixed = F.col(flt["fixed_col"]) == "FIXED"
        mov = flt.get("moveable_kinds_allowed") or []
        cond = cond & ((fixed | F.col(kind_col).isin(mov)) if mov else fixed)
    return df.where(cond)


def load_facilities(spark, cfg, path):
    """Sodir installation points -> inst_name, inst_kind, is_onshore, i_lat, i_lon (filtered per config)."""
    df = spark.read.option("header", True).csv(path)
    kind = cfg.get("facility_kind_col", "fclKind")
    df = filter_facilities(df, cfg.get("facility_filter"), kind)
    kind_c = F.col(kind) if kind in df.columns else F.lit(None).cast("string")
    onshore = F.coalesce(kind_c.isin(cfg.get("onshore_kinds", ["ONSHORE FACILITY"])), F.lit(False))
    return (df.select(F.col(cfg.get("facility_name_col", "fclName")).alias("inst_name"),
                      kind_c.alias("inst_kind"), onshore.alias("is_onshore"),
                      F.col("centroid_lat").cast("double").alias("i_lat"),
                      F.col("centroid_lon").cast("double").alias("i_lon"))
              .where("i_lat is not null and i_lon is not null"))
