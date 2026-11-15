"""
Gold: loitering over subsea pipelines (security use case). Runs right after each Gold publish, from the PUBLISHED
vessel_tracks (one position per vessel per 10 minutes) and bronze/reference/pipelines.csv (Sodir, weekly).

An episode = a vessel slower than `loiter_max_kn` within `pipeline_near_m` of an in-service pipeline, at least
`inst_near_m` from any installation (installation loitering is already covered by gold/loitering_events) and at least
`pipeline_min_shore_m` from any onshore terminal (pipelines come ashore through fjords and harbours, where slow traffic
is normal), for at least `loiter_min_minutes`; a gap of more than `zone_gap_s` without such a position ends it.
Distances use a local flat projection around each position (error well under 1% at these distances).

Implementation: the spatial part (grid join, point-to-segment distance) uses the DataFrame API; the episode logic
(nearest pipeline per position, gaps-and-islands with LAG and a running SUM, aggregation) is Spark SQL, where window
functions read most clearly.

Writes gold/pipeline_loitering (partitioned by dt) with a whole-table swap and gold/_pipelines.json (row counts).
It never fails Gold: gold_job calls it inside try/except, like the dashboard.
  APP_ENV=prod PYTHONPATH=. python -m spark.gold_pipelines
"""
import json, math, os
from datetime import datetime, timezone
from pyspark.sql import functions as F

M_PER_DEG = 111320.0
SEGMENT_COLUMNS = ["pipeline_id", "name", "label", "medium", "phase", "seg_no", "lon1", "lat1", "lon2", "lat2"]
CELL_DEG = 0.1


def segments_df(spark, path, phases):
    s = spark.read.option("header", True).option("escape", '"').csv(path)   # Python's csv doubles quotes (36"" Gas)
    for c in ("lon1", "lat1", "lon2", "lat2"):
        s = s.withColumn(c, F.col(c).cast("double"))
    if phases:
        s = s.where(F.col("phase").isin(list(phases)))
    return s.select(*SEGMENT_COLUMNS)


def segment_cells(seg, near_m):
    """Each segment once per grid cell its bounding box (grown by near_m) touches, for an equality join with positions."""
    mlat = near_m / M_PER_DEG
    mid = F.radians((F.col("lat1") + F.col("lat2")) / 2)
    mlon = F.lit(near_m) / (F.lit(M_PER_DEG) * F.cos(mid))
    cy = lambda v: F.floor(v / CELL_DEG).cast("int")
    return (seg.withColumn("_y0", cy(F.least("lat1", "lat2") - mlat)).withColumn("_y1", cy(F.greatest("lat1", "lat2") + mlat))
               .withColumn("_x0", cy(F.least("lon1", "lon2") - mlon)).withColumn("_x1", cy(F.greatest("lon1", "lon2") + mlon))
               .withColumn("cy", F.explode(F.sequence("_y0", "_y1"))).withColumn("cx", F.explode(F.sequence("_x0", "_x1")))
               .drop("_y0", "_y1", "_x0", "_x1"))


def point_segment_m(lat, lon, lat1, lon1, lat2, lon2):
    """Distance in metres from (lat, lon) to the segment, in a flat projection centred on the point (Spark columns)."""
    k = F.cos(F.radians(lat)) * M_PER_DEG
    ax, ay = (lon1 - lon) * k, (lat1 - lat) * M_PER_DEG
    bx, by = (lon2 - lon) * k, (lat2 - lat) * M_PER_DEG
    dx, dy = bx - ax, by - ay
    l2 = dx * dx + dy * dy
    t = F.when(l2 > 0, F.greatest(F.lit(0.0), F.least(F.lit(1.0), -(ax * dx + ay * dy) / l2))).otherwise(F.lit(0.0))
    px, py = ax + t * dx, ay + t * dy
    return F.sqrt(px * px + py * py)


def point_segment_m_py(lat, lon, lat1, lon1, lat2, lon2):
    """The same formula in plain Python, for tests."""
    k = math.cos(math.radians(lat)) * M_PER_DEG
    ax, ay, bx, by = (lon1 - lon) * k, (lat1 - lat) * M_PER_DEG, (lon2 - lon) * k, (lat2 - lat) * M_PER_DEG
    dx, dy = bx - ax, by - ay
    l2 = dx * dx + dy * dy
    t = max(0.0, min(1.0, -(ax * dx + ay * dy) / l2)) if l2 > 0 else 0.0
    return math.hypot(ax + t * dx, ay + t * dy)


EPISODES_SQL = """
WITH nearest AS (            -- one row per vessel position: the nearest pipeline segment within reach
  SELECT *, ROW_NUMBER() OVER (PARTITION BY mmsi, event_ts ORDER BY pipe_dist_m) AS rn
  FROM pipeline_positions
), marked AS (               -- a new episode starts at the first position or after a silence longer than {gap_s} s
  SELECT *, CASE WHEN LAG(event_ts) OVER w IS NULL
                   OR unix_timestamp(event_ts) - unix_timestamp(LAG(event_ts) OVER w) > {gap_s} THEN 1 ELSE 0 END AS new_ep
  FROM nearest WHERE rn = 1
  WINDOW w AS (PARTITION BY mmsi, pipeline_id ORDER BY event_ts)
), numbered AS (             -- running sum of the starts = episode number (gaps and islands)
  SELECT *, SUM(new_ep) OVER (PARTITION BY mmsi, pipeline_id ORDER BY event_ts) AS ep FROM marked
)
SELECT mmsi, pipeline_id, first(name) AS pipeline_name, first(label) AS pipeline_label, first(medium) AS medium,
       min(event_ts) AS start_ts, max(event_ts) AS end_ts, min(pipe_dist_m) AS min_dist_m, avg(sog_kn) AS avg_sog_kn,
       count(*) AS n_positions, avg(lat) AS lat, avg(lon) AS lon,
       (unix_timestamp(max(event_ts)) - unix_timestamp(min(event_ts))) / 60.0 AS duration_min,
       to_date(min(event_ts)) AS dt
FROM numbered
GROUP BY mmsi, pipeline_id, ep
HAVING (unix_timestamp(max(event_ts)) - unix_timestamp(min(event_ts))) / 60.0 >= {min_minutes}
"""


def haversine_m(lat1, lon1, lat2, lon2):
    p1, p2 = F.radians(lat1), F.radians(lat2)
    h = F.sin((p2 - p1) / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(F.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371000.0 * F.asin(F.sqrt(h))


def build_pipeline_loitering(tracks, seg, near_m, inst_near_m, max_kn, min_minutes, gap_s, shore=None, min_shore_m=0):
    slow = (tracks.where(F.col("lat").isNotNull() & F.col("lon").isNotNull() & (F.col("sog_kn") < max_kn)
                         & (F.col("dist_m").isNull() | (F.col("dist_m") > inst_near_m)))
                  .select("mmsi", F.col("slot_ts").alias("event_ts"), "lat", "lon", "sog_kn")
                  .withColumn("cy", F.floor(F.col("lat") / CELL_DEG).cast("int"))
                  .withColumn("cx", F.floor(F.col("lon") / CELL_DEG).cast("int")))
    near = (slow.join(F.broadcast(segment_cells(seg, near_m)), ["cy", "cx"])
                .withColumn("pipe_dist_m", point_segment_m(F.col("lat"), F.col("lon"), F.col("lat1"), F.col("lon1"),
                                                           F.col("lat2"), F.col("lon2")))
                .where(F.col("pipe_dist_m") <= near_m))
    if shore is not None and min_shore_m > 0:   # drop positions close to an onshore terminal; checked only for the few
        near_shore = (near.select("mmsi", "event_ts", "lat", "lon").distinct()    # positions already near a pipeline
                          .crossJoin(F.broadcast(shore.select(F.col("i_lat").alias("_slat"), F.col("i_lon").alias("_slon"))))
                          .where(haversine_m(F.col("lat"), F.col("lon"), F.col("_slat"), F.col("_slon")) < min_shore_m)
                          .select("mmsi", "event_ts").distinct())
        near = near.join(near_shore, ["mmsi", "event_ts"], "left_anti")
    near.createOrReplaceTempView("pipeline_positions")
    return near.sparkSession.sql(EPISODES_SQL.format(gap_s=int(gap_s), min_minutes=float(min_minutes)))


def run(spark, cfg, P):
    from spark.gold_plan import has_data, swap_dir
    seg_path, tracks_path = P("bronze/reference/pipelines.csv"), P("gold/vessel_tracks")
    if not os.path.exists(seg_path) or not has_data(tracks_path):
        print("pipeline loitering skipped: no pipelines.csv or no vessel_tracks yet")
        return None
    sc = cfg.get("surveillance", {})
    seg = segments_df(spark, seg_path, sc.get("pipeline_phases", ["IN SERVICE"])).cache()
    tracks = spark.read.parquet(tracks_path)
    if sc.get("live_since"):
        tracks = tracks.where(F.col("slot_ts") >= F.to_timestamp(F.lit(sc["live_since"])))
    from spark.common import load_facilities
    fac_path, shore = P("bronze/reference/facilities.csv"), None
    if os.path.exists(fac_path):
        shore = load_facilities(spark, cfg, fac_path).where("is_onshore AND i_lat IS NOT NULL AND NOT isnan(i_lat)").cache()
    args = (seg, sc.get("pipeline_near_m", 500), cfg.get("near_m", 5000), sc.get("loiter_max_kn", 2.0),
            sc.get("loiter_min_minutes", 120), cfg.get("zone_gap_s", 1800))
    min_shore = sc.get("pipeline_min_shore_m", 15000)
    without_shore_filter = build_pipeline_loitering(tracks, *args).count()      # evidence: what the shore rule removes
    out = build_pipeline_loitering(tracks, *args, shore=shore, min_shore_m=min_shore)
    stage = P("gold/_staging_pipeline_loitering")
    out.write.mode("overwrite").partitionBy("dt").parquet(stage)
    rows = spark.read.parquet(stage).count() if has_data(stage) else 0
    swap_dir(stage, P("gold/pipeline_loitering"))
    info = {"finished_at": datetime.now(timezone.utc).isoformat(), "rows": rows, "rows_without_shore_filter": without_shore_filter,
            "onshore_terminals": shore.count() if shore is not None else 0, "min_shore_m": min_shore,
            "segments": seg.count(), "pipelines": seg.select("pipeline_id").distinct().count(), "near_m": sc.get("pipeline_near_m", 500)}
    json.dump(info, open(P("gold/_pipelines.json"), "w"), indent=2)
    seg.unpersist()
    print(json.dumps({"pipeline_loitering": info}))
    return info


def main():
    import pipeline_config
    from spark.session import get_spark
    cfg = pipeline_config.load()
    run(get_spark(cfg, "gold-pipelines"), cfg, lambda rel: pipeline_config.path(cfg, rel))


if __name__ == "__main__":
    main()
