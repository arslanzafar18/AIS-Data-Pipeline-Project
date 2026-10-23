from pyspark.sql import DataFrame, Window, functions as F


def build_visits(near: DataFrame, gap_s: int) -> DataFrame:
    """One row per vessel visit to an installation; a silence longer than gap_s starts a new visit."""
    w = Window.partitionBy("mmsi", "inst_name").orderBy("event_ts")
    gap = F.col("event_ts").cast("long") - F.lag("event_ts").over(w).cast("long")
    s = (near.withColumn("new_visit", F.when(gap.isNull() | (gap > gap_s), 1).otherwise(0))
             .withColumn("visit_no", F.sum("new_visit").over(w)))
    return (s.groupBy("mmsi", "inst_name", "inst_kind", "visit_no")
             .agg(F.first("name", ignorenulls=True).alias("vessel_name"), F.first("shipType", ignorenulls=True).alias("ship_type"),
                  F.min("event_ts").alias("arrival_ts"), F.max("event_ts").alias("departure_ts"),
                  F.min("dist_m").alias("min_dist_m"), F.count("*").alias("n_msgs"),
                  F.max(F.col("is_at_installation").cast("int")).alias("entered_zone"),
                  F.sum(F.col("is_at_installation").cast("int")).alias("msgs_in_zone"))
             .withColumn("duration_min", (F.col("departure_ts").cast("long") - F.col("arrival_ts").cast("long")) / 60.0))


def build_hourly(near: DataFrame) -> DataFrame:
    return (near.groupBy("inst_name", "inst_kind", F.date_trunc("hour", "event_ts").alias("hour_ts"))
                .agg(F.countDistinct("mmsi").alias("distinct_vessels"), F.count("*").alias("position_msgs"),
                     F.sum(F.col("is_at_installation").cast("int")).alias("msgs_in_zone"),
                     F.avg("speedOverGround").alias("avg_sog_kn")))
