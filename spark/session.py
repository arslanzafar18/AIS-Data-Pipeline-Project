from pyspark.sql import SparkSession


def get_spark(cfg, name):
    return (SparkSession.builder.appName(f"{name}-{cfg['env']}").master(cfg["spark_master"])
            .config("spark.sql.shuffle.partitions", str(cfg["shuffle_partitions"]))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "false")      # same behaviour on Spark 3.5 and 4.x
            .config("spark.ui.enabled", "false").getOrCreate())
