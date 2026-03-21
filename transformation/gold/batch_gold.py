import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    round as spark_round, lit
)

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
SILVER_PATH      = "s3a://lakehouse/silver/yellow_taxi"
GOLD_PATH        = "s3a://lakehouse/gold"

def create_spark_session():
    print("Starting Spark session...")
    jars_dir = os.path.abspath("jars")
    jars = ",".join([
        os.path.join(jars_dir, "delta-spark_2.12-3.0.0.jar"),
        os.path.join(jars_dir, "delta-storage-3.0.0.jar"),
        os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
        os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar"),
    ])
    os.makedirs("C:\\tmp\\spark", exist_ok=True)
    spark = (
        SparkSession.builder
        .appName("GoldAggregation_YellowTaxi")
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.local.dir", "C:\\tmp\\spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session started successfully")
    return spark

def build_daily_revenue(silver_df, job_run_id):
    print("\n[1/3] Building daily_revenue table...")
    daily = (
        silver_df
        .withColumn("pickup_date", col("tpep_pickup_datetime").cast("date"))
        .groupBy("pickup_date", "year", "month")
        .agg(
            count("*").alias("total_trips"),
            spark_round(spark_sum("fare_amount"), 2).alias("total_fare"),
            spark_round(spark_sum("tip_amount"), 2).alias("total_tips"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_mins"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare")
        )
        .withColumn("_gold_created_at", lit(job_run_id))
        .orderBy("pickup_date")
    )
    (
        daily.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year", "month")
        .save(GOLD_PATH + "/daily_revenue")
    )
    print("      daily_revenue written successfully")
    daily.show(5, truncate=False)

def build_hourly_patterns(silver_df, job_run_id):
    print("\n[2/3] Building hourly_patterns table...")
    hourly = (
        silver_df
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("total_trips"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_mins"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue")
        )
        .withColumn("_gold_created_at", lit(job_run_id))
        .orderBy("pickup_hour")
    )
    (
        hourly.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH + "/hourly_patterns")
    )
    print("      hourly_patterns written successfully")
    hourly.show(24, truncate=False)

def build_location_summary(silver_df, job_run_id):
    print("\n[3/3] Building location_summary table...")
    location = (
        silver_df
        .groupBy("PULocationID")
        .agg(
            count("*").alias("total_pickups"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip")
        )
        .withColumn("_gold_created_at", lit(job_run_id))
        .orderBy(col("total_pickups").desc())
    )
    (
        location.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH + "/location_summary")
    )
    print("      location_summary written successfully")
    location.show(10, truncate=False)

def main():
    job_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    spark = create_spark_session()
    try:
        print("\n" + "="*60)
        print("GOLD AGGREGATION - Job Run: " + job_run_id)
        print("="*60)

        print("\nReading Silver Delta Lake...")
        silver_df = spark.read.format("delta").load(SILVER_PATH)
        print("Silver loaded successfully")

        build_daily_revenue(silver_df, job_run_id)
        build_hourly_patterns(silver_df, job_run_id)
        build_location_summary(silver_df, job_run_id)

        print("\n" + "="*60)
        print("GOLD AGGREGATION COMPLETE")
        print("="*60)
        print("  daily_revenue    : done")
        print("  hourly_patterns  : done")
        print("  location_summary : done")
        print("  Location         : " + GOLD_PATH)
        print("  View in MinIO    : http://localhost:9001")

    except Exception as e:
        print("\nERROR: " + str(e))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()