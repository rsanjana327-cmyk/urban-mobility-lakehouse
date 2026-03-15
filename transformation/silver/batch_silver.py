import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, row_number
from pyspark.sql.window import Window

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BRONZE_PATH      = "s3a://lakehouse/bronze/yellow_taxi"
SILVER_PATH      = "s3a://lakehouse/silver/yellow_taxi"

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
        .appName("SilverTransform_YellowTaxi")
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "3g")
        .config("spark.local.dir", "C:\\tmp\\spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session started successfully")
    return spark

def main():
    job_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    spark = create_spark_session()
    try:
        print("\n" + "="*60)
        print("SILVER TRANSFORM - Job Run: " + job_run_id)
        print("="*60)

        print("\n[1/5] Reading Bronze Delta Lake...")
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
        print("      Columns : " + str(len(bronze_df.columns)))

        print("\n[2/5] Deduplicating records...")
        window = Window.partitionBy(
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID"
        ).orderBy(col("_bronze_ingested_at").desc())
        deduped_df = (
            bronze_df
            .withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        print("      Deduplication applied")

        print("\n[3/5] Applying quality filters...")
        clean_df = (
            deduped_df
            .filter(col("trip_distance") > 0)
            .filter(col("fare_amount") > 0)
            .filter(col("passenger_count") > 0)
            .filter(col("tpep_pickup_datetime").isNotNull())
            .filter(col("tpep_dropoff_datetime").isNotNull())
            .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
            .filter(col("PULocationID").isNotNull())
            .filter(col("DOLocationID").isNotNull())
        )
        print("      Quality filters applied")

        print("\n[4/5] Adding Silver columns...")
        silver_df = (
            clean_df
            .withColumn("trip_duration_minutes",
                (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60)
            .withColumn("cost_per_mile",
                col("fare_amount") / col("trip_distance"))
            .withColumn("pickup_hour",
                col("tpep_pickup_datetime").cast("string").substr(12, 2).cast("int"))
            .withColumn("_silver_ingested_at", current_timestamp())
            .withColumn("_silver_job_run_id", lit(job_run_id))
        )
        print("      Added: trip_duration_minutes, cost_per_mile, pickup_hour")

        print("\n[5/5] Writing Silver Delta Lake...")
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year", "month")
            .save(SILVER_PATH)
        )

        count = spark.read.format("delta").load(SILVER_PATH).count()
        print("      Silver records : " + str(count))

        print("\n" + "="*60)
        print("SILVER TRANSFORM COMPLETE")
        print("="*60)
        print("  Records in Silver : " + str(count))
        print("  New columns       : trip_duration_minutes, cost_per_mile, pickup_hour")
        print("  Location          : " + SILVER_PATH)
        print("  View in MinIO     : http://localhost:9001")

    except Exception as e:
        print("\nERROR: " + str(e))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()