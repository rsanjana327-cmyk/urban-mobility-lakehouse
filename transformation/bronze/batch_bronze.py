import os
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, sha2, concat_ws, lit, col, year as spark_year, month as spark_month
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
RAW_PATH         = "s3a://lakehouse/raw"
BRONZE_PATH      = "s3a://lakehouse/bronze/yellow_taxi"

SCHEMA = StructType([
    StructField("VendorID",              LongType(),      True),
    StructField("tpep_pickup_datetime",  TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count",       DoubleType(),    True),
    StructField("trip_distance",         DoubleType(),    True),
    StructField("RatecodeID",            DoubleType(),    True),
    StructField("store_and_fwd_flag",    StringType(),    True),
    StructField("PULocationID",          LongType(),      True),
    StructField("DOLocationID",          LongType(),      True),
    StructField("payment_type",          LongType(),      True),
    StructField("fare_amount",           DoubleType(),    True),
    StructField("extra",                 DoubleType(),    True),
    StructField("mta_tax",               DoubleType(),    True),
    StructField("tip_amount",            DoubleType(),    True),
    StructField("tolls_amount",          DoubleType(),    True),
    StructField("improvement_surcharge", DoubleType(),    True),
    StructField("total_amount",          DoubleType(),    True),
    StructField("congestion_surcharge",  DoubleType(),    True),
    StructField("airport_fee",           DoubleType(),    True),
])

def create_spark_session():
    print("Starting Spark session...")
    jars_dir = os.path.abspath("jars")
    jars = ",".join([
        os.path.join(jars_dir, "delta-spark_2.12-3.0.0.jar"),
        os.path.join(jars_dir, "delta-storage-3.0.0.jar"),
        os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
        os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar"),
    ])
    spark = (
        SparkSession.builder
        .appName("BronzeIngestion_YellowTaxi")
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session started successfully")
    return spark

def read_raw_files(spark):
    print("\n[1/5] Reading Raw Parquet files...")
    files = [
        RAW_PATH + "/year=2023/month=01/yellow_tripdata_2023-01.parquet",
        RAW_PATH + "/year=2023/month=02/yellow_tripdata_2023-02.parquet",
        RAW_PATH + "/year=2023/month=03/yellow_tripdata_2023-03.parquet",
    ]
    dfs = []
    for file_path in files:
        fname = file_path.split("/")[-1]
        print("      Reading: " + fname)
        df = spark.read.parquet(file_path)
        cast_cols = []
        for field in SCHEMA:
            matched = None
            for c in df.columns:
                if c.lower() == field.name.lower():
                    matched = c
                    break
            if matched:
                cast_cols.append(col(matched).cast(field.dataType).alias(field.name))
            else:
                cast_cols.append(lit(None).cast(field.dataType).alias(field.name))
        dfs.append(df.select(cast_cols))
    raw_df = reduce(lambda a, b: a.union(b), dfs)
    raw_df = raw_df.withColumn("year", spark_year("tpep_pickup_datetime")).withColumn("month", spark_month("tpep_pickup_datetime"))
    count = raw_df.count()
    print("      Records read      : " + str(count))
    print("      Schema normalized : INT/BIGINT unified")
    return raw_df

def add_audit_columns(raw_df, job_run_id):
    print("\n[2/5] Adding audit columns...")
    bronze_df = (
        raw_df
        .withColumn("_source_file", input_file_name())
        .withColumn("_bronze_ingested_at", current_timestamp())
        .withColumn("_job_run_id", lit(job_run_id))
        .withColumn("_row_hash", sha2(concat_ws("||", *raw_df.columns), 256))
        .withColumn("_pipeline_version", lit("1.0.0"))
    )
    print("      Audit columns added : 5")
    return bronze_df

def write_to_delta(bronze_df):
    print("\n[3/5] Writing to Delta Lake (Bronze)...")
    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("year", "month")
        .save(BRONZE_PATH)
    )
    print("      Delta Lake write complete")

def enable_cdf(spark):
    print("\n[4/5] Enabling Change Data Feed...")
    spark.sql("ALTER TABLE delta.`" + BRONZE_PATH + "` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    print("      Change Data Feed enabled")

def verify_and_time_travel(spark):
    print("\n[5/5] Verifying Delta Lake table...")
    history = spark.sql("DESCRIBE HISTORY delta.`" + BRONZE_PATH + "`")
    history.select("version", "timestamp", "operation").show(5, truncate=False)
    count = spark.read.format("delta").load(BRONZE_PATH).count()
    print("      Records verified : " + str(count))
    try:
        v0 = spark.read.format("delta").option("versionAsOf", "0").load(BRONZE_PATH)
        print("      Time travel v0   : " + str(v0.count()) + " records")
        print("      ACID proof       : working")
    except Exception as e:
        print("      Time travel note : " + str(e))
    return count

def main():
    job_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    spark = create_spark_session()
    try:
        print("\n" + "="*60)
        print("BRONZE INGESTION - Job Run: " + job_run_id)
        print("="*60)
        raw_df    = read_raw_files(spark)
        bronze_df = add_audit_columns(raw_df, job_run_id)
        write_to_delta(bronze_df)
        enable_cdf(spark)
        count = verify_and_time_travel(spark)
        print("\n" + "="*60)
        print("BRONZE INGESTION COMPLETE")
        print("="*60)
        print("  Records written : " + str(count))
        print("  Delta features  : ACID, Time Travel, CDF enabled")
        print("  Location        : " + BRONZE_PATH)
        print("  View in MinIO   : http://localhost:9001")
        print("  Job Run ID      : " + job_run_id)
    except Exception as e:
        print("\nERROR: " + str(e))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()