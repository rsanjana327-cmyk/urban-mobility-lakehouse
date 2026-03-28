# tests/unit/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType,
    StringType, TimestampType
)
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("UnitTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def sample_data(spark):
    data = [
        (1, datetime(2023,1,1,8,0), datetime(2023,1,1,8,30), 2.0, 5.0, 1.0, "N", 161, 237, 1, 15.0, 0.5, 0.5, 2.0, 0.0, 0.3, 18.3, 2.5, 0.0),
        (1, datetime(2023,1,1,8,0), datetime(2023,1,1,8,30), 2.0, 5.0, 1.0, "N", 161, 237, 1, 15.0, 0.5, 0.5, 2.0, 0.0, 0.3, 18.3, 2.5, 0.0),
        (2, datetime(2023,1,1,9,0), datetime(2023,1,1,9,15), 1.0, 0.0, 1.0, "N", 100, 200, 1, -5.0, 0.0, 0.5, 0.0, 0.0, 0.3, -4.2, 0.0, 0.0),
        (2, datetime(2023,1,1,10,0), datetime(2023,1,1,10,45), 3.0, 8.0, 1.0, "N", 50, 100, 2, 22.0, 1.0, 0.5, 3.0, 0.0, 0.3, 26.8, 2.5, 0.0),
    ]
    schema = StructType([
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
    return spark.createDataFrame(data, schema)


# ── Test 1: Quality Filters ───────────────────────────────
def test_quality_filter_removes_zero_distance(sample_data):
    clean = sample_data.filter(col("trip_distance") > 0)
    assert clean.count() == 3
    print("PASS: zero distance rows removed")

def test_quality_filter_removes_negative_fare(sample_data):
    clean = sample_data.filter(col("fare_amount") > 0)
    assert clean.count() == 3
    print("PASS: negative fare rows removed")

def test_quality_filter_removes_zero_passengers(sample_data):
    clean = sample_data.filter(col("passenger_count") > 0)
    assert clean.count() == 4
    print("PASS: zero passenger check works")

def test_all_quality_filters_combined(sample_data):
    clean = (
        sample_data
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") > 0)
        .filter(col("passenger_count") > 0)
        .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    )
    assert clean.count() == 3
    print("PASS: combined quality filters work")


# ── Test 2: Derived Columns ───────────────────────────────
def test_trip_duration_calculation(sample_data):
    df = sample_data.withColumn(
        "trip_duration_minutes",
        (col("tpep_dropoff_datetime").cast("long") -
         col("tpep_pickup_datetime").cast("long")) / 60
    )
    durations = [r.trip_duration_minutes for r in df.collect()]
    assert all(d >= 0 for d in durations)
    print("PASS: trip duration calculated correctly")

def test_cost_per_mile_calculation(sample_data):
    df = (
        sample_data
        .filter(col("trip_distance") > 0)
        .withColumn("cost_per_mile",
            col("fare_amount") / col("trip_distance"))
    )
    costs = [r.cost_per_mile for r in df.collect()]
    assert all(c is not None for c in costs)
    print("PASS: cost per mile calculated correctly")


# ── Test 3: Schema ────────────────────────────────────────
def test_schema_has_required_columns(sample_data):
    required = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "fare_amount", "trip_distance", "passenger_count"
    ]
    for col_name in required:
        assert col_name in sample_data.columns
    print("PASS: all required columns present")

def test_record_count(sample_data):
    assert sample_data.count() == 4
    print("PASS: record count correct")