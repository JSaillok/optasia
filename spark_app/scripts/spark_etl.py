from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, date_format

def process_subscribers(spark):
    """Read, cleanse, transform, and save subscribers data."""
    # Read CSV
    subscribers_df = spark.read.csv(
        "/data/subscribers.csv",
        header=False,
        schema="subscriber_id STRING, activation_date STRING"
    )

    # Cleanse (drop nulls)
    cleaned_df = subscribers_df.na.drop()

    # Transform (add row_key, format dates)
    transformed_df = cleaned_df.withColumn(
        "row_key",
        concat(col("subscriber_id"), lit("_"), date_format("activation_date", "yyyyMMdd"))
    ).withColumn(
        "act_dt",
        col("activation_date").cast("date")
    ).select(
        "row_key",
        col("subscriber_id").alias("sub_id"),
        "act_dt"
    )

    # Write to SQLite
    transformed_df.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlite:/data/subscribers.db") \
        .option("dbtable", "subscribers") \
        .option("driver", "org.sqlite.JDBC") \
        .mode("overwrite") \
        .save()

def process_transactions(spark):
    """Read transactions and save as Parquet."""
    transactions_df = spark.read.csv(
        "/data/data_transactions.csv",
        header=False,
        schema="timestamp STRING, subscriber_id STRING, amount DOUBLE, channel STRING"
    )
    transactions_df.write.parquet("/data/output/transactions.parquet")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkETL") \
        .config("spark.jars", "/opt/spark/jars/sqlite-jdbc-3.36.0.3.jar") \
        .getOrCreate()
    
    process_subscribers(spark)
    process_transactions(spark)
    spark.stop()