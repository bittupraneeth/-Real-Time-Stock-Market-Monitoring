from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, first, last, stddev_samp
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("DailyBatch").getOrCreate()

# Read today's data
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/stockdb") \
    .option("dbtable", "realtime_stock_data") \
    .option("user", "postgres") \
    .option("password", "password") \
    .load()

# Transform
df = df.withColumn("date", to_date("timestamp"))

agg_df = df.groupBy("company", "date").agg( 
    first("open").alias("open"),
    last("price").alias("close"),
    max("high").alias("high"),
    min("low").alias("low"),
    avg("price").alias("avg_price"),
    sum("volume").alias("total_volume"),
    stddev_samp("price").alias("price_deviation")
)

# Write
agg_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/stockdb") \
    .option("dbtable", "daily_aggregated_data") \
    .option("user", "postgres") \
    .option("password", "password") \
    .mode("append") \
    .save()
