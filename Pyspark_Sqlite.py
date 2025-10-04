from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit, sum, count, max, min, stddev, variance, percentile_approx, udf, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
import sqlite3
import pandas as pd

# Start Spark Session (only needed if not already running)
spark = SparkSession.builder.appName("HousingSQLite").getOrCreate()

# Load dataset
df2 = spark.read.csv("/content/sample_data/california_housing_train.csv", header=True, inferSchema=True)
df2.createOrReplaceTempView("test")

# Show Spark DataFrame
df2.show(5)

# Convert to Pandas DataFrame
pandas_df = df2.toPandas()

# Connect to SQLite
conn = sqlite3.connect("example.db")

# Create table explicitly (optional, pandas will also create schema automatically)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS housing_data (
    longitude REAL,
    latitude REAL,
    housing_median_age INTEGER,
    total_rooms INTEGER,
    total_bedrooms INTEGER,
    population INTEGER,
    households INTEGER,
    median_income REAL,
    median_house_value INTEGER
)
""")

# Insert data into SQLite using Pandas
pandas_df.to_sql("housing_data", conn, if_exists="replace", index=False)

# Query back to check
result = pd.read_sql_query("SELECT * FROM housing_data LIMIT 10;", conn)
print(result)

# Close connection
conn.close()
