from pyspark.sql import SparkSession
import json, sys

spark = SparkSession.builder.getOrCreate()
table = "default.livy_runner_hive_test"

spark.sql(f"DROP TABLE IF EXISTS {table}")
spark.sql(f"CREATE TABLE {table} (id INT, txt STRING) STORED AS PARQUET")
spark.sql(f"INSERT INTO {table} VALUES (1, 'Hello from livy')")

spark.sql(f"SELECT * FROM {table}").show()

spark.sql(f"DROP TABLE {table}")
