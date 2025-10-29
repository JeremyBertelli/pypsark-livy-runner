from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LivyTestJob").getOrCreate()

print("Spark session started")

# 1. Création d’une table temporaire (dans le warehouse local ou /tmp)
df = spark.createDataFrame([(1, "foo"), (2, "bar"), (3, "baz")], ["id", "value"])
df.createOrReplaceTempView("test_table")

print("Table test_table created")

# 2. Lecture + affichage
count = spark.sql("SELECT COUNT(*) AS c FROM test_table").collect()[0]["c"]
print("Table test_table has {} rows".format(count))

# 3. Suppression
spark.catalog.dropTempView("test_table")
print("Table test_table dropped")

print("Job done")
