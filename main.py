from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName("readfromcsv").master("local[4]").getOrCreate()

schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

data = spark.readStream.format("csv").schema(schema).option("header", True).option("maxFilesPerTrigger", 1).load("/home/balintpataki/dev/spark")
print(data.printSchema())
print(data.isStreaming)
avg_age = data.groupBy("gender").agg((avg("age").alias("average_age"))).sort(desc("average_age"))
query = avg_age.writeStream.format("console").outputMode("complete").start().awaitTermination()

