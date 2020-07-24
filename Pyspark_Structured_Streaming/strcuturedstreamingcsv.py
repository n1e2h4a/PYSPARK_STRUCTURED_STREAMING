from pyspark.sql.types import TimestampType, StringType, StructType, StructField
import pandas as pd

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext
# Path to our 20 JSON files
inputpath = "historicdataset"

schema = StructType([StructField("Date", TimestampType(), True),
                      StructField("Volume", StringType(), True),
                      StructField("Close/Last", StringType(), True),
                      StructField("Open", StringType(), True),
                      StructField("High", StringType(), True),
                      StructField("Low", StringType(), True)])

#print(schema)
 # # Create DataFrame representing data in the JSON files
inputDF = (
  spark
    .read
    .schema(schema)
    .csv(inputpath)
)

#print(inputDF)

# Aggregate number of actions
OpenDF = (
    inputDF
    .groupBy(
        inputDF.Date
    )
    .count()
)
OpenDF.cache()

# Create temp table named 'iot_action_counts'
OpenDF.createOrReplaceTempView("iot_counts")
spark.sql("select Date, sum(count) as total_count from iot_counts group by Date")

#To load data into a streaming DataFrame, we create a DataFrame just how we did with
# inputDF with one key difference: instead of .read, we'll be using .readStream:

# Create streaming equivalent of `inputDF` using .readStream
streamingDF = (
  spark
    .readStream
    .schema(schema)
    .option("maxlinesPerTrigger", 40)
    .csv(inputpath)
)

# Stream `streamingDF` while aggregating by action
streamingCountsDF = (
  streamingDF
    .groupBy(
      streamingDF.Date
    )
    .count()
)

# Is `streamingActionCountsDF` actually streaming?
print(streamingCountsDF.isStreaming)

#
#Now we have a streaming DataFrame, but it isn't streaming anywhere. To
#stream to a destination, we need to call writeStream() on our DataFrame#
spark.conf.set("spark.sql.shuffle.partitions", "2")

# View stream in real-time
query = (
  streamingCountsDF
    .writeStream
    .format("console")
    .queryName("counts")
    .outputMode("complete")
    .start()
)

from time import sleep
sleep(5)