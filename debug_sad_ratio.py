from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("DebugSadRatio").getOrCreate()

logs = spark.read.option("header", True).csv("listening_logs.csv")
songs = spark.read.option("header", True).csv("songs_metadata.csv")

logs = logs.join(songs, "song_id")

sad_plays = logs.filter(col("mood") == "Sad").groupBy("user_id").agg(count("*").alias("sad_count"))
total_plays = logs.groupBy("user_id").agg(count("*").alias("total_count"))

ratios = sad_plays.join(total_plays, "user_id") \
    .withColumn("sad_ratio", col("sad_count") / col("total_count"))

ratios.orderBy(col("sad_ratio").desc()).show(20)

