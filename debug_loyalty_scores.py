from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("DebugLoyaltyScores").getOrCreate()

logs = spark.read.option("header", True).csv("listening_logs.csv")
songs = spark.read.option("header", True).csv("songs_metadata.csv")

logs = logs.join(songs, "song_id")

user_genre_counts = logs.groupBy("user_id", "genre").agg(count("*").alias("genre_count"))
user_total_counts = logs.groupBy("user_id").agg(count("*").alias("total_count"))

loyalty = user_genre_counts.join(user_total_counts, "user_id") \
    .withColumn("loyalty_score", col("genre_count") / col("total_count"))

loyalty.orderBy(col("loyalty_score").desc()).show(50)

