# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: User Favorite Genres
user_songs = logs_df.join(songs_df, "song_id")
user_genre_time = user_songs.groupBy("user_id", "genre").agg(sum("duration_sec").alias("total_listen_time"))
window_spec = Window.partitionBy("user_id").orderBy(col("total_listen_time").desc())
user_favorite_genre = user_genre_time.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")
user_favorite_genre.show()
user_favorite_genre.write.csv("outputs/user_favorite_genres", header=True)

# Task 2: Average Listen Time
avg_listen_per_user = logs_df.groupBy("user_id").agg(avg("duration_sec").alias("avg_listen_duration"))
avg_listen_per_user.show()
avg_listen_per_user.write.csv("outputs/avg_listen_per_user", header=True)

avg_listen_per_song = logs_df.groupBy("song_id").agg(avg("duration_sec").alias("avg_listen_duration"))
avg_listen_per_song.show()
avg_listen_per_song.write.csv("outputs/avg_listen_per_song", header=True)

# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
genre_stats = user_songs.groupBy("user_id", "genre").agg(
    count("song_id").alias("listen_count"),
    sum("duration_sec").alias("total_time")
)
user_total_time = logs_df.groupBy("user_id").agg(sum("duration_sec").alias("user_total_time"))
genre_loyalty = genre_stats.join(user_total_time, "user_id") \
    .withColumn("loyalty_score", col("listen_count") * col("total_time") / col("user_total_time"))
top_10_loyalty = genre_loyalty.orderBy(col("loyalty_score").desc()).limit(10)
top_10_loyalty.select("user_id", "genre", "loyalty_score").show()
top_10_loyalty.write.csv("outputs/top_10_loyalty_scores", header=True)

# Task 4: Identify users who listen between 12 AM and 5 AM
logs_with_hour = logs_df.withColumn("hour", hour(to_timestamp("timestamp")))
night_listeners = logs_with_hour.filter((col("hour") >= 0) & (col("hour") <= 5))
night_users = night_listeners.select("user_id").distinct()
night_users.show()
night_users.write.csv("outputs/night_listeners", header=True)

spark.stop()
