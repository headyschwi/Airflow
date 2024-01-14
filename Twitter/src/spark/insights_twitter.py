from pyspark.sql import functions as f
from pyspark.sql import SparkSession
import argparse

def get_twitter_insights(tweets):
    return tweets.groupBy(f.to_date("created_at").alias("created_date"))\
                            .agg(
                                f.countDistinct("author_id").alias("n_users"),
                                f.sum("like_count").alias("n_likes"),
                                f.sum("quote_count").alias("n_quotes"),
                                f.sum("reply_count").alias("n_replies"),
                                f.sum("retweet_count").alias("n_retweets"),
                            ).withColumn("weekday", f.date_format("created_date", "E"))

def export_json(data, path):
    data.coalesce(1).write.mode("overwrite").json(path)
    return

def insights_twitter(spark, src, dest, process_date):
    data = spark.read.json(f"{src}/tweets/*")

    insights = get_twitter_insights(data)

    export_json(insights, f"{dest}/tweets/{process_date}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Twitter Insights")

    parser.add_argument("--src")
    parser.add_argument("--dest")
    parser.add_argument("--process_date")

    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("Getting tweet data insights").getOrCreate()

    insights_twitter(spark, args.src, args.dest, args.process_date)
    