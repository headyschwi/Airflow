from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from os.path import join
import argparse

def get_tweets_data(data):
    tweets = data.select(f.explode("data").alias("tweets"))\
    .select("tweets.author_id", "tweets.conversation_id", "tweets.created_at", "tweets.id", "tweets.public_metrics.*", "tweets.text")

    return tweets

def get_users_data(data):
    users = data.select(f.explode("includes.users").alias("users")).select("users.*")

    return users

def export_json(data, path):
    data.coalesce(1).write.mode("overwrite").json(path)

def twitter_transformation(spark, src, dest, process_date):
    data = spark.read.json(src)

    tweets = get_tweets_data(data)
    users = get_users_data(data)

    table_dest = join(dest, "{table_name}", f"{process_date}")
    export_json(tweets, table_dest.format(table_name = "tweets"))
    export_json(users, table_dest.format(table_name = "users"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Twitter Transformation")

    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process_date", required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

    twitter_transformation(spark, args.src, args.dest, args.process_date)