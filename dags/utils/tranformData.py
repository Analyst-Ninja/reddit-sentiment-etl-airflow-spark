import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    TimestampType,
    DecimalType,
    StructField,
    StructType,
    FloatType,
)
from pyspark.sql.functions import udf, when
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import mysql
from pyspark.sql import functions as F


def readFromMySQL(connection, cursor):
    cursor.execute("SELECT * FROM r_posts")
    result = cursor.fetchall()
    return result


def transformData():
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOSTNAME"),
        database=os.getenv("MYSQL_DATABASE"),
        user=os.getenv("MYSQL_USERNAME"),
        password=os.getenv("MYSQL_PASSWORD"),
        port=os.getenv("MYSQL_PORT"),
    )

    cursor = connection.cursor()

    data = readFromMySQL(connection, cursor)
    # columns = [i[0] for i in cursor.description]

    schema = StructType(
        [
            StructField("id", StringType()),
            StructField("sub_reddit", StringType()),
            StructField("post_type", StringType()),
            StructField("title", StringType()),
            StructField("author", StringType()),
            StructField("text_content", StringType()),
            StructField("url", StringType()),
            StructField("score", StringType()),
            StructField("num_comments", StringType()),
            StructField("upvote_ratio", StringType()),
            StructField("over_18", StringType()),
            StructField("edited", TimestampType()),
            StructField("created_at", TimestampType()),
            StructField("fetched_at", TimestampType()),
            StructField("etl_insert_date", TimestampType()),
        ]
    )

    spark = SparkSession.Builder().appName("reddit-sentiment-analysis").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    df = (
        spark.createDataFrame(data, schema)
        .withColumnRenamed("id", "post_id")
        .withColumn("score", F.col("score").cast("int"))
        .withColumn("num_comments", F.col("num_comments").cast("int"))
        .withColumn("over_18", F.col("over_18").cast("int"))
        .withColumn("upvote_ratio", F.col("upvote_ratio").cast("float"))
        .select("post_id", "title", "text_content", "sub_reddit", "created_at", "score")
    )

    filtered_df = df.filter("text_content IS NOT NULL AND LENGTH(text_content) > 20")

    # Initialize VADER sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Define a UDF to compute sentiment using VADER
    def analyze_sentiment(text):
        sentiment_score = analyzer.polarity_scores(text)["compound"]
        return sentiment_score

    # Register UDF in Spark
    sentiment_udf = udf(analyze_sentiment, FloatType())

    # Apply UDF to DataFrame to calculate sentiment score
    data_with_sentiment = filtered_df.withColumn(
        "sentiment_score", sentiment_udf(filtered_df["text_content"])
    )

    data_with_sentiment = data_with_sentiment.withColumn(
        "sentiment_score_flag",
        when(data_with_sentiment["sentiment_score"] > 0.5, "Positive")
        .when(data_with_sentiment["sentiment_score"] < -0.5, "Negative")
        .otherwise("Neutral"),
    )

    data_with_sentiment.write.mode("overwrite").parquet(
        f"{os.getenv('STAGING_AREA')}/transformed_data.parquet"
    )

    spark.stop()

    return 0
