import os
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf, when
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def readFromMySQL(connection, cursor):
    cursor.execute("SELECT * FROM r_posts")
    result = cursor.fetchall()
    return result

def transformData(connection, cursor):

    data = readFromMySQL(connection, cursor)
    columns = [i[0] for i in cursor.description]

    spark = SparkSession.Builder().appName('reddit-sentiment-analysis').getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    df = \
        spark.createDataFrame(data, columns) \
        .withColumnRenamed('id', 'post_id')  \
        .select(
            'post_id', 
            'title', 
            'text_content', 
            'sub_reddit',
            'created_at',
            'score')

    filtered_df = df.filter('text_content IS NOT NULL AND LENGTH(text_content) > 20')

    # Initialize VADER sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Define a UDF to compute sentiment using VADER
    def analyze_sentiment(text):
        sentiment_score = analyzer.polarity_scores(text)['compound']
        return sentiment_score

    # Register UDF in Spark
    sentiment_udf = udf(analyze_sentiment, FloatType())

    # Apply UDF to DataFrame to calculate sentiment score
    data_with_sentiment = filtered_df.withColumn('sentiment_score', sentiment_udf(filtered_df['text_content']))

    data_with_sentiment = data_with_sentiment \
        .withColumn('sentiment_score_flag',
                    when(data_with_sentiment['sentiment_score'] > 0.5, "Positive")\
                    .when(data_with_sentiment['sentiment_score'] < -0.5, "Negative")\
                    .otherwise("Neutral"))

    data_with_sentiment.write.mode('overwrite').parquet(f"{os.getenv('STAGING_AREA')}/transformed_data.parquet")

    return 0


