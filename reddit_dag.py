from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from utilts.downloadPosts import extractDataToParquet
from utilts.dataLoad import insertDataToMySQL, insertDataToPostgreSQL
from utilts.tranformData import transformData
from dotenv import load_dotenv
import os
import praw
import mysql.connector
import psycopg2
load_dotenv()

default_args = {
    'owner': 'rohit',
    'start_date': days_ago(0),
    'email': 'r.kumar01@hotmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='SentimentAnalysis-Reddit',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='Reddit ETL to store the new subreddit posts with comments'
)

reddit = praw.Reddit(
    client_id = os.getenv('REDDIT_CLIENT_ID'),
    client_secret = os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent = os.getenv('REDDIT_USER_AGENT'),
    username=os.getenv('REDDIT_USERNAME'),
    password=os.getenv('REDDIT_PASSWORD'),
)

subRedditList = ['python', 'career', 'dataengineering', 'data']

downloadPosts = PythonOperator(
    task_id='Reddit-API-Data-Extraction',
    dag=dag,
    python_callable=extractDataToParquet,
    op_kwargs={'subRedditList': subRedditList, 'redditInstance': reddit}
)

mySQLconnection = mysql.connector.connect(
    host=os.getenv('MYSQL_HOSTNAME'), 
    database=os.getenv('MYSQL_DATABASE'), 
    user=os.getenv('MYSQL_USERNAME'), 
    password=os.getenv('MYSQL_PASSWORD'),
    port=os.getenv('MYSQL_PORT')
)

mySQLcursor = mySQLconnection.cursor()

loadToMySQL = PythonOperator(
    task_id='Data-Ingestion-to-MySQL',
    dag=dag,
    python_callable=insertDataToMySQL,
    op_kwargs={'connection': mySQLconnection, 'cursor': mySQLcursor}
)

transformDataWithSpark = PythonOperator(
    task_id='Sentiment-Analysis-Powered-by-Spark',
    dag=dag,
    python_callable=transformData,
    op_kwargs={'connection': mySQLconnection, 'cursor': mySQLcursor}
)

postgreSQLconnection = psycopg2.connect(
    database = os.getenv('POSTGRES_DATABASE'), 
    user = os.getenv('POSTGRES_USERNAME'), 
    host= os.getenv('POSTGRES_HOSTNAME'),
    password = os.getenv('POSTGRES_PASSWORD'),
    port = os.getenv('POSTGRES_PORT')
)

postgreSQLcursor = postgreSQLconnection.cursor()

loadToPostgreSQL = PythonOperator(
    task_id = "Postgres-Ingestion-for-Analytical-Processing",
    dag=dag,
    python_callable=insertDataToPostgreSQL,
    op_kwargs={'connection': postgreSQLconnection, 'cursor': postgreSQLcursor}
)

downloadPosts >> loadToMySQL >> transformDataWithSpark >> loadToPostgreSQL