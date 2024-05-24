from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def create_cassandra_connection():
    cluster = Cluster(['cassandra'])
    cass_session = cluster.connect()
    return cass_session

def createSparkSession():
    spark_conn = SparkSession.builder\
        .appName('streaming_alert')\
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
        .config('spark.cassandra.connection.host', 'cassandra')\
        .getOrCreate()
    return spark_conn

def get_all_dangerous_users(spark_conn):
    filter_condition = "dangerosity_score > " + os.environ.get("THRESHOLD_DANGEROSITY_SCORE")
    
    users = spark_conn.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="users", keyspace="spark_streams")\
    .load()\
    .filter(filter_condition)
    
    users.cache()
    return users

if __name__ == "__main__":
    spark_conn = createSparkSession()
    users = get_all_dangerous_users(spark_conn)
    users.createOrReplaceTempView('users')
    spark_conn.sql("SELECT count(*) as nb_of_alerts_trigered from users").show()
    spark_conn.sql("SELECT avg(age) as Average_age_of_dangerous_people from users").show()
    spark_conn.sql("SELECT last_name, first_name, count(*) as nb_reports from users group by id, last_name, first_name order by nb_reports desc limit 10").show()
    spark_conn.sql("SELECT timeframe, count(*) as nb_reports_trigered_by_timeframe from users group by timeframe order by nb_reports_trigered_by_timeframe desc").show()
    