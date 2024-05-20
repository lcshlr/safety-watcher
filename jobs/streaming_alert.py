import logging
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def createSparkSession():
    spark_conn = SparkSession.builder\
        .appName('streaming_alert')\
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
        .config('spark.cassandra.connection.host', 'cassandra')\
        .getOrCreate()
    logger.info("Spark connection created successfully!")
    return spark_conn

def create_cassandra_connection():
    cluster = Cluster(['cassandra'])
    cass_session = cluster.connect()
    return cass_session

def get_kafka_stream(spark_conn):
    kafka_df = spark_conn.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'broker:29092') \
        .option('subscribe', 'users') \
        .option('startingOffsets', 'earliest') \
        .load()
    logger.info("kafka dataframe created successfully")
    return kafka_df

def format_kafka_data(data):
    user_schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField('location', ArrayType(FloatType()), False),
        StructField("dangerosity_score", IntegerType(), False),
        StructField("behaviors", ArrayType(StringType()), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    json_data = data.selectExpr("CAST(value AS STRING) as value")
    formatted_data = json_data.withColumn("value", from_json(json_data.value, user_schema)).select("value.*")
    return formatted_data

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    logger.info("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        age INT,
        phone TEXT,
        location LIST<FLOAT>,
        dangerosity_score INT,
        behaviors LIST<TEXT>,
        picture TEXT);
    """)
    logger.info("Table created successfully!")

def write_streaming_to_cassandra_and_trigger_alert(data, _):
    cass_session = create_cassandra_connection()
    create_keyspace(cass_session)
    create_table(cass_session)
    (data.write.format("org.apache.spark.sql.cassandra")
                .mode("append") \
                .option('checkpointLocation', '/tmp/checkpoint')
                .option('keyspace', 'spark_streams')
                .option('table', 'users')
                .save())
    
    filter_condition = "dangerosity_score > " + os.environ.get("THRESHOLD_DANGEROSITY_SCORE")

    data.filter(filter_condition).show()

if __name__ == "__main__":
    spark_conn = createSparkSession()
    kafka_df = get_kafka_stream(spark_conn)
    formatted_data = format_kafka_data(kafka_df)
    query = formatted_data.writeStream \
    .foreachBatch(write_streaming_to_cassandra_and_trigger_alert) \
    .outputMode("update") \
    .start()

    query.awaitTermination()

