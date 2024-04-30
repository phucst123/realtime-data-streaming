import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    
    session.execute("""INSERT INTO spark_streams.created_users(
                                id, 
                                first_name, 
                                last_name, 
                                gender, 
                                address, 
                                post_code, 
                                email, 
                                username, 
                                dob, 
                                registered_date, 
                                phone, 
                                picture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", kwargs.values())

    logging.info(f'Data inserted succesfully to cassandra')



def get_cassandra_session():
    session = Cluster(["cassandra"]).connect()

    if session is not None:
        create_keyspace(session=session)
        create_table(session=session)
    
    return session

def main():
    logging.basicConfig(level=logging.INFO)

    spark = (SparkSession.builder 
            .appName('SparkDataStreaming')
            .config('spark.cassandra.connection.host', "cassandra") 
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            .getOrCreate())
    
    kafka_df = (spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', 'users_created')
                .option('startingOffsets', 'earliest')
                .load())

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    kafka_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as value")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    

    cassandra_query = (kafka_df.writeStream
                            .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(
                                lambda row: insert_data(get_cassandra_session(), **row.asDict())))
                            .start()
                            .awaitTermination())


if __name__ == "__main__":
    main()