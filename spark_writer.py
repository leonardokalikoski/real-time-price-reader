from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from dotenv import load_dotenv
import os
import yaml

load_dotenv()
PASSWORD = os.getenv('password')
print('Environment Variables Loaded')

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

SPARK_KAFKA_PACKAGES = config["spark"]["spark_kafka_jar"]
KAFKA_BOOTSTRAP_SERVER = config["kafka"]["bootstrap_server"]
POSTGRES_PACKAGES = config["postgres"]["postgres_jar"]
POSTGRES_SERVER = config["postgres"]["server"]
POSTGRES_DB = config["postgres"]["database"]
POSTGRES_TABLE = config["postgres"]["table"]
print('Yaml Configs Loaded')

class Spark:
    def __init__(self):
        ''' Initializes spark session with kafka and postgres packages '''
        self.spark = SparkSession.builder \
            .appName("KafkaSparkPostgresIntegration") \
            .config("spark.jars.packages", SPARK_KAFKA_PACKAGES) \
            .config("spark.jars", POSTGRES_PACKAGES) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
            .getOrCreate()


    def read_stream(self):
        ''' Reads stream from kafka local server '''
        stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
            .option("startingOffsets", "earliest") \
            .option("subscribe", "real-time-reader") \
            .load()
        return stream


    def process_stream(self):
        # Define column types of dataframe
        schema = StructType([
            StructField("Symbol", StringType(), False),
            StructField("Price", DoubleType(), False),
            StructField("Date", TimestampType(), False)
        ])

        kafka_stream = self.read_stream()

        # Convert Kafka binary message into a DataFrame
        df = kafka_stream.selectExpr("CAST(value AS STRING)")

        # Parse JSON into columns based on the schema above
        df = df.select(from_json(col("value"), schema).alias("data"))

        # Expand Json into columns
        df = df.select("data.*")
        return df


    def postgres_write_config(self, batch_df, batch_id):
        batch_df.show(20)
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_SERVER}/{POSTGRES_DB}") \
                .option("dbtable", POSTGRES_TABLE) \
                .option("user", "postgres") \
                .option("password", PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error while writing to PostgreSQL: {e}")


    
    def write_to_postgres(self, df):
        # Define the stream to write data to PostgreSQL
        query_postgres = df.writeStream \
            .foreachBatch(self.postgres_write_config) \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # Keeps the stream running
        query_postgres.awaitTermination()
        
def main():
    spark_instance = Spark()
    df = spark_instance.process_stream()
    spark_instance.write_to_postgres(df)


if __name__ == '__main__':
    main()
