from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, BooleanType
from pyspark.sql import SparkSession
import pyspark
from configs import kafka_config
import os

# Змушую використовувати PySpark з мого venv
os.environ['SPARK_HOME'] = os.path.abspath("./venv/lib/python3.12/site-packages/pyspark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# # Створення Spark сесії
spark = (SparkSession.builder 
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
    .appName("JDBCToKafka") 
    .getOrCreate())


# Визначення схеми для JSON
schema = StructType(
    [
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("noc_country", StringType(), True),
        StructField("avg_height", StringType(), True),
        StructField("avg_weight", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Читання потокових даних із Kafka
kafka_streaming_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("subscribe", "vitalii_vasylets_athlete_enriched_agg")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "50")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Виведення до консолі
(kafka_streaming_df.writeStream
    .trigger(availableNow=True)
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination())