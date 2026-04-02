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

# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table_athlete_bio = "athlete_bio"
jdbc_table_athlete_event_results = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# # Створення Spark сесії
spark = (SparkSession.builder 
    .config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
    .appName("JDBCToKafka") 
    .getOrCreate())

print(spark.version)
print(pyspark.__version__)

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio
table_athlete_bio = (spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=jdbc_table_athlete_bio,
    user=jdbc_user,
    password=jdbc_password)
    .load())

table_athlete_bio.limit(10).show()
print(table_athlete_bio.count())

# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами
table_athlete_bio = table_athlete_bio.withColumn(
    "height", expr("try_cast(height as double)")
).withColumn(
    "weight", expr("try_cast(weight as double)")
).filter(
    col("height").isNotNull() & col("weight").isNotNull()
)

table_athlete_bio.limit(10).show()
print(table_athlete_bio.count())

# 3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results.
# Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results.
# Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.

table_athlete_event_results = (spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=jdbc_table_athlete_event_results,
    user=jdbc_user,
    password=jdbc_password)
    .load())

table_athlete_event_results.limit(10).show()
print(table_athlete_event_results.count())

# Визначення схеми для JSON

schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True),
])

#  Перетворення в JSON
df_kafka = table_athlete_event_results.selectExpr(
    "CAST(result_id AS STRING) AS key",
    "to_json(struct(*)) AS value"
)

# Запис у Kafka
(df_kafka.write
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config", kafka_config['sasl_jaas_config'])
    .option("topic", "vitalii_vasylets_athlete_event_results")
    .option("checkpointLocation", "/tmp/checkpoint2")
    .save())

# Читання з Kafka
kafka_streaming_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("subscribe", "vitalii_vasylets_athlete_event_results")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "50")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL 
# таблиці за допомогою ключа athlete_id.
expanded_table = kafka_streaming_df.drop("country_noc").join(table_athlete_bio, "athlete_id")

# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності,
#  статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.
aggregated_df = expanded_table.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
# а) вихідний кафка-топік,
# b) базу даних.

# Функція для обробки кожної партії даних
def foreach_batch_function(batch_df, batch_id):
    #  Перетворення в JSON
    kafka_payload = batch_df.selectExpr(
        "CAST(NULL AS STRING) AS key", 
        "to_json(struct(*)) AS value"
    )

    # Відправка збагачених даних до Kafka
    (kafka_payload
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
        .option("kafka.security.protocol", kafka_config['security_protocol'])
        .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
        .option("kafka.sasl.jaas.config", kafka_config['sasl_jaas_config'])
        .option("topic", "vitalii_vasylets_athlete_enriched_agg")
        .option("checkpointLocation", "/tmp/checkpoint3")
        .save())

    # Збереження збагачених даних до MySQL
    (batch_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "vitalii_vasylets_athlete_enriched_agg")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .mode("append")
        .save())

# Налаштування потоку даних для обробки кожної партії за допомогою вказаної функції
(aggregated_df
    .writeStream
    .foreachBatch(foreach_batch_function)
    .outputMode("update")
    .start()
    .awaitTermination())
