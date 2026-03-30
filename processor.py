from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import os


# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table_athlete_bio = "athlete_bio"
jdbc_table_athlete_event_results = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# # Створення Spark сесії
spark = (SparkSession.builder 
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") 
    .appName("JDBCToKafka") 
    .getOrCreate())

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio
table_athlete_bio = (spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
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

