from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, regexp_replace, expr

# Створюємо SparkSession
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Читаємо дані з паркетних файлів для таблиць "athlete_bio" та "athlete_event_results"
table_athlete_bio = spark.read.parquet("/tmp/silver/athlete_bio")
table_athlete_event_results = spark.read.parquet(
    "/tmp/silver/athlete_event_results")

# Глибоке очищення даних
table_athlete_bio = table_athlete_bio.withColumn(
    "height",
    expr("try_cast(regexp_replace(height, ',', '.') as double)")
).withColumn(
    "weight",
    expr("try_cast(regexp_replace(weight, ',', '.') as double)")
)

# Виконуємо об'єднання двох DataFrame за стовпцем "athlete_id"
expanded_table = table_athlete_event_results.drop(
    "country_noc").join(table_athlete_bio, "athlete_id")

# Агрегуємо дані: обчислюємо середні значення для зросту та ваги, додаємо поточний час
aggregated_df = expanded_table.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

# Визначаємо шлях для збереження результатів у форматі Parquet
output_path = "/tmp/gold/avg_stats"

# Записуємо агреговані дані у форматі Parquet, перезаписуючи існуючі файли
aggregated_df.write.mode("overwrite").parquet(output_path)

# Виводимо повідомлення про успішне збереження даних
print(f"Data saved to {output_path}")

# Читаємо збережені дані і виводимо їх
df = spark.read.parquet(output_path)
df.show(truncate=False)

# Завершуємо сесію Spark
spark.stop()
