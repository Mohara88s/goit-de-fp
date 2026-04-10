from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re


# Створюємо SparkSession
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Функція для очищення текстових колонок 
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

# Проходимо по таблицям
tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    df = spark.read.parquet(f"/tmp/bronze/{table}")

    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    df = df.dropDuplicates()

    output_path = f"/tmp/silver/{table}"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

# Завершуємо сесію Spark
spark.stop()