from pyspark.sql import SparkSession
import requests

# Для завантаження із ftp-сервера використовуємо функцію
def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

# Створюємо SparkSession
spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

# Проходимо по таблицям
tables = ["athlete_bio", "athlete_event_results"]
for table in tables:
    local_path = f"{table}.csv"
    download_data(table)

    df = spark.read.csv(local_path, header=True, inferSchema=True)

    output_path = f"/tmp/bronze/{table}"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

# Завершуємо сесію Spark
spark.stop()