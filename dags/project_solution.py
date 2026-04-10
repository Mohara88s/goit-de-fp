from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Визначаємо базовий шлях до файлів в DAG (якщо змінна оточення не задана, використовуємо за замовчуванням)
BASE_PATH = os.getenv("BASE_PATH", "/opt/airflow/dags")

# Визначення DAG
default_args = {
    "owner": "airflow",
    'start_date': datetime(2026, 4, 9, 0, 0),
}

# Створюємо об'єкт DAG
dag = DAG(
    "vitalii_vasylets_dag",
    default_args=default_args,  
    description="goit-de-fp DAG", 
    schedule=None,  
    tags=["vitalii_vasylets"],
)

# Завдання для виконання скрипту landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id="landing_to_bronze",  
    bash_command=f"python3 {BASE_PATH}/landing_to_bronze.py",  
    dag=dag, 
)

# Завдання для виконання скрипту bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=f"python3 {BASE_PATH}/bronze_to_silver.py",  
    dag=dag,  
)

# Завдання для виконання скрипту silver_to_gold.py
silver_to_gold = BashOperator(
    task_id="silver_to_gold", 
    bash_command=f"python3 {BASE_PATH}/silver_to_gold.py",  
    dag=dag,  
)

# Визначаємо порядок виконання завдань
landing_to_bronze >> bronze_to_silver >> silver_to_gold

