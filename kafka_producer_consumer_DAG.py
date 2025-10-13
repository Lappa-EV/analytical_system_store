# dags/kafka_producer_consumer.py
"""
Этот DAG запускает задачи Kafka producer и consumer.
Задача producer извлекает данные из MongoDB, преобразует их и отправляет в Kafka topic.
Задача consumer получает сообщения из Kafka topic, преобразует их и загружает в ClickHouse.
Задачи выполняются последовательно, начиная с producer, каждый понедельник или по ручному запуску.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Функция для запуска внешнего Python-скрипта
def run_script(script_path):
    def execute():
        process = subprocess.Popen(["python", script_path],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise Exception(f"Script {script_path} failed with error: {stderr.decode()}")

        print(stdout.decode()) # Вывод логов в Airflow

    return execute

with DAG(
    dag_id="kafka_producer_consumer_DAG",
    start_date=datetime(2025, 10, 9),
    schedule_interval="0 0 * * 1", # Запускать каждый понедельник в 00:00 + возможность ручного запуска
    catchup=False,
    tags=['kafka', 'mongodb', 'clickhouse']
) as dag:
    # Задача запуска Kafka producer
    producer_task = PythonOperator(
        task_id="kafka_producer",
        python_callable=run_script("/mongodb_kafka_clickhouse/producer.py")
    )

    # Задача запуска Kafka consumer
    consumer_task = PythonOperator(
        task_id="kafka_consumer",
        python_callable=run_script("/mongodb_kafka_clickhouse/consumer.py")
    )

    # Определение порядка выполнения задач: producer -> consumer
    producer_task >> consumer_task
