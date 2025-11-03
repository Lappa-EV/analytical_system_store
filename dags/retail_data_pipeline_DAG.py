# dags/retail_data_pipeline_DAG.py
"""
DAG для обработки данных ритейла:
1. Извлечение данных из MongoDB
2. Отправка данных в Kafka
3. Чтение данных из Kafka
4. Загрузка данных в ClickHouse
5. Создание и заполнение MART-таблиц в ClickHouse
6. Создание витрины признаков клиентов с помощью Spark
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator



# Импортируем задачи
from tasks.producer_task import run_producer
from tasks.consumer_task import run_consumer
from tasks.feature_matrix_creator_task import create_feature_matrix
from tasks.clickhouse_mart_tasks import (
    addresses,
    categories,
    manufacturers,
    store_networks,
    store_managers,
    dim_customers,
    dim_products,
    dim_stores,
    store_categories,
    fact_purchases,
    fact_purchase_items,
    duplicate_analysis_results
)

# Параметры DAG по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
dag = DAG(
    'retail_data_pipeline_DAG',
    default_args=default_args,
    description='Конвейер данных для ритейла',
    schedule_interval='0 7 * * *',
    catchup=False,
)

# 1. Отправка данных в Kafka из MongoDB
task_producer = PythonOperator(
    task_id='kafka_producer_task',
    python_callable=run_producer,
    op_kwargs={
        'kafka_broker': Variable.get('KAFKA_BROKER'),
        'mongo_uri': Variable.get('MONGO_URI'),
        'mongo_db': Variable.get('MONGO_DATABASE'),
        # Используются значения по умолчанию для topics и sensitive_topics
    },
    dag=dag,
)

# 2. Получение данных из Kafka и загрузка их в ClickHouse
task_consumer = PythonOperator(
    task_id='kafka_consumer_task',
    python_callable=run_consumer,
    op_kwargs={
        'clickhouse_host': Variable.get('CLICKHOUSE_HOST'),
        'clickhouse_port': Variable.get('CLICKHOUSE_PORT'),
        'clickhouse_user': Variable.get('CLICKHOUSE_USER'),
        'clickhouse_password': Variable.get('CLICKHOUSE_PASSWORD'),
        'clickhouse_db': Variable.get('CLICKHOUSE_DB'),
        'kafka_broker': Variable.get('KAFKA_BROKER'),
        'kafka_group': Variable.get('KAFKA_GROUP'),
        'kafka_topics': Variable.get('KAFKA_TOPICS'),
    },
    dag=dag,
)

# 3. Создание MART-таблиц начинается с Dummy оператора
start_mart_tasks = DummyOperator(
    task_id='start_mart_tasks',
    dag=dag,
)

# 4. Создание и заполнение справочников и MART-таблиц в ClickHouse
clickhouse_conn_id = 'clickhouse_conn'

# Создаем каждую задачу для мартов
task_addresses = PythonOperator(
    task_id='create_addresses',
    python_callable=addresses,
    dag=dag,
)

task_categories = PythonOperator(
    task_id='create_categories',
    python_callable=categories,
    dag=dag,
)

task_manufacturers = PythonOperator(
    task_id='create_manufacturers',
    python_callable=manufacturers,
    dag=dag,
)

task_store_networks = PythonOperator(
    task_id='create_store_networks',
    python_callable=store_networks,
    dag=dag,
)

task_store_managers = PythonOperator(
    task_id='create_store_managers',
    python_callable=store_managers,
    dag=dag,
)

# Создание таблиц dim_
task_dim_customers = PythonOperator(
    task_id='create_dim_customers',
    python_callable=dim_customers,
    dag=dag,
)

task_dim_products = PythonOperator(
    task_id='create_dim_products',
    python_callable=dim_products,
    dag=dag,
)

task_dim_stores = PythonOperator(
    task_id='create_dim_stores',
    python_callable=dim_stores,
    dag=dag,
)

task_store_categories = PythonOperator(
    task_id='create_store_categories',
    python_callable=store_categories,
    dag=dag,
)

# Создание таблиц fact_
task_fact_purchases = PythonOperator(
    task_id='create_fact_purchases',
    python_callable=fact_purchases,
    dag=dag,
)

task_fact_purchase_items = PythonOperator(
    task_id='create_fact_purchase_items',
    python_callable=fact_purchase_items,
    dag=dag,
)

# Анализ дубликатов
task_duplicate_analysis = PythonOperator(
    task_id='duplicate_analysis',
    python_callable=duplicate_analysis_results,
    dag=dag,
)

# 5. Завершение MART задач с Dummy оператором
end_mart_tasks = DummyOperator(
    task_id='end_mart_tasks',
    dag=dag,
)

# 6. Создание витрины признаков с помощью Spark
task_feature_matrix_creator = PythonOperator(
    task_id='create_feature_matrix',
    python_callable=create_feature_matrix,
    op_kwargs={
        'clickhouse_host': Variable.get('CLICKHOUSE_HOST'),
        'clickhouse_port_jdbc': Variable.get('CLICKHOUSE_PORT_JDBC'),
        'clickhouse_user': Variable.get('CLICKHOUSE_USER'),
        'clickhouse_password': Variable.get('CLICKHOUSE_PASSWORD'),
        'clickhouse_db': Variable.get('CLICKHOUSE_DB'),
        'clickhouse_jar_path': Variable.get("CLICKHOUSE_JAR_PATH"),
        's3_endpoint': Variable.get('S3_ENDPOINT'),
        's3_access_key': Variable.get('S3_KEY_ID'),
        's3_secret_key': Variable.get('S3_SECRET'),
        's3_container': Variable.get('S3_BUCKET')
    },
    dag=dag,
)

# Определяем зависимости между задачами
task_producer >> task_consumer >> start_mart_tasks

# Зависимости для справочников
start_mart_tasks >> [
    task_addresses,
    task_categories,
    task_manufacturers,
    task_store_networks,
    task_store_managers
]

# Зависимости для dim-таблиц (требуют справочников)
task_addresses >> task_dim_customers
task_addresses >> task_dim_stores
task_categories >> task_dim_products
task_manufacturers >> task_dim_products
task_store_networks >> task_dim_stores
task_store_managers >> task_dim_stores

# Зависимости для store_categories и fact-таблиц
task_dim_stores >> task_store_categories
task_categories >> task_store_categories
task_dim_stores >> task_fact_purchases
task_addresses >> task_fact_purchases
task_dim_products >> task_fact_purchase_items

# Анализ дубликатов после загрузки всех данных
[
    task_dim_customers,
    task_dim_products,
    task_dim_stores,
    task_store_categories,
    task_fact_purchases,
    task_fact_purchase_items
] >> task_duplicate_analysis >> end_mart_tasks

# Запускаем создание витрины после завершения всех задач с маркетами
end_mart_tasks >> task_feature_matrix_creator
