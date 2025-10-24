# dags/tasks/feature_matrix_creator_task.py
"""
Скрипт для создания витрины с признаками пользователей.
Этот скрипт вызывается как Python-задача в Airflow.
"""

from pyspark.sql import SparkSession
from datetime import datetime
import boto3
from botocore.client import Config
import os
import glob
import logging
import time
from airflow.exceptions import AirflowException

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Максимальное количество попыток подключения
MAX_RETRIES = 3
# Время задержки между попытками (в секундах)
RETRY_DELAY = 5


def create_feature_matrix(**kwargs):
    """
    Создание витрины с признаками пользователей, используя Apache Spark.
    Извлекает данные из ClickHouse, формирует признаки и загружает результат в S3.

    Args:
        **kwargs: Аргументы, переданные из Airflow.
            clickhouse_host: Хост ClickHouse
            clickhouse_port_jdbc: Порт ClickHouse
            clickhouse_user: Пользователь ClickHouse
            clickhouse_password: Пароль ClickHouse
            clickhouse_db: База данных ClickHouse
            s3_endpoint: Endpoint для S3
            s3_access_key: Access Key для S3
            s3_secret_key: Secret Key для S3
            s3_container: Имя бакета S3
    """
    spark = None
    try:
        # Инициализируем переменные подключения из kwargs
        CLICKHOUSE_HOST = kwargs.get('clickhouse_host')
        CLICKHOUSE_PORT_JDBC = kwargs.get('clickhouse_port_jdbc')
        CLICKHOUSE_USER = kwargs.get('clickhouse_user')
        CLICKHOUSE_PASSWORD = kwargs.get('clickhouse_password')
        CLICKHOUSE_DB = kwargs.get('clickhouse_db')
        CLICKHOUSE_JAR_PATH = kwargs.get('clickhouse_jar_path')

        # Параметры для S3
        ENDPOINT = kwargs.get('s3_endpoint')
        KEY_ID = kwargs.get('s3_access_key')
        SECRET = kwargs.get('s3_secret_key')
        CONTAINER = kwargs.get('s3_container')

        # Формируем строку подключения к ClickHouse с дополнительными параметрами
        CLICKHOUSE_JDBC_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_JDBC}/{CLICKHOUSE_DB}"

        # Создаем настройки подключения к ClickHouse
        CLICKHOUSE_PROPERTIES = {
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "socket_timeout": "3600",
            "connect_timeout": "300",
            "retry_count": "3",
            "retry_delay": "1"
        }

        # Проверяем наличие JAR файла
        if not os.path.exists(CLICKHOUSE_JAR_PATH):
            raise AirflowException(f"JAR файл не найден по пути: {CLICKHOUSE_JAR_PATH}")

        logging.info(f"Используется JAR файл: {CLICKHOUSE_JAR_PATH}")

        # Инициализация SparkSession
        logging.info("Инициализация SparkSession...")
        spark = SparkSession.builder \
            .appName("ClickHouse Feature Matrix Creator") \
            .config("spark.jars", CLICKHOUSE_JAR_PATH) \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "120s") \
            .getOrCreate()
        logging.info("SparkSession инициализирована успешно")


        def try_connect_to_clickhouse():
            """ Функция для попытки подключения к БД с повторами """

            retry_count = 0
            last_exception = None

            while retry_count < MAX_RETRIES:
                try:
                    logging.info(f"Попытка подключения к ClickHouse ({retry_count + 1}/{MAX_RETRIES})...")
                    result = spark.read.jdbc(
                        url=CLICKHOUSE_JDBC_URL,
                        table="system.one",
                        properties=CLICKHOUSE_PROPERTIES
                    ).limit(1).count()

                    logging.info("Успешное подключение к ClickHouse!")
                    return True
                except Exception as e:
                    last_exception = e
                    logging.warning(f"Не удалось подключиться к ClickHouse: {e}")
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        logging.info(f"Ожидание {RETRY_DELAY} сек перед повторной попыткой...")
                        time.sleep(RETRY_DELAY)

            if last_exception:
                raise AirflowException(
                    f"Не удалось подключиться к ClickHouse после {MAX_RETRIES} попыток: {last_exception}")

            return False

        # Пытаемся подключиться к ClickHouse
        try_connect_to_clickhouse()


        def execute_clickhouse_query(query, max_retries=3):
            """ Функция для выполнения прямых запросов к ClickHouse """

            query_table = f"({query}) as query_result"
            retry_count = 0
            last_exception = None

            while retry_count < max_retries:
                try:
                    logging.debug(f"Выполнение запроса к ClickHouse (попытка {retry_count + 1}/{max_retries})")
                    df = spark.read.jdbc(
                        url=CLICKHOUSE_JDBC_URL,
                        table=query_table,
                        properties=CLICKHOUSE_PROPERTIES
                    )
                    # Переименовываем столбцы, если необходимо
                    if 'cu.customer_id' in df.columns:
                        df = df.withColumnRenamed('cu.customer_id', 'customer_id')
                    elif 'c.customer_id' in df.columns:
                        df = df.withColumnRenamed('c.customer_id', 'customer_id')
                    return df
                except Exception as e:
                    last_exception = e
                    logging.warning(f"Ошибка при выполнении запроса (попытка {retry_count + 1}/{max_retries}): {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(RETRY_DELAY)

            if last_exception:
                raise AirflowException(f"Не удалось выполнить запрос после {max_retries} попыток: {last_exception}")

        # Создаем базовый DataFrame
        logging.info("Создание базового DataFrame с информацией о клиентах...")
        base_query = """
        SELECT
            customer_id,
            first_name,
            last_name,
            loyalty_card_number
        FROM dim_customers
        """
        base_df = execute_clickhouse_query(base_query)
        logging.info(f"Базовый DataFrame создан, количество записей: {base_df.count()}")

        # 1. bought_milk_last_30d - Покупал молочные продукты за последние 30 дней
        logging.info("Вычисление признака: bought_milk_last_30d...")
        bought_milk_last_30d = """
        SELECT
            cu.customer_id AS customer_id,
            CAST(MAX(c.category_name = 'молочные продукты' AND i.purchase_datetime BETWEEN NOW() - INTERVAL 30 DAY AND NOW()) AS INT) AS bought_milk_last_30d
        FROM dim_customers cu
        LEFT JOIN fact_purchases pu ON cu.customer_id = pu.customer_id
        LEFT JOIN fact_purchase_items i ON pu.purchase_id = i.purchase_id
        LEFT JOIN dim_products pr ON i.product_id = pr.product_id
        LEFT JOIN categories c ON pr.category_id = c.category_id
        GROUP BY cu.customer_id
        """
        bought_milk_last_30d_df = execute_clickhouse_query(bought_milk_last_30d)

        # 2. bought_fruits_last_14d - Покупал фрукты и ягоды за последние 14 дней
        logging.info("Вычисление признака: bought_fruits_last_14d...")
        bought_fruits_last_14d = """
        SELECT
            cu.customer_id AS customer_id,
            CAST(MAX(c.category_name = 'фрукты и ягоды' AND i.purchase_datetime BETWEEN NOW() - INTERVAL 14 DAY AND NOW()) AS INT) AS bought_fruits_last_14d
        FROM dim_customers cu
        LEFT JOIN fact_purchases pu ON cu.customer_id = pu.customer_id
        LEFT JOIN fact_purchase_items i ON pu.purchase_id = i.purchase_id
        LEFT JOIN dim_products pr ON i.product_id = pr.product_id
        LEFT JOIN categories c ON pr.category_id = c.category_id
        GROUP BY cu.customer_id
        """
        bought_fruits_last_14d_df = execute_clickhouse_query(bought_fruits_last_14d)

        # 3. not_bought_veggies_14d - Не покупал овощи и зелень за последние 14 дней
        logging.info("Вычисление признака: not_bought_veggies_14d...")
        not_bought_veggies_14d = """
        SELECT
            cu.customer_id AS customer_id,
            CAST(NOT MAX(c.category_name = 'овощи и зелень' AND i.purchase_datetime >= NOW() - INTERVAL 14 DAY) AS INT) AS not_bought_veggies_14d
        FROM dim_customers cu
        LEFT JOIN fact_purchases pu ON cu.customer_id = pu.customer_id
        LEFT JOIN fact_purchase_items i ON pu.purchase_id = i.purchase_id
        LEFT JOIN dim_products pr ON i.product_id = pr.product_id
        LEFT JOIN categories c ON pr.category_id = c.category_id
        GROUP BY cu.customer_id
        """
        not_bought_veggies_14d_df = execute_clickhouse_query(not_bought_veggies_14d)

        # 4. recurrent_buyer - Делал более 2 покупок за последние 30 дней
        logging.info("Вычисление признака: recurrent_buyer...")
        recurrent_buyer = """
        SELECT
            cu.customer_id AS customer_id,
            CAST(COUNT(CASE
                WHEN pu.purchase_datetime BETWEEN NOW() - INTERVAL 30 DAY AND NOW()
                THEN pu.purchase_id ELSE NULL END) > 2 AS INT) AS recurrent_buyer
        FROM dim_customers cu
        LEFT JOIN fact_purchases pu ON cu.customer_id = pu.customer_id
        GROUP BY cu.customer_id
        """
        recurrent_buyer_df = execute_clickhouse_query(recurrent_buyer)

        # 5. inactive_14_30 - Не покупал 14–30 дней (ушедший клиент?)
        logging.info("Вычисление признака: inactive_14_30...")
        inactive_14_30 = """
        SELECT
            cu.customer_id AS customer_id,
            CAST(COUNT(CASE WHEN pu.purchase_datetime BETWEEN NOW() - INTERVAL 30 DAY AND NOW() - INTERVAL 14 DAY THEN 1 END) = 0 AS INT) AS inactive_14_30
        FROM dim_customers cu
        LEFT JOIN fact_purchases pu ON cu.customer_id = pu.customer_id
        GROUP BY cu.customer_id
        """
        inactive_14_30_df = execute_clickhouse_query(inactive_14_30)

        # 6. new_customer - Покупатель зарегистрировался менее 30 дней назад
        logging.info("Вычисление признака: new_customer...")
        new_customer = """
        SELECT
            customer_id,
            CASE 
                WHEN registration_date BETWEEN NOW() - INTERVAL 30 DAY AND NOW() 
                THEN 1 
                ELSE 0 
            END AS new_customer
        FROM dim_customers
        """
        new_customer_df = execute_clickhouse_query(new_customer)

        # 7. delivery_user - Пользовался доставкой хотя бы раз
        logging.info("Вычисление признака: delivery_user...")
        delivery_user = """
        SELECT
            customer_id,
            CAST(MAX(is_delivery = true) AS INT) AS delivery_user
        FROM fact_purchases
        GROUP BY customer_id
        """
        delivery_user_df = execute_clickhouse_query(delivery_user)

        # 8. organic_preference - Купил хотя бы 1 органический продукт
        logging.info("Вычисление признака: organic_preference...")
        organic_preference = """
        SELECT
            fp.customer_id AS customer_id,
            CAST(MAX(dp.is_organic = true) AS INT) AS organic_preference
        FROM fact_purchases fp
        LEFT JOIN fact_purchase_items fpi ON fp.purchase_id=fpi.purchase_id
        LEFT JOIN dim_products dp ON fpi.product_id=dp.product_id
        GROUP BY fp.customer_id
        """
        organic_preference_df = execute_clickhouse_query(organic_preference)

        # 9. bulk_buyer - Средняя корзина > 1000₽
        logging.info("Вычисление признака: bulk_buyer...")
        bulk_buyer = """
        SELECT
            customer_id,
            CAST(AVG(total_amount) > 1000 AS INT) AS bulk_buyer
        FROM fact_purchases
        GROUP BY customer_id
        """
        bulk_buyer_df = execute_clickhouse_query(bulk_buyer)

        # 10. low_cost_buyer - Средняя корзина < 200₽
        logging.info("Вычисление признака: low_cost_buyer...")
        low_cost_buyer = """
        SELECT
            customer_id,
            CAST(AVG(total_amount) < 200 AS INT) AS low_cost_buyer
        FROM fact_purchases
        GROUP BY customer_id
        """
        low_cost_buyer_df = execute_clickhouse_query(low_cost_buyer)

        # 11. buys_bakery - Покупал хлеб/выпечку хотя бы раз
        logging.info("Вычисление признака: buys_bakery...")
        buys_bakery = """
        SELECT
            cu.customer_id AS customer_id,
            CAST(MAX(c.category_name = 'зерновые и хлебобулочные изделия') AS INT) AS buys_bakery
        FROM dim_customers cu
        LEFT JOIN fact_purchases fp ON cu.customer_id = fp.customer_id
        LEFT JOIN fact_purchase_items fpi ON fp.purchase_id = fpi.purchase_id
        LEFT JOIN dim_products dp ON fpi.product_id = dp.product_id
        LEFT JOIN categories c ON dp.category_id = c.category_id
        GROUP BY cu.customer_id
        """
        buys_bakery_df = execute_clickhouse_query(buys_bakery)

        # 12. loyal_customer - Лояльный клиент (карта и ≥3 покупки)
        logging.info("Вычисление признака: loyal_customer...")
        loyal_customer = """
        SELECT
            c.customer_id AS customer_id,
            CAST(c.is_loyalty_member AND COUNT(DISTINCT fp.purchase_id) >= 3 AS INT) AS loyal_customer
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id, c.is_loyalty_member
        """
        loyal_customer_df = execute_clickhouse_query(loyal_customer)

        # 13. multicity_buyer - Делал покупки в разных городах
        logging.info("Вычисление признака: multicity_buyer...")
        multicity_buyer = """
        SELECT 
            c.customer_id AS customer_id,
            CAST(COUNT(DISTINCT a.city) > 1 AS INT) AS multicity_buyer
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        LEFT JOIN dim_stores s ON fp.store_id = s.store_id
        LEFT JOIN addresses a ON s.address_id = a.address_id
        GROUP BY c.customer_id
        """
        multicity_buyer_df = execute_clickhouse_query(multicity_buyer)

        # 14. bought_meat_last_week - Покупал мясо/рыбу/яйца за последнюю неделю
        logging.info("Вычисление признака: bought_meat_last_week...")
        bought_meat_last_week = """
        SELECT
            c.customer_id AS customer_id,
            CAST(MAX(CASE WHEN ct.category_name IN ('мясо, рыба, яйца и бобовые') 
                     AND fp.purchase_datetime >= NOW() - INTERVAL 1 WEEK THEN 1 ELSE 0 END) AS INT) AS bought_meat_last_week
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        LEFT JOIN fact_purchase_items fpi ON fp.purchase_id = fpi.purchase_id
        LEFT JOIN dim_products p ON fpi.product_id = p.product_id
        LEFT JOIN categories ct ON p.category_id = ct.category_id
        GROUP BY c.customer_id
        """
        bought_meat_last_week_df = execute_clickhouse_query(bought_meat_last_week)

        # 15. night_shopper - Делал покупки после 20:00
        logging.info("Вычисление признака: night_shopper...")
        night_shopper = """
        SELECT
            c.customer_id AS customer_id,
            CAST(MAX(CASE WHEN HOUR(fp.purchase_datetime) > 20 THEN 1 ELSE 0 END) AS INT) AS night_shopper
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        night_shopper_df = execute_clickhouse_query(night_shopper)

        # 16. morning_shopper - Делал покупки до 10:00
        logging.info("Вычисление признака: morning_shopper...")
        morning_shopper = """
        SELECT
            c.customer_id AS customer_id,
            CAST(MAX(CASE WHEN HOUR(fp.purchase_datetime) < 10 THEN 1 ELSE 0 END) AS INT) AS morning_shopper
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        morning_shopper_df = execute_clickhouse_query(morning_shopper)

        # 17. prefers_cash - Оплачивал наличными ≥ 70% покупок
        logging.info("Вычисление признака: prefers_cash...")
        prefers_cash = """
        SELECT 
            c.customer_id AS customer_id,
            CAST(COUNT(CASE WHEN fp.payment_method = 'cash' THEN 1 END) / NULLIF(COUNT(fp.purchase_id), 0) >= 0.7 AS INT) AS prefers_cash
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        prefers_cash_df = execute_clickhouse_query(prefers_cash)

        # 18. prefers_card - Оплачивал картой ≥ 70% покупок
        logging.info("Вычисление признака: prefers_card...")
        prefers_card = """
        SELECT 
            c.customer_id AS customer_id,
            CAST(COUNT(CASE WHEN fp.payment_method = 'card' THEN 1 END) / NULLIF(COUNT(fp.purchase_id), 0) >= 0.7 AS INT) AS prefers_card
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        prefers_card_df = execute_clickhouse_query(prefers_card)

        # 19. weekend_shopper - Делал ≥ 60% покупок в выходные
        logging.info("Вычисление признака: weekend_shopper...")
        weekend_shopper = """
        SELECT
            c.customer_id AS customer_id,
            CAST(SUM(CASE WHEN DAYOFWEEK(fp.purchase_datetime) IN (1, 7) THEN 1 ELSE 0 END) / NULLIF(COUNT(fp.purchase_id), 0) >= 0.6 AS INT) AS weekend_shopper
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        weekend_shopper_df = execute_clickhouse_query(weekend_shopper)

        # 20. weekday_shopper - Делал ≥ 60% покупок в будни
        logging.info("Вычисление признака: weekday_shopper...")
        weekday_shopper = """
        SELECT
            c.customer_id AS customer_id,
            CAST(SUM(CASE WHEN DAYOFWEEK(fp.purchase_datetime) BETWEEN 2 AND 6 THEN 1 ELSE 0 END) / NULLIF(COUNT(fp.purchase_id), 0) >= 0.6 AS INT) AS weekday_shopper
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        weekday_shopper_df = execute_clickhouse_query(weekday_shopper)

        # 21. single_item_buyer - ≥50% покупок — 1 товар в корзине
        logging.info("Вычисление признака: single_item_buyer...")
        single_item_buyer = """
        SELECT
            c.customer_id AS customer_id,
            CAST(SUM(item_counts.is_single) * 1.0 / NULLIF(COUNT(fp.purchase_id), 0) >= 0.5 AS INT) AS single_item_buyer
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        LEFT JOIN (
            SELECT
                purchase_id,
                CAST(COUNT(1) = 1 AS INT) AS is_single
            FROM fact_purchase_items
            GROUP BY purchase_id
        ) item_counts ON fp.purchase_id = item_counts.purchase_id
        GROUP BY c.customer_id
        """
        single_item_buyer_df = execute_clickhouse_query(single_item_buyer)

        # 22. varied_shopper - Покупал ≥4 разных категорий продуктов
        logging.info("Вычисление признака: varied_shopper...")
        varied_shopper = """
        SELECT
            c.customer_id AS customer_id,
            CAST(COUNT(DISTINCT p.category_id) >= 4 AS INT) AS varied_shopper
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        LEFT JOIN fact_purchase_items fpi ON fp.purchase_id = fpi.purchase_id
        LEFT JOIN dim_products p ON fpi.product_id = p.product_id
        GROUP BY c.customer_id
        """
        varied_shopper_df = execute_clickhouse_query(varied_shopper)

        # 23. store_loyal - Ходит только в один магазин
        logging.info("Вычисление признака: store_loyal...")
        store_loyal = """
        SELECT
            c.customer_id AS customer_id,
            CAST(COUNT(DISTINCT fp.store_id) = 1 AND COUNT(fp.purchase_id) > 0 AS INT) AS store_loyal
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        store_loyal_df = execute_clickhouse_query(store_loyal)

        # 24. switching_store - Ходит в разные магазины
        logging.info("Вычисление признака: switching_store...")
        switching_store = """
        SELECT
            c.customer_id AS customer_id,
            CAST(COUNT(DISTINCT fp.store_id) > 1 AS INT) AS switching_store
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        switching_store_df = execute_clickhouse_query(switching_store)

        # 25. family_shopper - Среднее кол-во позиций в корзине ≥4
        logging.info("Вычисление признака: family_shopper...")
        family_shopper = """
        SELECT
            c.customer_id AS customer_id,
            CAST(AVG(item_counts.count) >= 4 AND COUNT(fp.purchase_id) > 0 AS INT) AS family_shopper
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        LEFT JOIN (
            SELECT
                purchase_id,
                COUNT(1) AS count
            FROM fact_purchase_items
            GROUP BY purchase_id
        ) item_counts ON fp.purchase_id = item_counts.purchase_id
        GROUP BY c.customer_id
        """
        family_shopper_df = execute_clickhouse_query(family_shopper)

        # 26. early_bird - Покупка в промежутке между 12 и 15 часами дня
        logging.info("Вычисление признака: early_bird...")
        early_bird = """
        SELECT
            c.customer_id AS customer_id,
            CAST(MAX(CASE WHEN HOUR(fp.purchase_datetime) BETWEEN 12 AND 14 THEN 1 ELSE 0 END) AS INT) AS early_bird
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        early_bird_df = execute_clickhouse_query(early_bird)

        # 27. no_purchases - Не совершал ни одной покупки (только регистрация)
        logging.info("Вычисление признака: no_purchases...")
        no_purchases = """
        SELECT
            c.customer_id AS customer_id,
            CAST(COUNT(fp.purchase_id) = 0 AS INT) AS no_purchases
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        no_purchases_df = execute_clickhouse_query(no_purchases)

        # 28. recent_high_spender - Купил на сумму >2000₽ за последние 7 дней
        logging.info("Вычисление признака: recent_high_spender...")
        recent_high_spender = """
        SELECT
            c.customer_id AS customer_id,
            CAST(SUM(CASE WHEN fp.purchase_datetime >= NOW() - INTERVAL 7 DAY THEN fp.total_amount ELSE 0 END) > 2000 AS INT) AS recent_high_spender
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        GROUP BY c.customer_id
        """
        recent_high_spender_df = execute_clickhouse_query(recent_high_spender)

        # 29. fruit_lover - ≥3 покупок фруктов за 30 дней
        logging.info("Вычисление признака: fruit_lover...")
        fruit_lover = """
        SELECT
            c.customer_id AS customer_id,
            CAST(COUNT(DISTINCT
                CASE
                    WHEN ct.category_name = 'фрукты и ягоды' AND fp.purchase_datetime >= NOW() - INTERVAL 30 DAY
                    THEN fp.purchase_id
                    ELSE NULL
                END
            ) >= 3 AS INT) AS fruit_lover
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        LEFT JOIN fact_purchase_items fpi ON fp.purchase_id = fpi.purchase_id
        LEFT JOIN dim_products p ON fpi.product_id = p.product_id
        LEFT JOIN categories ct ON p.category_id = ct.category_id
        GROUP BY c.customer_id
        """
        fruit_lover_df = execute_clickhouse_query(fruit_lover)

        # 30. vegetarian_profile - Не купил ни одного мясного продукта за 90 дней
        logging.info("Вычисление признака: vegetarian_profile...")
        vegetarian_profile = """
        SELECT
            c.customer_id AS customer_id,
            CAST(SUM(CASE WHEN ct.category_name = 'мясо, рыба, яйца и бобовые' AND fp.purchase_datetime >= NOW() - INTERVAL 90 DAY THEN 1 ELSE 0 END) = 0 AS INT) AS vegetarian_profile
        FROM dim_customers c
        LEFT JOIN fact_purchases fp ON c.customer_id = fp.customer_id
        LEFT JOIN fact_purchase_items fpi ON fp.purchase_id = fpi.purchase_id
        LEFT JOIN dim_products p ON fpi.product_id = p.product_id
        LEFT JOIN categories ct ON p.category_id = ct.category_id
        GROUP BY c.customer_id
        """
        vegetarian_profile_df = execute_clickhouse_query(vegetarian_profile)

        # Объединяем все DataFrame в итоговую таблицу
        logging.info("Начало объединения всех DataFrame в итоговую таблицу...")
        feature_matrix_df = base_df
        feature_matrix_df = feature_matrix_df.join(bought_milk_last_30d_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(bought_fruits_last_14d_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(not_bought_veggies_14d_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(recurrent_buyer_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(inactive_14_30_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(new_customer_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(delivery_user_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(organic_preference_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(bulk_buyer_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(low_cost_buyer_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(buys_bakery_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(loyal_customer_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(multicity_buyer_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(bought_meat_last_week_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(night_shopper_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(morning_shopper_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(prefers_cash_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(prefers_card_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(weekend_shopper_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(weekday_shopper_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(single_item_buyer_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(varied_shopper_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(store_loyal_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(switching_store_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(family_shopper_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(early_bird_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(no_purchases_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(recent_high_spender_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(fruit_lover_df, on="customer_id", how="left")
        feature_matrix_df = feature_matrix_df.join(vegetarian_profile_df, on="customer_id", how="left")

        logging.info("Объединение DataFrame завершено")

        # Заполняем null значения нулями
        feature_matrix_df = feature_matrix_df.fillna(0)

        # Формируем имя файла с текущей датой
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_filename = f"feature_matrix_{current_time}.csv"
        temp_output_path = "/tmp/temp_output"

        logging.info(f"Сохранение результатов в CSV файл: {output_filename}")

        # Сохраняем в один файл CSV
        feature_matrix_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)

        # Находим и переименовываем выходной файл
        csv_files = glob.glob(f"{temp_output_path}/part-*.csv")
        if not csv_files:
            raise AirflowException("Ошибка: файл CSV не был создан!")

        local_output_path = f"/tmp/{output_filename}"
        os.rename(csv_files[0], local_output_path)

        # Загружаем файл в S3
        logging.info(f"Загрузка файла в S3: {CONTAINER}/{output_filename}")
        s3_client = boto3.client(
            's3',
            endpoint_url=ENDPOINT,
            aws_access_key_id=KEY_ID,
            aws_secret_access_key=SECRET,
            config=Config(signature_version='s3v4')
        )

        s3_client.upload_file(local_output_path, CONTAINER, output_filename)
        logging.info(f"Файл успешно загружен в S3: {CONTAINER}/{output_filename}")

    except Exception as e:
        logging.error(f"Ошибка при работе скрипта: {e}")
        raise AirflowException(f"Ошибка при создании витрины признаков: {e}")
    finally:
        if spark:
            spark.stop()

    logging.info("Работа скрипта успешно завершена.")
    return True
