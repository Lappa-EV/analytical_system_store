# mongodb_kafka_clickhouse/consumer.py
"""
Консюмер для пайплайна MongoDB-Kafka-ClickHouse.

Этот скрипт потребляет сообщения из топиков Kafka, обрабатывает данные в формате JSON,
и сохраняет их в таблицы ClickHouse с движком MergeTree. Скрипт извлекает поля
из вложенных JSON-структур и преобразует их в столбцы таблицы.
"""

import json
import logging
import os
from datetime import datetime
import traceback
from kafka import KafkaConsumer
from clickhouse_driver import Client
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка переменных окружения
load_dotenv()

# Параметры подключения
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

# Топики для обработки
TOPICS = ["stores", "products", "customers", "purchases"]

# Словарь для сопоставления топиков и полей ID
TOPIC_ID_MAP = {
    "products": "id",
    "customers": "customer_id",
    "purchases": "purchase_id",
    "stores": "store_id"
}

# Определение служебных полей
SERVICE_FIELDS = {
    "json_data": "String",
    "event_time": "DateTime64(9)"
}

# Определяем поля для каждой таблицы
TABLE_FIELDS = {
    "stores": {
        "store_id": "String",
        "store_name": "String",
        "store_network": "String",
        "store_type_description": "String",
        "type": "String",
        "categories": "String",
        "manager_name": "String",
        "manager_phone": "String",
        "manager_email": "String",
        "location_country": "String",
        "location_city": "String",
        "location_street": "String",
        "location_house": "String",
        "location_postal_code": "String",
        "location_coordinates_latitude": "String",
        "location_coordinates_longitude": "String",
        "opening_hours_mon_fri": "String",
        "opening_hours_sat": "String",
        "opening_hours_sun": "String",
        "accepts_online_orders": "String",
        "delivery_available": "String",
        "warehouse_connected": "String",
        "last_inventory_date": "String"
    },
    "purchases": {
        "purchase_id": "String",
        "customer_customer_id": "String",
        "customer_first_name": "String",
        "customer_last_name": "String",
        "customer_email": "String",
        "customer_phone": "String",
        "customer_is_loyalty_member": "String",
        "customer_loyalty_card_number": "String",
        "store_store_id": "String",
        "store_store_name": "String",
        "store_store_network": "String",
        "store_location_country": "String",
        "store_location_city": "String",
        "store_location_street": "String",
        "store_location_house": "String",
        "store_location_postal_code": "String",
        "store_location_coordinates_latitude": "String",
        "store_location_coordinates_longitude": "String",
        "items_product_id": "String",
        "items_name": "String",
        "items_category": "String",
        "items_quantity": "String",
        "items_unit": "String",
        "items_price_per_unit": "String",
        "items_total_price": "String",
        "items_kbju_calories": "String",
        "items_kbju_protein": "String",
        "items_kbju_fat": "String",
        "items_kbju_carbohydrates": "String",
        "items_manufacturer_name": "String",
        "items_manufacturer_country": "String",
        "items_manufacturer_website": "String",
        "items_manufacturer_inn": "String",
        "total_amount": "String",
        "payment_method": "String",
        "is_delivery": "String",
        "delivery_address_country": "String",
        "delivery_address_city": "String",
        "delivery_address_street": "String",
        "delivery_address_house": "String",
        "delivery_address_apartment": "String",
        "delivery_address_postal_code": "String",
        "purchase_datetime": "String"
    },
    "products": {
        "id": "String",
        "name": "String",
        "group": "String",
        "description": "String",
        "kbju_calories": "String",
        "kbju_protein": "String",
        "kbju_fat": "String",
        "kbju_carbohydrates": "String",
        "price": "String",
        "unit": "String",
        "origin_country": "String",
        "expiry_days": "String",
        "is_organic": "String",
        "barcode": "String",
        "manufacturer_name": "String",
        "manufacturer_country": "String",
        "manufacturer_website": "String",
        "manufacturer_inn": "String"
    },
    "customers": {
        "customer_id": "String",
        "first_name": "String",
        "last_name": "String",
        "email": "String",
        "phone": "String",
        "birth_date": "String",
        "gender": "String",
        "registration_date": "String",
        "is_loyalty_member": "String",
        "loyalty_card_number": "String",
        "purchase_location_country": "String",
        "purchase_location_city": "String",
        "purchase_location_street": "String",
        "purchase_location_house": "String",
        "purchase_location_postal_code": "String",
        "purchase_location_coordinates_latitude": "String",
        "purchase_location_coordinates_longitude": "String",
        "delivery_address_country": "String",
        "delivery_address_city": "String",
        "delivery_address_street": "String",
        "delivery_address_house": "String",
        "delivery_address_apartment": "String",
        "delivery_address_postal_code": "String",
        "preferences_preferred_language": "String",
        "preferences_preferred_payment_method": "String",
        "preferences_receive_promotions": "String"
    }
}

# Отображение полей JSON на имена столбцов
FIELD_MAPPING = {
    "products": {
        "manufacturer_name": "manufacturer.name",
        "manufacturer_country": "manufacturer.country",
        "manufacturer_website": "manufacturer.website",
        "manufacturer_inn": "manufacturer.inn",
        "kbju_calories": "kbju.calories",
        "kbju_protein": "kbju.protein",
        "kbju_fat": "kbju.fat",
        "kbju_carbohydrates": "kbju.carbohydrates"
    },
    "stores": {
        "categories": "categories",  # Обрабатываем особым образом ниже
        "manager_name": "manager.name",
        "manager_phone": "manager.phone",
        "manager_email": "manager.email",
        "location_country": "location.country",
        "location_city": "location.city",
        "location_street": "location.street",
        "location_house": "location.house",
        "location_postal_code": "location.postal_code",
        "location_coordinates_latitude": "location.coordinates.latitude",
        "location_coordinates_longitude": "location.coordinates.longitude",
        "opening_hours_mon_fri": "opening_hours.mon_fri",
        "opening_hours_sat": "opening_hours.sat",
        "opening_hours_sun": "opening_hours.sun"
    },
    "customers": {
        "purchase_location_country": "purchase_location.country",
        "purchase_location_city": "purchase_location.city",
        "purchase_location_street": "purchase_location.street",
        "purchase_location_house": "purchase_location.house",
        "purchase_location_postal_code": "purchase_location.postal_code",
        "purchase_location_coordinates_latitude": "purchase_location.coordinates.latitude",
        "purchase_location_coordinates_longitude": "purchase_location.coordinates.longitude",
        "delivery_address_country": "delivery_address.country",
        "delivery_address_city": "delivery_address.city",
        "delivery_address_street": "delivery_address.street",
        "delivery_address_house": "delivery_address.house",
        "delivery_address_apartment": "delivery_address.apartment",
        "delivery_address_postal_code": "delivery_address.postal_code",
        "preferences_preferred_language": "preferences.preferred_language",
        "preferences_preferred_payment_method": "preferences.preferred_payment_method",
        "preferences_receive_promotions": "preferences.receive_promotions"
    },
    "purchases": {
        "customer_customer_id": "customer.customer_id",
        "customer_first_name": "customer.first_name",
        "customer_last_name": "customer.last_name",
        "customer_email": "customer.email",
        "customer_phone": "customer.phone",
        "customer_is_loyalty_member": "customer.is_loyalty_member",
        "customer_loyalty_card_number": "customer.loyalty_card_number",
        "store_store_id": "store.store_id",
        "store_store_name": "store.store_name",
        "store_store_network": "store.store_network",
        "store_location_country": "store.location.country",
        "store_location_city": "store.location.city",
        "store_location_street": "store.location.street",
        "store_location_house": "store.location.house",
        "store_location_postal_code": "store.location.postal_code",
        "store_location_coordinates_latitude": "store.location.coordinates.latitude",
        "store_location_coordinates_longitude": "store.location.coordinates.longitude",
        "items_product_id": "items.0.product_id",
        "items_name": "items.0.name",
        "items_category": "items.0.category",
        "items_quantity": "items.0.quantity",
        "items_unit": "items.0.unit",
        "items_price_per_unit": "items.0.price_per_unit",
        "items_total_price": "items.0.total_price",
        "items_kbju_calories": "items.0.kbju.calories",
        "items_kbju_protein": "items.0.kbju.protein",
        "items_kbju_fat": "items.0.kbju.fat",
        "items_kbju_carbohydrates": "items.0.kbju.carbohydrates",
        "items_manufacturer_name": "items.0.manufacturer.name",
        "items_manufacturer_country": "items.0.manufacturer.country",
        "items_manufacturer_website": "items.0.manufacturer.website",
        "items_manufacturer_inn": "items.0.manufacturer.inn",
        "delivery_address_country": "delivery_address.country",
        "delivery_address_city": "delivery_address.city",
        "delivery_address_street": "delivery_address.street",
        "delivery_address_house": "delivery_address.house",
        "delivery_address_apartment": "delivery_address.apartment",
        "delivery_address_postal_code": "delivery_address.postal_code"
    }
}


def create_table(client, topic):
    """Создает таблицу в ClickHouse, если она не существует."""
    table_name = topic

    # Формируем определения полей
    field_definitions = []

    # Добавляем служебные поля
    for field_name, field_type in SERVICE_FIELDS.items():
        field_definitions.append(f"{field_name} {field_type}")

    # Добавляем специфичные для таблицы поля
    if topic in TABLE_FIELDS:
        for field_name, field_type in TABLE_FIELDS[topic].items():
            field_definitions.append(f"{field_name} {field_type}")

    # Собираем SQL-запрос с новым движком MergeTree
    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(field_definitions)}
        ) 
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_time)
        ORDER BY event_time
        TTL event_time + INTERVAL 180 DAY
        SETTINGS index_granularity =8192
    """

    try:
        client.execute(query)
        logging.info(f"Таблица {table_name} готова")
    except Exception as e:
        logging.error(f"Ошибка создания таблицы {table_name}: {e}")


def get_safe_value(json_obj, path, default=""):
    """Безопасно извлекает значение из JSON по пути."""
    if not json_obj:
        return default

    # Особая обработка категорий для stores
    if path == "categories" and isinstance(json_obj.get("categories"), list):
        categories = json_obj.get("categories", [])
        # Объединяем категории в строку через запятую без скобок
        return ", ".join(categories) if categories else default

    # Проверка на путь с индексом массива (например, items.0.product_id)
    if '.' in path and any(part.isdigit() for part in path.split('.')):
        parts = path.split('.')
        current = json_obj

        try:
            for part in parts:
                if part.isdigit():
                    # Обработка индекса массива
                    idx = int(part)
                    if not isinstance(current, list) or idx >= len(current):
                        return default
                    current = current[idx]
                elif current is None or not isinstance(current, dict) or part not in current:
                    return default
                else:
                    current = current[part]

            # Преобразуем значение в строку
            if current is None:
                return default
            elif isinstance(current, (dict, list)):
                return json.dumps(current, ensure_ascii=False)  # Корректная обработка юникода
            else:
                return str(current)
        except Exception:
            return default
    else:
        # Стандартная обработка пути без индексов массива
        parts = path.split('.')
        current = json_obj

        try:
            for part in parts:
                if current is None or not isinstance(current, dict) or part not in current:
                    return default
                current = current[part]

            # Преобразуем значение в строку
            if current is None:
                return default
            elif isinstance(current, (dict, list)):
                # Если значение - это список, преобразуем его в строку через запятую
                if isinstance(current, list):
                    return ", ".join(str(item) for item in current)
                return json.dumps(current, ensure_ascii=False)  # Корректная обработка юникода
            else:
                return str(current)
        except Exception:
            return default


def process_message(client, topic, message_value):
    """Обрабатывает сообщение из Kafka и вставляет данные в ClickHouse."""
    try:
        # Получаем JSON данные
        json_data_string = message_value.get('json_data')
        if not json_data_string:
            logging.error(f"{topic}: отсутствует поле json_data")
            return

        try:
            parsed_json = json.loads(json_data_string)
        except json.JSONDecodeError as e:
            logging.error(f"{topic}: ошибка парсинга JSON: {e}")
            return

        # Получаем event_time
        event_time = datetime.fromisoformat(message_value.get('event_time', datetime.now().isoformat()))

        # Создаем словарь данных для вставки
        data_dict = {
            "json_data": json_data_string,
            "event_time": event_time
        }

        # Извлекаем дополнительные поля из JSON
        if topic in TABLE_FIELDS:
            for field_name in TABLE_FIELDS[topic].keys():
                json_path = field_name
                if topic in FIELD_MAPPING and field_name in FIELD_MAPPING[topic]:
                    json_path = FIELD_MAPPING[topic][field_name]
                data_dict[field_name] = get_safe_value(parsed_json, json_path, "")

        # Формируем и выполняем запрос на вставку
        fields = ", ".join(data_dict.keys())
        insert_sql = f"INSERT INTO {topic} ({fields}) VALUES"
        values = [list(data_dict.values())]

        client.execute(insert_sql, values)

        # Логируем только ID для отслеживания
        id_field = TOPIC_ID_MAP.get(topic, f"{topic}_id")
        id_value = get_safe_value(parsed_json, id_field, "?")
        logging.info(f"{topic}: сохранено, {id_field}={id_value}")

    except Exception as e:
        logging.error(f"{topic}: ошибка обработки: {e}")
        logging.debug(traceback.format_exc())


def main():
    """Основная функция consumer."""
    logging.info(f"Запуск consumer для топиков: {', '.join(TOPICS)}")

    # Создание клиента ClickHouse
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    # Создаем таблицы
    for topic in TOPICS:
        create_table(client, topic)

    # Конфигурация Kafka Consumer
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        # Основной цикл обработки сообщений
        for message in consumer:
            process_message(client, message.topic, message.value)
            consumer.commit()

    except KeyboardInterrupt:
        logging.info("Остановлено пользователем")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        logging.debug(traceback.format_exc())
    finally:
        consumer.close()
        client.disconnect()
        logging.info("Работа завершена")


if __name__ == "__main__":
    main()
