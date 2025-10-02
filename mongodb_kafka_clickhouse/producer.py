# mongodb_kafka_clickhouse/producer.py
"""
Продюсер для пайплайна MongoDB-Kafka-ClickHouse.

Этот скрипт считывает данные из коллекций MongoDB, обрабатывает конфиденциальную информацию
(хэширует email и телефоны) и отправляет данные в топики Kafka. Каждый документ обрабатывается
и преобразуется в сообщение с исходными данными, сохраненными в поле json_data,
а также дополнен служебным полем event_time.
"""

import json
import os
import re
import hashlib
import logging
from kafka import KafkaProducer
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv

# Настройка логирования и загрузка переменных окружения
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# Параметры подключения из переменных окружения
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DATABASE")

# Конфигурация обработки
TOPICS = ["products", "stores", "customers", "purchases"]
SENSITIVE_TOPICS = ["stores", "customers", "purchases"]


def normalize_phone(phone):
    """Нормализует номер телефона в формат +7..."""
    if not phone:
        return phone

    phone = re.sub(r'[^\d+]', '', str(phone).strip())

    if phone.startswith('8'):
        return '+7' + phone[1:]
    elif phone.startswith('7') and '+' not in phone:
        return '+7' + phone[1:]
    elif len(phone) == 10:
        return '+7' + phone

    return phone


def normalize_email(email):
    """Нормализует email"""
    return str(email).strip().lower() if email else email


def hash_value(value):
    """Хэширует значение"""
    return hashlib.md5(str(value).encode('utf-8')).hexdigest() if value else value


def process_data(data, topic):
    """Обрабатывает данные, хэшируя чувствительные поля при необходимости"""
    if not isinstance(data, dict):
        return data

    result = data.copy()

    # Обработка чувствительных полей только для указанных топиков
    if topic in SENSITIVE_TOPICS:
        for key, value in data.items():
            if isinstance(value, str):
                if 'phone' in key.lower():
                    result[key] = hash_value(normalize_phone(value))
                elif 'email' in key.lower():
                    result[key] = hash_value(normalize_email(value))

    # Обработка вложенных структур
    for key, value in result.items():
        if isinstance(value, dict):
            result[key] = process_data(value, topic)
        elif isinstance(value, list):
            result[key] = [process_data(item, topic) if isinstance(item, dict) else item for item in value]

    return result


def main():
    """Основная функция для чтения из MongoDB и отправки в Kafka"""
    try:
        # Подключение к MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]

        # Проверка наличия данных
        for topic in TOPICS:
            count = db[topic].count_documents({})
            logging.info(f"Коллекция {topic}: {count} документов")

        # Подключение к Kafka
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
        )

        # Текущее время для event_time
        current_time = datetime.now().isoformat(timespec='microseconds')

        # Обработка коллекций
        for topic in TOPICS:
            count = 0
            for doc in db[topic].find({}):
                try:
                    # Преобразование ObjectId в строку
                    doc['_id'] = str(doc['_id'])

                    # Обработка данных и отправка в Kafka
                    processed_doc = process_data(doc, topic)

                    producer.send(topic, {
                        'json_data': json.dumps(processed_doc, ensure_ascii=False),
                        'event_time': current_time
                    })

                    count += 1
                    if count % 500 == 0:
                        logging.info(f"{topic}: обработано {count} документов")

                except Exception as e:
                    logging.error(f"{topic}: ошибка обработки документа: {e}")

            producer.flush()
            logging.info(f"{topic}: всего обработано {count} документов")

    except Exception as e:
        logging.error(f"Общая ошибка: {e}")
    finally:
        if 'client' in locals():
            client.close()
        if 'producer' in locals() and producer:
            producer.close()


if __name__ == "__main__":
    main()
