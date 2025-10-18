# dags/tasks/producer_task.py
"""
Задача для запуска Kafka-продюсера в Airflow, использующая confluent-kafka.
Извлекает данные из MongoDB, преобразует и отправляет в топики Kafka.
Обрабатывает конфиденциальные данные (хэширование emails и телефонов).
Получает параметры подключения из аргументов функции.
"""

import json
import re
import hashlib
import logging
from confluent_kafka import Producer
from pymongo import MongoClient
from datetime import datetime


def delivery_report(err, msg):
    """Callback-функция для отчетов о доставке сообщений"""
    if err is not None:
        logging.error(f"Ошибка доставки сообщения: {err}")
    else:
        logging.debug(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")


def run_producer(**kwargs):
    """
    Запускает Kafka producer для извлечения данных из MongoDB и отправки в Kafka.

    Args:
        **kwargs: аргументы, содержащие параметры подключения:
            - kafka_broker: адрес брокера Kafka
            - mongo_uri: URI для подключения к MongoDB
            - mongo_db: имя базы данных MongoDB
            - topics: список топиков Kafka (по умолчанию: products, stores, customers, purchases)
            - sensitive_topics: список топиков с чувствительными данными (по умолчанию: stores, customers, purchases)
    """
    # Получение параметров из kwargs
    kafka_broker = kwargs.get('kafka_broker')
    mongo_uri = kwargs.get('mongo_uri')
    mongo_db = kwargs.get('mongo_db')
    topics = kwargs.get('topics', ["products", "stores", "customers", "purchases"])

    # Определяем чувствительные топики на основе списка топиков
    default_sensitive = ["stores", "customers", "purchases"]
    sensitive_topics = kwargs.get('sensitive_topics', [t for t in topics if t in default_sensitive])

    # Настройка логирования, если оно еще не настроено
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        if topic in sensitive_topics:
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

    client = None
    producer = None
    try:
        # Подключение к MongoDB
        client = MongoClient(mongo_uri)
        db = client[mongo_db]

        # Проверка наличия данных
        for topic in topics:
            count = db[topic].count_documents({})
            logging.info(f"Коллекция {topic}: {count} документов")

        # Подключение к Kafka с использованием confluent-kafka
        producer_conf = {
            'bootstrap.servers': kafka_broker
        }
        producer = Producer(producer_conf)

        # Текущее время для event_time
        current_time = datetime.now().isoformat(timespec='microseconds')

        # Обработка коллекций
        for topic in topics:
            count = 0
            for doc in db[topic].find({}):
                try:
                    # Преобразование ObjectId в строку
                    doc['_id'] = str(doc['_id'])

                    # Обработка данных
                    processed_doc = process_data(doc, topic)

                    # Подготовка данных для отправки
                    message_data = {
                        'json_data': json.dumps(processed_doc, ensure_ascii=False),
                        'event_time': current_time
                    }

                    # Сериализация в JSON и отправка в Kafka
                    message_json = json.dumps(message_data, ensure_ascii=False).encode('utf-8')
                    producer.produce(topic, value=message_json, callback=delivery_report)

                    count += 1
                    if count % 500 == 0:
                        producer.flush()  # Периодически сбрасываем сообщения
                        logging.info(f"{topic}: обработано {count} документов")

                except Exception as e:
                    logging.error(f"{topic}: ошибка обработки документа: {e}")

            producer.flush()  # Обеспечиваем отправку всех сообщений
            logging.info(f"{topic}: всего обработано {count} документов")

    except Exception as e:
        logging.error(f"Общая ошибка: {e}")
        raise
    finally:
        if client:
            client.close()
        if producer:
            producer.flush()  # Финальный flush перед завершением
