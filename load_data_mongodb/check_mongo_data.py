# load_data_mongodb/check_mongo_data.py
"""Файл для проверки загрузки в базу данных MongoDB"""

import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получение параметров подключения из переменных окружения
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE")

# Определение коллекций, которые нужно проверить
COLLECTION_NAMES = [
    "stores",
    "products",
    "customers",
    "purchases"
]

# Функция для проверки наличия загруженных данных в MongoDB
def check_and_display_data():
    """
    Подключается к MongoDB, перебирает коллекции, подсчитывает количество документов
    и отображает первые 3 документа каждой коллекции.
    """
    client = None # Инициализируем client вне блока try, чтобы он был виден и в finally
    try:
        # Подключение к MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]

        print("--- Проверка загруженных данных в MongoDB ---")

        # Итерация по списку коллекций
        for collection_name in COLLECTION_NAMES:

            # Получаем коллекцию из базы данных
            collection = db[collection_name]

            # Подсчитываем общее количество документов в коллекции
            total_documents = collection.count_documents({})

            print(f"\nКоллекция '{collection_name}':")
            print(f"  Всего документов: {total_documents}")

            # Если в коллекции есть документы
            if total_documents > 0:
                print("  Первые 3 документа:")

                # Выводим первые 3 документа из коллекции для проверки
                for i, doc in enumerate(collection.find().limit(3)):
                    print(f"    Документ {i+1}: {doc}")
            else:
                print("  (Коллекция пуста)")

    # Обработка исключений при подключении или запросе к MongoDB
    except Exception as e:
        print(f"Произошла ошибка при подключении или запросе к MongoDB: {e}")

    # Блок закрытия соединения с MongoDB
    finally:
        if client: # Закрываем соединение только если оно было успешно установлено
            client.close()
            print("\nСоединение с MongoDB закрыто.")

# Запуск функции проверки
if __name__ == "__main__":
    check_and_display_data()
    print("Проверка данных завершена.")
