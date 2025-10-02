# load_data_mongodb/load_data_mongo.py
"""
Настоящий скрипт будет:
1. Сканировать указанные папки (data/stores, data/products, data/customers, data/purchases).
2. Парсить каждый JSON-файл.
3. Загружать данные в соответствующие коллекции MongoDB.
"""
from os import path, listdir, getenv
from dotenv import load_dotenv
from pymongo import MongoClient
import json

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получение параметров подключения из переменных окружения
MONGO_URI = getenv("MONGO_URI")
MONGO_DATABASE = getenv("MONGO_DATABASE")
DATA_DIR = getenv("DATA_DIR")

print(f"DATA_DIR: {DATA_DIR}")
print(f"Path exists: {path.exists(DATA_DIR)}")

# Словарь, связывающий названия папок с названиями коллекций MongoDB
COLLECTION_MAP = {
    "stores": "stores",
    "products": "products",
    "customers": "customers",
    "purchases": "purchases"
}

# Функция для загрузки JSON-файлов из директории в коллекцию MongoDB
def load_insert_json(directory, collection):
    """
    Загружает JSON-файлы из указанной директории в  коллекцию MongoDB.
    """
    inserted_count = 0
    # Если директория не существует, возвращаем 0 загруженных файлов
    if not path.exists(directory):
        return inserted_count

    # Перебираем все файлы в директории
    for filename in listdir(directory):

        # Если файл имеет расширение .json
        if filename.endswith(".json"):

            # Формируем путь к файлу
            filepath = path.join(directory, filename)

            try:
                # Открываем файл для чтения
                with open(filepath, "r", encoding="utf-8") as f:

                    # Загружаем JSON данные из файла
                    data = json.load(f)

                    # Вставляем данные в коллекцию MongoDB
                    collection.insert_one(data)

                    # Увеличиваем счетчик загруженных файлов
                    inserted_count += 1

            # Обработка исключений, возникающих при чтении или парсинге JSON
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Ошибка чтения файла {filename}: {e}")

    # Возвращаем количество загруженных файлов
    return inserted_count


# Основной блок исполнения скрипта
if __name__ == "__main__":

    # Подключение к MongoDB
    client = MongoClient(MONGO_URI)

    # Получение доступа к указанной базе данных
    db = client[MONGO_DATABASE]

    # Очистка всех коллекций перед загрузкой данных
    for collection_name in COLLECTION_MAP.values():
        db[collection_name].delete_many({})
        print(f"Коллекция {collection_name} очищена.")

    # Загрузка данных из каждой папки в соответствующую коллекцию
    for folder_name, collection_name in COLLECTION_MAP.items():

        # Формируем полный путь к папке с данными
        folder_path = path.join(DATA_DIR, folder_name)
        folder_base_name = path.basename(folder_path) # Получаем только имя паки

        # Выводим информацию о количестве загруженных файлов из папки
        print(f"Из папки {folder_base_name} загружено {load_insert_json(folder_path, db[collection_name])} файлов")

    # Закрытие подключения к MongoDB
    client.close()
    print("Загрузка завершена.")

