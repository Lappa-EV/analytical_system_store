import os
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("ru_RU")

# Создание директорий (убедимся, что они существуют)
os.makedirs("data/stores", exist_ok=True)
os.makedirs("data/products", exist_ok=True)
os.makedirs("data/customers", exist_ok=True)
os.makedirs("data/purchases", exist_ok=True)


# --- 1. Генерация и сохранение магазинов ---
print("Генерация магазинов...")
categories = [
    "🥖 Зерновые и хлебобулочные изделия",
    "🥩 Мясо, рыба, яйца и бобовые",
    "🥛 Молочные продукты",
    "🍏 Фрукты и ягоды",
    "🥦 Овощи и зелень"
]

store_networks = [("Большая Пикча", 30), ("Маленькая Пикча", 15)]
generated_stores = [] # Используем другое имя, чтобы не конфликтовать с переменной stores для загрузки

for network, count in store_networks:
    for i in range(count):
        store_id = f"store-{len(generated_stores)+1:03}"
        city = fake.city()
        store = {
            "store_id": store_id,
            "store_name": f"{network} — Магазин на {fake.street_name()}",
            "store_network": network,
            "store_type_description": f"{'Супермаркет более 200 кв.м.' if network == 'Большая Пикча' else 'Магазин у дома менее 100 кв.м.'} Входит в сеть из {count} магазинов.",
            "type": "offline",
            "categories": categories,
            "manager": {
                "name": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email()
            },
            "location": {
                "country": "Россия",
                "city": city,
                "street": fake.street_name(),
                "house": str(fake.building_number()),
                "postal_code": fake.postcode(),
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude())
                }
            },
            "opening_hours": {
                "mon_fri": "09:00-21:00",
                "sat": "10:00-20:00",
                "sun": "10:00-18:00"
            },
            "accepts_online_orders": True,
            "delivery_available": True,
            "warehouse_connected": random.choice([True, False]),
            "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
        }
        generated_stores.append(store)
        with open(f"data/stores/{store_id}.json", "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)
print(f"Сгенерировано и сохранено магазинов: {len(generated_stores)}")


# --- 2. Генерация и сохранение товаров ---
print("Генерация товаров...")
generated_products = [] # Используем другое имя, чтобы не конфликтовать с переменной products для загрузки
for i in range(50): # Генерация 50 продуктов
    product = {
        "id": f"prd-{1000+i}",
        "name": fake.word().capitalize(),
        "group": random.choice(categories),
        "description": fake.sentence(),
        "kbju": {
            "calories": round(random.uniform(50, 300), 1),
            "protein": round(random.uniform(0.5, 20), 1),
            "fat": round(random.uniform(0.1, 15), 1),
            "carbohydrates": round(random.uniform(0.5, 50), 1)
        },
        "price": round(random.uniform(30, 700), 2), # Увеличил диапазон цен
        "unit": random.choice(["шт", "кг", "л"]), # Добавил разные единицы
        "origin_country": fake.country(),
        "expiry_days": random.randint(5, 365),
        "is_organic": random.choice([True, False]),
        "barcode": fake.ean(length=13),
        "manufacturer": { # Производитель всегда есть при генерации
            "name": fake.company(),
            "country": fake.country(),
            "website": f"https://{fake.domain_name()}",
            "inn": fake.bothify(text='##########')
        }
    }
    generated_products.append(product)
    with open(f"data/products/{product['id']}.json", "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)
print(f"Сгенерировано и сохранено товаров: {len(generated_products)}")


# --- 3. Генерация и сохранение покупателей ---
print("Генерация покупателей...")
generated_customers = [] # Используем другое имя, чтобы не конфликтовать с переменной customers для загрузки
# Генерируем покупателей для каждого магазина, чтобы они были связаны с локацией
for store in generated_stores:
    # Генерируем несколько покупателей на магазин (например, от 1 до 3)
    num_customers_per_store = random.randint(1, 3)
    for _ in range(num_customers_per_store):
        customer_id = f"cus-{1000 + len(generated_customers):04}"
        # Дата регистрации будет в прошлом, чтобы можно было генерировать покупки
        registration_date = (datetime.now() - timedelta(days=random.randint(30, 365 * 5))).isoformat()

        customer = {
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "gender": random.choice(["male", "female"]),
            "registration_date": registration_date,
            "is_loyalty_member": random.choice([True, False]), # Некоторые могут быть не участниками
            "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:10].upper()}" if random.random() < 0.8 else None, # 80% имеют карту
            "purchase_location": store["location"], # Привязываем к локации магазина
            "delivery_address": {
                "country": "Россия",
                "city": store["location"]["city"],
                "street": fake.street_name(),
                "house": str(fake.building_number()),
                "apartment": str(random.randint(1, 100)),
                "postal_code": fake.postcode()
            },
            "preferences": {
                "preferred_language": "ru",
                "preferred_payment_method": random.choice(["card", "cash"]),
                "receive_promotions": random.choice([True, False])
            }
        }
        # Убедимся, что у не-участников лояльности нет номера карты
        if not customer["is_loyalty_member"]:
            customer["loyalty_card_number"] = None

        generated_customers.append(customer)
        with open(f"data/customers/{customer_id}.json", "w", encoding="utf-8") as f:
            json.dump(customer, f, ensure_ascii=False, indent=2)
print(f"Сгенерировано и сохранено покупателей: {len(generated_customers)}")


# --- ЗАГРУЗКА СУЩЕСТВУЮЩИХ ДАННЫХ ДЛЯ СОЗДАНИЯ ПОКУПОК ---
# Всегда загружаем данные, чтобы убедиться, что они актуальны или если скрипт запускается только для создания покупок

# Загрузка магазинов
stores = []
stores_dir = "data/stores"
for filename in os.listdir(stores_dir):
    if filename.endswith(".json"):
        filepath = os.path.join(stores_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                stores.append(json.load(f))
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON в файле {filepath}: {e}. Пропуск файла.")
        except UnicodeDecodeError as e:
            print(f"Ошибка кодировки в файле {filepath}: {e}. Попытка с cp1251.")
            try:
                with open(filepath, "r", encoding="cp1251") as f:
                    stores.append(json.load(f))
            except Exception as ex:
                print(f"Не удалось прочитать файл {filepath} ни с utf-8, ни с cp1251: {ex}")

# Загрузка товаров
products = []
products_dir = "data/products"
for filename in os.listdir(products_dir):
    if filename.endswith(".json"):
        filepath = os.path.join(products_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                products.append(json.load(f))
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON в файле {filepath}: {e}. Пропуск файла.")
        except UnicodeDecodeError as e:
            print(f"Ошибка кодировки в файле {filepath}: {e}. Попытка с cp1251.")
            try:
                with open(filepath, "r", encoding="cp1251") as f:
                    products.append(json.load(f))
            except Exception as ex:
                print(f"Не удалось прочитать файл {filepath} ни с utf-8, ни с cp1251: {ex}")

# Загрузка покупателей
customers = []
customers_dir = "data/customers"
for filename in os.listdir(customers_dir):
    if filename.endswith(".json"):
        filepath = os.path.join(customers_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                customers.append(json.load(f))
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON в файле {filepath}: {e}. Пропуск файла.")
        except UnicodeDecodeError as e:
            print(f"Ошибка кодировки в файле {filepath}: {e}. Попытка с cp1251.")
            try:
                with open(filepath, "r", encoding="cp1251") as f:
                    customers.append(json.load(f))
            except Exception as ex:
                print(f"Не удалось прочитать файл {filepath} ни с utf-8, ни с cp1251: {ex}")


# Проверка, что данные загружены
if not stores:
    print("В папке 'data/stores' не найдено файлов магазинов. Создание покупок невозможно.")
    exit()
if not products:
    print("В папке 'data/products' не найдено файлов товаров. Создание покупок невозможно.")
    exit()
if not customers:
    print("В папке 'data/customers' не найдено файлов покупателей. Создание покупок невозможно.")
    exit()

print(f"\nЗагружено для покупок:")
print(f"- Магазинов: {len(stores)}")
print(f"- Товаров: {len(products)}")
print(f"- Покупателей: {len(customers)}")

# === 4. Генерация и сохранение покупок ===
print("\nНачинаем генерацию покупок...")
num_purchases = 500 # Увеличим количество покупок
for i in range(num_purchases):
    customer = random.choice(customers)
    store = random.choice(stores)

    # Убедимся, что есть хотя бы один товар для выбора
    if not products:
        print("Нет товаров для создания покупок. Пропустите генерацию этой покупки.")
        continue

    # === Обработка дат ===
    customer_registration_str = customer.get("registration_date")

    if customer_registration_str:
        try:
            customer_registration_date = datetime.fromisoformat(customer_registration_str)
        except ValueError:
            print(f"Некорректный формат registration_date для покупателя {customer.get('customer_id')}. Используется условная дата.")
            customer_registration_date = datetime.now() - timedelta(days=365*2) # Стандартная дата, если парсинг не удался
    else:
        # Если registration_date совсем отсутствует, устанавливаем ее очень давно
        customer_registration_date = datetime.now() - timedelta(days=365*3)

    now = datetime.now()
    # Убедимся, что customer_registration_date не в будущем
    if customer_registration_date > now:
        customer_registration_date = now - timedelta(days=random.randint(1,30)) # Если вдруг дата регистрации в будущем, ставим недавнюю

    time_difference = now - customer_registration_date # timedelta object

    # Если customer_registration_date очень близок к now, или even future bug, time_difference can be negative or zero
    if time_difference.total_seconds() <= 0:
        # Устанавливаем purchase_datetime как current_time - небольшой случайный период
        purchase_datetime = now - timedelta(seconds=random.randint(60, 3600))
    else:
        # Генерация случайного timedelta между 0 и time_difference
        random_seconds = random.randint(0, int(time_difference.total_seconds()))
        purchase_datetime = customer_registration_date + timedelta(seconds=random_seconds)

    purchase_datetime_str = purchase_datetime.isoformat()

    # === Выбор товаров и формирование покупки ===
    # Выбираем от 1 до 5 случайных товаров
    num_items_to_select = random.randint(1, min(5, len(products)))
    items = random.sample(products, k=num_items_to_select)

    purchase_items = []
    total = 0
    for item in items:
        qty = random.randint(1, 10) # Случайное количество от 1 до 10
        total_price = round(item["price"] * qty, 2)
        total += total_price
        purchase_items.append({
            "product_id": item["id"],
            "name": item["name"],
            "category": item["group"],
            "quantity": qty,
            "unit": item["unit"],
            "price_per_unit": item["price"],
            "total_price": total_price,
            "kbju": item.get("kbju", {}),
            "manufacturer": item.get("manufacturer", None) # Использование .get() для manufacturer
        })

    if not purchase_items: # Маловероятно, но на случай если items окажется пустым
        continue

    # Получение данных о лояльности покупателя
    is_loyalty_member = customer.get("is_loyalty_member", False)
    loyalty_card_number = customer.get("loyalty_card_number", None)


    purchase_id = f"ord-{i+1:05}"
    purchase = {
        "purchase_id": purchase_id,
        "customer": {
            "customer_id": customer["customer_id"],
            "first_name": customer["first_name"],
            "last_name": customer["last_name"],
            "email": customer.get("email"),
            "phone": customer.get("phone"),
            "is_loyalty_member": is_loyalty_member,
            "loyalty_card_number": loyalty_card_number,
        },
        "store": {
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "store_network": store["store_network"],
            "location": store["location"]
        },
        "items": purchase_items,
        "total_amount": round(total, 2),
        "payment_method": random.choice(["card", "cash", "online"]), # Добавим онлайн оплату
        "is_delivery": random.choice([True, False]),
        "delivery_address": customer.get("delivery_address") if random.random() < 0.5 else None, # 50% заказов с доставкой
        "purchase_datetime": purchase_datetime_str
    }

    # Сохранение покупки
    with open(f"data/purchases/{purchase['purchase_id']}.json", "w", encoding="utf-8") as f:
        json.dump(purchase, f, ensure_ascii=False, indent=2)

print(f"Генерация {num_purchases} покупок завершена!")
