# Проект аналитической системы для сети магазинов "Пикча"

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![SQL](https://img.shields.io/badge/SQL-Database-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-NoSQL-green)
![Kafka](https://img.shields.io/badge/Kafka-MessageBroker-red)
![ClickHouse](https://img.shields.io/badge/ClickHouse-OLAP-orange)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-yellow)
![Jupyter Notebook](https://img.shields.io/badge/Jupyter-Notebook-orange)
![PySpark](https://img.shields.io/badge/PySpark-3.x-blue)
![S3](https://img.shields.io/badge/S3-Storage-yellow)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-brightgreen)

## Описание проекта

Разработка аналитической системы для сетевого магазина "Пикча", которая позволит эффективнее и выгоднее продавать товары. Заказчик запрашивает демо-версию рабочего инструмента перед предоставлением доступа к NoSQL хранилищу.

## Структура сети магазинов

1. **Большая Пикча** (помещение более 200 кв.м.)
   - 30 магазинов по стране (некоторые в одном городе)

2. **Маленькая Пикча** (помещение менее 100 кв.м.)
   - 15 магазинов по стране (некоторые в одном городе)

## Ассортимент товаров

В каждом магазине представлены товары пяти основных продовольственных групп:

1. 🥖 Зерновые и хлебобулочные изделия
2. 🥩 Мясо, рыба, яйца и бобовые
3. 🥛 Молочные продукты
4. 🍏 Фрукты и ягоды
5. 🥦 Овощи и зелень

В каждую группу входит не менее 20 позиций (всего около 100 товаров в ассортименте).

## Структура данных

### Товар

```json
{
  "id": "prd-0037",
  "name": "Творог 5%",
  "group": "Молочные",
  "description": "Творог из коровьего молока, 5% жирности",
  "kbju": {
    "calories": 121,
    "protein": 16.5,
    "fat": 5,
    "carbohydrates": 3.3
  },
  "price": 89.99,
  "unit": "упаковка",
  "origin_country": "Россия",
  "expiry_days": 12,
  "is_organic": false,
  "barcode": "4600111222333",
  "manufacturer": {
    "name": "ООО Молочный Комбинат",
    "country": "Россия",
    "website": "https://moloko.ru",
    "inn": "7723456789"
  }
}
```

| Поле | Тип | Описание |
|------|-----|----------|
| id | string | Уникальный идентификатор товара в системе |
| name | string | Название продукта |
| group | string | Продовольственная группа |
| description | string | Краткое описание товара |
| kbju | object | Пищевая ценность на 100 г продукта |
| price | number | Стоимость товара за единицу |
| unit | string | Единица измерения (упаковка, кг, л, шт) |
| origin_country | string | Страна происхождения товара |
| expiry_days | integer | Срок годности в днях |
| is_organic | boolean | Органический ли продукт |
| barcode | string | Штрихкод товара (EAN-13) |
| manufacturer | object | Информация о производителе |

### Покупатель

```json
{
  "customer_id": "cus-102345",
  "first_name": "Алексей",
  "last_name": "Иванов",
  "email": "alexey.ivanov@example.com",
  "phone": "+7-900-123-45-67",
  "birth_date": "1990-04-15",
  "gender": "male",
  "registration_date": "2023-08-20T14:32:00Z",
  "is_loyalty_member": true,
  "loyalty_card_number": "LOYAL-987654321",
  "purchase_location": {
    "store_id": "store-001", 
    "store_name": "Большая Пикча — Магазин на Тверской",
    "store_network": "Большая Пикча",
    "store_type_description": "Супермаркет площадью более 200 кв.м.",
    "country": "Россия",
    "city": "Москва",
    "street": "ул. Тверская",
    "house": "15",
    "postal_code": "125009"
  },
  "delivery_address": {
    "country": "Россия",
    "city": "Москва",
    "street": "ул. Ленина",
    "house": "12",
    "apartment": "45",
    "postal_code": "101000"
  },
  "preferences": {
    "preferred_language": "ru",
    "preferred_payment_method": "card",
    "receive_promotions": true
  }
}
```

| Поле | Описание | Примечание                                    |
|------|----------|-----------------------------------------------|
| customer_id | Уникальный идентификатор клиента | Всегда указывается                            |
| first_name/last_name | Имя и фамилия | Может быть NULL                               |
| email | Адрес электронной почты | Может быть NULL                               |
| phone | Номер телефона | Всегда указывается                            |
| birth_date | Дата рождения | Для акций, идентификации                      |
| gender | Пол | male, female, NULL                            |
| registration_date | Дата регистрации в системе | Всегда указывается                            |
| is_loyalty_member | Признак участия в программе лояльности | Всегда True                                   |
| loyalty_card_number | Номер карты лояльности | Всегда указывается |
| purchase_location | Адрес магазина, где была совершена покупка | Всегда указывается                            |
| delivery_address | Адрес доставки | По умолчанию это адрес, где оформляется карта клиента |

### Магазин

```json
{
  "store_id": "store-001",
  "store_name": "Большая Пикча — Магазин на Тверской",
  "store_network": "Большая Пикча",
  "store_type_description": "Супермаркет площадью более 200 кв.м.",
  "type": "offline",
  "categories": [
    "🥖 Зерновые и хлебобулочные изделия",
    "🥩 Мясо, рыба, яйца и бобовые",
    "🥛 Молочные продукты",
    "🍏 Фрукты и ягоды",
    "🥦 Овощи и зелень"
  ],
  "manager": {
    "name": "Светлана Петрова", 
    "phone": "+7-900-555-33-22",
    "email": "manager@tverskoy-store.ru"
  },
  "location": {
    "country": "Россия",
    "city": "Москва",
    "street": "ул. Тверская",
    "house": "15",
    "postal_code": "125009",
    "coordinates": {
      "latitude": 55.7575,
      "longitude": 37.6156
    }
  },
  "opening_hours": {
    "mon_fri": "09:00-21:00",
    "sat": "10:00-20:00",
    "sun": "10:00-18:00"
  },
  "accepts_online_orders": true,
  "delivery_available": true,
  "warehouse_connected": true,
  "last_inventory_date": "2025-06-28"
}
```

## Ключевые требования

1. Разработать демо-версию аналитической системы без доступа к NoSQL хранилищу заказчика
2. Учесть структуру данных о товарах, покупателях и магазинах.
</br>Полный текст задания [здесь](/Задание.pdf)

## Реализация решения
### Требования:
Установлен Docker (Docker Desktop).

### Запуск проекта

Для запуска проекта используется Docker Compose:

- [docker-compose.yml](/docker-compose.yml)
- [Dockerfile.jupyter](/Dockerfile.jupyter)

#### Сборка локальных сервисов из Dockerfile-ов (jupyter, airflow) и подтягивание остальных образов:
`docker compose up -d --build`


### Генерация и загрузка данных

#### Генерация тестовых данных

Для имитации взаимодействия с заказчиком создан скрипт [generate_synthetic_data.py](/generate_synthetic_data.py), который генерирует следующие тестовые данные:

1. 45 JSON-файлов, описывающих каждый магазин
2. 20 JSON-файлов, описывающих товары
3. 45 JSON-файлов, описывающих покупателей (по 1 покупателю в каждом магазине)
4. 200 JSON-файлов, описывающих покупки от различных покупателей в разных магазинах

Эти файлы сохраняются в директории `data` проекта, разделенные по соответствующим папкам.

![Структура данных](Photo/1.png)

#### Загрузка данных в MongoDB

1. С помощью скрипта [load_data_mongo.py](load_data_mongodb/load_data_mongo.py) загружаем все JSON-файлы в NoSQL хранилище MongoDB. 

2. Проверяем загрузку с помощью скрипта [check_mongo_data.py](load_data_mongodb/check_mongo_data.py).
</br>Настройки подключения хранятся в файле [.env](load_data_mongodb/.env)

#### Обработка через Kafka

Для загрузки данных из MongoDB в ClickHouse используется Kafka:

1. Устанавливаем необходимую библиотеку: `pip install kafka-python`
2. Созданы файлы [producer.py](mongodb_kafka_clickhouse/producer.py) и [consumer.py](mongodb_kafka_clickhouse/consumer.py) для работы с Kafka
3. Настройки подключения хранятся в файле [.env](mongodb_kafka_clickhouse/.env)

**Producer.py**:
- Считывает данные из MongoDB
- Приводит их к единому виду
- Хеширует конфиденциальную информацию (email и телефоны)
- Отправляет данные в топики Kafka
- Добавляет поле `event_time` (дата загрузки)

**Consumer.py**:
- Принимает сообщения из топиков Kafka
- Создает таблицы в ClickHouse (RAW-слой)
- Преобразует вложенные JSON-структуры в столбцы таблиц с типами полей String, для максимального сохранения поступивших данных.
- Сохраняет полные данные в поле `json_data`
- Отправляет данные в ClickHouse

Данные в "сыром слое" хранятся 180 дней с момента их загрузки в ClickHouse, а затем удаляются.

## Архитектура хранилища данных

#### Слой MART в ClickHouse

Для нормализации данных до 3NF созданы таблицы фактов и измерений (файл со скриптом [Script_mart.sql](Clickhouse_MART/Script_mart.sql)):

**Таблицы фактов**:
- `fact_purchases` - содержит информацию о транзакциях покупок (время, покупатель, магазин, суммы, метод оплаты)
- `fact_purchase_items` - содержит позиции товаров в каждой покупке (количество, цена и стоимость)

**Таблицы измерений**:
- `dim_customers` - информация о клиентах (персональные данные, предпочтения, участие в программе лояльности)
- `dim_products` - информация о продуктах (наименование, категория, цена, КБЖУ, производитель)
- `dim_stores` - информация о магазинах (название, сеть, тип, адрес, часы работы)
- `categories` - справочник категорий товаров
- `manufacturers` - справочник производителей
- `store_networks` - справочник торговых сетей
- `addresses` - справочник нормализованных адресов
- `store_categories` - таблица связи "многие-ко-многим" между магазинами и категориями
- `duplicate_analysis_results` - таблица с анализом дубликатов при загрузке данных

#### Особенности реализации

**Для таблиц измерений**:
- Используется движок `ReplacingMergeTree()`
- Старые данные сохраняются для исторического анализа

**Для таблиц фактов**:
- Используется `ReplacingMergeTree()` + TTL
- Записи старше 360 дней удаляются

**Проверка дублирования**:
- Реализуется через движок `ReplacingMergeTree()`

**Обработка данных**:
- Все строковые данные приводятся к нижнему регистру
- Удаляются лишние пробелы (trim())
- Пустые строки заменяются на NULL
- Проверяется корректность дат

**Приведение типов данных**:
- Ключевые поля имеют тип UInt64
- Строковые поля оставлены как String
- Числовые поля преобразованы в Float64 или UInt
- Булевы поля преобразованы в UInt8 (0/1)
- Даты и время преобразованы в Date и DateTime
- Координаты преобразованы в Float64

**Индексация**:
- Основные таблицы ссылаются на справочники через идентификаторы
- В некоторых таблицах реализован хеш-ключ (где ID не передается от заказчика)
- Выбран метод хеширования вместо UUID, так как он занимает меньше места на диске и стабильно работает с движком ReplacingMergeTree()

### Визуализация данных и алертинг в Grafana

#### Построение дашбордов

1. Для подключения к Grafana используется URL `localhost:3000`
2. Настроен источник данных Altinity plugin для ClickHouse (URL: `http://clickhouse:8123`)
3. Созданы визуализации, отображающие количество покупок, магазинов, покупателей и товаров в таблицах

Дашборд в Grafana
![Дашборд в Grafana](Photo/2.png)

#### Настройка алертинга для мониторинга дубликатов

1. В ClickHouse создана целевая таблица для результатов подсчета процента дубликатов
2. Создан Telegram-бот для отправки уведомлений:
   - Регистрация бота через @BotFather
   - Получение Chat ID через @userinfobot или @get_id_bot
3. В Grafana настроена точка контакта для отправки алертов в Telegram
4. Созданы дашборды для мониторинга дубликатов в каждой таблице

Настройка алертинга
![Настройка алертинга](Photo/3.png)
![Настройка алертинга](Photo/4.png)

5. Для каждого дашборда настроено правило оповещения:
   - Настроен порог срабатывания (более 50% дубликатов)
   - Сконфигурированы информативные сообщения для администраторов

Создание алертов</br>
![Создание алертов](Photo/5.png)</br>
Настройка уведомлений</br>
![Настройка уведомлений](Photo/6.png)</br>
![Настройка уведомлений](Photo/7.png)</br>
Обзор готового алерта</br>
![Обзор готового алерта](Photo/8.png)</br>
Панель алертов</br>
![Панель алертов](Photo/9.png)</br>
Пример уведомления в Telegram</br>
<img src="Photo/10.png" width="30%">


## Традиционный ETL на PySpark

Для решения бизнес-задач заказчика разработан ETL-процесс, который создает витрину данных на основе клиентских характеристик 
для проведения кластеризации покупателей. 
Процесс включает извлечение данных из хранилища ClickHouse, преобразование с расчетом матрицы признаков и загрузку результатов в S3.

#### Матрица признаков

Для каждого клиента формируется набор из 30 бинарных признаков (0/1), описывающих его покупательское поведение:

| №  | Признак                  | Описание                                                        |
|----|--------------------------|-----------------------------------------------------------------|
| 1  | bought_milk_last_30d     | Покупал молочные продукты за последние 30 дней                |
| 2  | bought_fruits_last_14d   | Покупал фрукты и ягоды за последние 14 дней                  |
| 3  | not_bought_veggies_14d   | Не покупал овощи и зелень за последние 14 дней                 |
| 4  | recurrent_buyer          | Делал более 2 покупок за последние 30 дней                     |
| 5  | inactive_14_30           | Не покупал 14–30 дней (ушедший клиент?)                        |
| 6  | new_customer             | Покупатель зарегистрировался менее 30 дней назад              |
| 7  | delivery_user            | Пользовался доставкой хотя бы раз                             |
| 8  | organic_preference       | Купил хотя бы 1 органический продукт                            |
| 9  | bulk_buyer               | Средняя корзина > 1000₽                                   |
| 10 | low_cost_buyer           | Средняя корзина < 200₽                                    |
| 11 | buys_bakery              | Покупал хлеб/выпечку хотя бы раз                             |
| 12 | loyal_customer           | Лояльный клиент (карта и ≥3 покупки)                      |
| 13 | multicity_buyer          | Делал покупки в разных городах                                |
| 14 | bought_meat_last_week   | Покупал мясо/рыбу/яйца за последнюю неделю                   |
| 15 | night_shopper            | Делал покупки после 20:00                                    |
| 16 | morning_shopper          | Делал покупки до 10:00                                    |
| 17 | prefers_cash             | Оплачивал наличными ≥ 70% покупок                            |
| 18 | prefers_card             | Оплачивал картой ≥ 70% покупок                               |
| 19 | weekend_shopper          | Делал ≥ 60% покупок в выходные                                |
| 20 | weekday_shopper          | Делал ≥ 60% покупок в будни                                  |
| 21 | single_item_buyer        | ≥50% покупок — 1 товар в корзине                               |
| 22 | varied_shopper           | Покупал ≥4 разных категорий продуктов                          |
| 23 | store_loyal              | Ходит только в один магазин                                  |
| 24 | switching_store          | Ходит в разные магазины                                      |
| 25 | family_shopper           | Среднее кол-во позиций в корзине ≥4                           |
| 26 | early_bird               | Покупка в промежутке между 12 и 15 часами дня                |
| 27 | no_purchases             | Не совершал ни одной покупки (только регистрация)            |
| 28 | recent_high_spender      | Купил на сумму >2000₽ за последние 7 дней                    |
| 29 | fruit_lover              | ≥3 покупок фруктов за 30 дней                                |
| 30 | vegetarian_profile       | Не купил ни одного мясного продукта за 90 дней              |

### Реализация ETL-процесса на PySpark
Для реализации ETL-процесса на PySpark выполнены следующие действия:
1. Создан [Dockerfile.jupyter](notebooks/Dockerfile.jupyter) с параметрами подключения к JupyterLab 
2. Добавлены компоненты JupyterLab в существующий docker-compose.yaml 
3. Для работы с S3 выполнена регистрация в сервисе Selectel `https://selectel.ru/services/cloud/storage/`
4. Создан файл для хранения параметров подключения [config.py](notebooks/config.py)
5. Для работы с PySpark используем файл [PySpark_ETL.ipynb](notebooks/PySpark_ETL.ipynb)

Для работы с JupyterLab необходимо в браузере перейти на страницу `http://localhost:8888`

Файл `PySpark_ETL.ipynb` выполняет следующие функции:
* Подключается к БД ClickHouse и S3
* Создает внешние таблицы
* Производит расчет матрицы признаков
* Выводит на экран первые 5 строк итогового df
* Формирует, именует с указанием даты и записывает единый CSV-файл такого типа: 
[feature_matrix_2025-10-05_15-20-35.csv](notebooks/feature_matrix_2025-10-05_15-20-35.csv)
* Загружает CSV-файл в S3

Пример загрузки в S3</br>
![Пример загрузки](Photo/11.png)</br>

## Cоздание обертки для ETL-процесса в Airflow

После сборки Docker-образа в корневой папке проекта будут созданы следующие директории: `dags`, `logs`, `airflow_config` и `plugins`. 
</br>Каждая из этих директорий играет важную роль в функционировании Airflow.

### 1. Доступ к веб-интерфейсу Airflow

После успешного запуска Docker-контейнеров, Airflow будет доступен через веб-интерфейс по адресу:

http://localhost:8080

Перейдем по этой ссылке в браузере для начала работы с Airflow.

### 2. Настройка переменных Airflow

Для корректной работы DAG необходимо задать несколько переменных, которые будут использоваться в процессе ETL.
</br>Настройки производятся через веб-интерфейс Airflow:

1.  В верхнем меню выбераем "**Admin**" -> "**Variables**".
2.  Нажимаем кнопку "**+ Create**" для добавления новой переменной.
3.  Вносим значения для каждой переменной, следуя таблице ниже.

| Ключ                   | Значение                                                      | Описание                                                 |
|------------------------|---------------------------------------------------------------|----------------------------------------------------------|
| `KAFKA_BROKER`         | `kafka:9092`                                                  | Адрес брокера Kafka (сервис `kafka`, порт `9092`)        |
| `KAFKA_GROUP`          | `clickhouse_group`                                            | Группа консюмеров Kafka                                  |
| `KAFKA_TOPICS`         | `products,stores,customers,purchases`                         | Перечень топиков Kafka, разделённых запятыми             |
| `CLICKHOUSE_HOST`      | `clickhouse`                                                  | Хост ClickHouse (сервис `clickhouse`)                    |
| `CLICKHOUSE_PORT`      | `9000`                                                        | Порт ClickHouse для Kafka-консьюмера                                         |
| `CLICKHOUSE_PORT_JDBC` | `8123`                                                        | Порт ClickHouse для jdbc-соединения                     |`     | `9000`                                                        | Порт ClickHouse                                                 |
| `CLICKHOUSE_USER`      | `clickhouse`                                                  | Пользователь ClickHouse                                  |
| `CLICKHOUSE_PASSWORD`  | `clickhouse`                                                  | Пароль ClickHouse (для production использовать Secrets!) |
| `CLICKHOUSE_DB`        | `clickhouse`                                                  | База данных ClickHouse                                   |
| `MONGO_URI`            | `mongodb://mongo:27017/`                                      | URI подключения к MongoDB (сервис `mongo`, порт `27017`) |
| `MONGO_DATABASE`       | `mongo_db`                                                    | Имя базы данных MongoDB                                  |
| `S3_ENDPOINT`          | `https://s3.ru-7.storage.selcloud.ru`                         | URL-адрес S3-совместимого хранилища                      |
| `S3_KEY_ID`            | `Ваши значения при регистрации`                               | Ключ доступа к S3                                        |
| `S3_SECRET`            | `Ваши значения при регистрации`                               | Секретный ключ доступа к S3                              |
| `S3_BUCKET`            | `data-engineer-practice-2025`                                 | Название бакета/контейнера в S3                          |
| `CLICKHOUSE_JAR_PATH`  | `/opt/spark-3.4.2-bin-hadoop3/jars/clickhouse-jdbc-0.6.3.jar` | Путь к JAR-файлу JDBC-драйвера для ClickHouse            |

**ВАЖНО:** В производственной среде для хранения конфиденциальных данных, таких как пароли, рекомендуется использовать Airflow Connections или Secrets Backend. Это обеспечит более безопасное хранение и управление sensitive-информацией.

#### Проверка добавленных переменных

После добавления всех переменных убедимся, что они корректно отображаются в списке.  Для этого перейдем в "**Admin**" -> "**Variables**" и проверим, что все внесённые переменные присутствуют в списке ("**List Variables**").
![List Variables](Photo/12.png)

### 3. Размещение DAG и файлов задач

1.  Создадим Python-файл с именем [retail_data_pipeline_DAG.py](dags/retail_data_pipeline_DAG.py) в директории проекта `dags/`.
2.  В директории `dags/` создадим поддиректорию `tasks/`.  Эта директория будет содержать файлы с кодом отдельных задач, выполняемых в рамках DAG.
3.  Разместим следующие файлы с кодом задач в директорию `dags/tasks/`:

    *   **Файлы для работы с Kafka:**
        *   [producer_task.py](dags/tasks/producer_task.py):  Содержит логику Python для продюсера Kafka (отправку данных в топики Kafka).
        *   [consumer_task.py](dags/tasks/consumer_task.py):  Содержит логику Python для консюмера Kafka (получение данных из топиков Kafka).
    *   **Файл для создания и наполнения таблиц условного "MART-слоя" в ClickHouse:**
        *   [clickhouse_mart_tasks.py](dags/tasks/clickhouse_mart_tasks.py): Содержит Python-код для создания таблиц и заполнения их данными из Kafka в ClickHouse.
    *   **Файл для создания витрины с признаками пользователей:**
        *   [feature_matrix_creator_task.py](dags/tasks/feature_matrix_creator_task.py): Содержит код для создания витрины с признаками пользователей, используя Apache Spark. Скрипт извлекает данные из ClickHouse, формирует признаки и загружает результат в S3.     

    Эти файлы-задачи будут последовательно вызываться и запускаться в рамках DAG, определённого в файле `retail_data_pipeline_DAG.py`.  

### 4. Проверка и запуск DAG

После размещения файлов DAG и задач можно приступать к проверке и запуску DAG в веб-интерфейсе Airflow.

1.  Перейдем на вкладку "**DAGs**".
2.  Находим DAG с именем "`retail_data_pipeline_DAG`".
3.  **Включение DAG:** Убедимся, что DAG активен. Переключатель (toggle) слева от имени DAG должен быть в активном положении (включен). Если он выключен, включаем его.
4.  **Запуск DAG вручную:** Для немедленного запуска DAG нажмаем на кнопку "**▶**" ("**Trigger DAG**").
5.  **Мониторинг выполнения:** Процесс выполнения DAG можно отслеживать в разделах "**Grid**" или "**Graph**".

При возникновении ошибок изучаем логи задач и вносим исправления в код.
</br>Процесс отладки DAG-файла:
![Процесс отладки](Photo/13.png)
![Процесс отладки](Photo/14.png)
</br>Раздел "Graph":
![Процесс отладки](Photo/15.png)
</br>Демонстрация загрузки файла в хранилище S3:
![Процесс отладки](Photo/16.png)

## Структура проекта

```
├── data
│   ├── customers
│   │   ├── customer_001.json
│   │   ├── ...
│   │   └── customer_045.json
│   ├── products
│   │   ├── product_001.json
│   │   ├── ...
│   │   └── product_020.json
│   ├── purchases
│   │   ├── purchase_001.json
│   │   ├── ...
│   │   └── purchase_200.json
│   └── stores
│       ├── store_001.json
│       ├── ...
│       └── store_045.json
├── dags
│   └── tasks
│   │   ├── producer_task.py
│   │   ├── consumer_task.py
│   │   ├── clickhouse_mart_tasks.py
│   │   └── feature_matrix_creator_task.py
│   └── retail_data_pipeline_DAG.py
├── load_data_mongodb
│   ├── .env
│   ├── check_mongo_data.py
│   └── load_data_mongo.py
├── mongodb_kafka_clickhouse
│   ├── .env
│   ├── producer.py
│   └── consumer.py
├── notebooks
│   ├── config.py
│   ├── feature_matrix_2025-10-05_15-20-35.csv
│   ├── feature_matrix_2025-10-05_15-20-39.csv
│   └── PySpark_ETL.ipynb
├── Clickhouse_MART
│   └── Script_mart.sql
├── Photo
│   └── screenshots...
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.jupyter
├── requirements.txt
├── clickhouse-jdbc-0.4.6-shaded.jar
└── generate_synthetic_data.py
```
Структура проекта включает:
- **data** - папка с тестовыми данные заказчика
  - **customers** - 45 JSON-файлов таблицы customers
  - **products** - 20 JSON-файлов таблицы products
  - **purchases** - 200 JSON-файлов таблицы purchases
  - **stores** - 45 JSON-файлов таблицы stores
- **dags** - папка с DAG-файлами для запуска задач в Airflow
  - **tasks** - папка с файлами задач DAGов
    - **producer_task.py** - код задачи для запуска producer 
    - **consumer_task.py** - код задачи для запуска consumer 
    - **clickhouse_mart_tasks.py** - код с задачами для создания и заполнения таблиц в ClickHouse
    - **feature_matrix_creator_task.py** - код задачи для создания витрины с признаками пользователей
  - **retail_data_pipeline_DAG.py** - DAG-файл для запуска файлов producer.py и consumer.py по расписанию
  - **tables_mart_DAG.py** - DAG-файл для заполнения таблиц в MART-слое
- **load_data_mongodb** - папка со скриптами для загрузки тестовых данных в MongoDB
  - **.env** - конфигурация подключения
  - **check_mongo_data.py** - скрипт для проверки данных в MongoDB
  - **load_data_mongo.py** - скрипт для загрузки данных
- **mongodb_kafka_clickhouse** - папка со скриптами для передачи данных из MongoDB в ClickHouse через Kafka
  - **.env** - конфигурация подключения
  - **consumer.py** - скрипт для загрузки данных в ClickHouse
  - **producer.py** - скрипт для считывания данных из MongoDB
- **Clickhouse_MART** - папка со скриптом для создания MART-слоя таблиц
  - **Script_mart.sql** - SQL-скрипт для обработки данных и загрузки в MART-таблицы
- **notebooks** - папка для работы с файлами JupyterLab
  - **config.py** - файл с параметров подключения к ClickHouse и S3
  - **feature_matrix_2025-10-05_15-20-35.csv** - сформированный файл csv
  - **feature_matrix_2025-10-05_15-20-39.csv** - сформированный файл csv
  - **PySpark_ETL.ipynb** - файл JupyterLab для работы c PySpark
- **Photo** - папка со скриншотами
- **docker-compose.yml** - конфигурация Docker
- **Dockerfile.airflow** - Dockerfile c параметрами подключения Airflow
- **Dockerfile.jupyter** - Dockerfile c параметрами подключения JupyterLab
- **requirements.txt** - файл c библиотеками для работы внутри Airflow
- **clickhouse-jdbc-0.4.6-shaded.jar** - файл для взаимодействия Spark с ClickHouse
- **generate_synthetic_data.py** - скрипт для генерации тестовых данных


### Автор:
Катерина Лаппа
