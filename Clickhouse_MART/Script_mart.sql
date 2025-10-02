-----------------------------------------------Создание таблиц DWH и их MV --------------------------------------------
-------------------------------------------------1. Справочник адресов---------------------------------------------------------------
-- Таблица addresses (без изменений)
CREATE TABLE clickhouse.addresses (
    address_id UInt64,
    country Nullable(String),
    city Nullable(String),
    street Nullable(String),
    house Nullable(String),
    apartment Nullable(String),
    postal_code Nullable(String),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (country, city, street, house, apartment, postal_code, latitude, longitude)
SETTINGS allow_nullable_key = 1;

-- Материализованное представление для addresses из таблицы customers
CREATE MATERIALIZED VIEW clickhouse.mv_delivery_addresses_from_customers
TO clickhouse.addresses
AS
SELECT
    cityHash64(
        coalesce(nullIf(lowerUTF8(trim(c.delivery_address_country)), ''), ''),
        coalesce(nullIf(lowerUTF8(trim(c.delivery_address_city)), ''), ''),
        coalesce(nullIf(lowerUTF8(trim(c.delivery_address_street)), ''), ''),
        coalesce(nullIf(lowerUTF8(trim(c.delivery_address_house)), ''), ''),
        coalesce(nullIf(lowerUTF8(trim(c.delivery_address_apartment)), ''), ''),
        coalesce(trim(c.delivery_address_postal_code), ''),
        coalesce(toString(toFloat64OrNull(trim(c.purchase_location_coordinates_latitude))), ''),
        coalesce(toString(toFloat64OrNull(trim(c.purchase_location_coordinates_longitude))), '')
    ) AS address_id,
    nullIf(lowerUTF8(trim(c.delivery_address_country)), '') AS country,
    nullIf(lowerUTF8(trim(c.delivery_address_city)), '') AS city,
    nullIf(lowerUTF8(trim(c.delivery_address_street)), '') AS street,
    nullIf(lowerUTF8(trim(c.delivery_address_house)), '') AS house,
    nullIf(lowerUTF8(trim(c.delivery_address_apartment)), '') AS apartment,
    trim(c.delivery_address_postal_code) AS postal_code,
    toFloat64OrNull(trim(c.purchase_location_coordinates_latitude)) AS latitude,
    toFloat64OrNull(trim(c.purchase_location_coordinates_longitude)) AS longitude,
    c.event_time
FROM clickhouse.customers c
WHERE c.delivery_address_country IS NOT NULL;

-- Материализованное представление для addresses из таблицы stores
CREATE MATERIALIZED VIEW clickhouse.mv_addresses_from_stores
TO clickhouse.addresses
AS
SELECT
    cityHash64(
        coalesce(nullIf(lowerUTF8(trim(coalesce(s.location_country, ''))), ''), ''),
        coalesce(nullIf(lowerUTF8(trim(coalesce(s.location_city, ''))), ''), ''),
        coalesce(nullIf(lowerUTF8(trim(coalesce(s.location_street, ''))), ''), ''),
        coalesce(nullIf(lowerUTF8(trim(coalesce(s.location_house, ''))), ''), ''),
        '',  -- apartment is always NULL for stores
        coalesce(trim(s.location_postal_code), ''),
        coalesce(toString(toFloat64OrNull(trim(s.location_coordinates_latitude))), ''),
        coalesce(toString(toFloat64OrNull(trim(s.location_coordinates_longitude))), '')
    ) AS address_id,
    nullIf(lowerUTF8(trim(coalesce(s.location_country, ''))), '') AS country,
    nullIf(lowerUTF8(trim(coalesce(s.location_city, ''))), '') AS city,
    nullIf(lowerUTF8(trim(coalesce(s.location_street, ''))), '') AS street,
    nullIf(lowerUTF8(trim(coalesce(s.location_house, ''))), '') AS house,
    CAST(NULL AS Nullable(String)) AS apartment,
    trim(s.location_postal_code) AS postal_code,
    toFloat64OrNull(trim(s.location_coordinates_latitude)) AS latitude,
    toFloat64OrNull(trim(s.location_coordinates_longitude)) AS longitude,
    s.event_time
FROM stores s
WHERE s.location_country IS NOT NULL;


-------------------------------------------------------2. Справочник категорий товаров ----------------------------------------------------------------------
-- Дедупликация данных по category_name
CREATE TABLE clickhouse.categories (
    category_id UInt64,
    category_name String,
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (category_name);

-- MV для таблицы categories
-- Материализованное представление для categories из таблицы products
CREATE MATERIALIZED VIEW clickhouse.mv_categories_from_products
TO clickhouse.categories
AS
SELECT
    cityHash64(
        trim(replaceRegexpAll(nullIf(trim(lowerUTF8(p.group)), ''), CAST('^[\\x{1F300}-\\x{1F5FF}\\x{1F900}-\\x{1F9FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{FE00}-\\x{FE0F}]+' AS String), ''))
    ) AS category_id,
    trim(replaceRegexpAll(nullIf(trim(lowerUTF8(p.group)), ''), CAST('^[\\x{1F300}-\\x{1F5FF}\\x{1F900}-\\x{1F9FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{FE00}-\\x{FE0F}]+' AS String), '')) AS category_name,
    p.event_time
FROM clickhouse.products p
WHERE nullIf(trim(lowerUTF8(p.group)), '') IS NOT NULL;


-----------------------------------------------------------3. Справочник производителей-----------------------------------------------------------------------
-- Дедупликация данных по inn
CREATE TABLE clickhouse.manufacturers (
    inn String,
    manufacturer_name Nullable(String),
    manufacturer_country Nullable(String),
    manufacturer_website Nullable(String),
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (inn);

-- MV для таблицы manufacturers
CREATE MATERIALIZED VIEW clickhouse.mv_manufacturers_from_products
TO clickhouse.manufacturers
AS
SELECT
    nullIf(trim(coalesce(p.manufacturer_inn, '')), '') AS inn,
    nullIf(trim(lowerUTF8(coalesce(p.manufacturer_name, ''))), '') AS manufacturer_name,
    nullIf(trim(lowerUTF8(coalesce(p.manufacturer_country, ''))), '') AS manufacturer_country,
    nullIf(trim(lowerUTF8(p.manufacturer_website)), '') AS manufacturer_website,
    p.event_time
FROM clickhouse.products p
WHERE
    nullIf(trim(coalesce(p.manufacturer_inn, '')), '') IS NOT NULL
    AND nullIf(trim(coalesce(p.manufacturer_name, '')), '') IS NOT NULL;


---------------------------------------------------------4. Справочник торговых сетей--------------------------------------------------------------------------
-- Таблица store_networks
CREATE TABLE clickhouse.store_networks (
    store_network_id UInt64,
    store_network_name String,
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (store_network_name);

-- MV для таблицы store_networks
CREATE MATERIALIZED VIEW clickhouse.mv_store_networks_from_stores
TO clickhouse.store_networks
AS
SELECT
    cityHash64(normalized_name) AS store_network_id, -- Генерируем стабильный ID
    normalized_name AS store_network_name,
    event_time
FROM (
    SELECT
        s.event_time,
        nullIf(lowerUTF8(trimBoth(s.store_network)), '') AS normalized_name,
        row_number() OVER (PARTITION BY s.store_id ORDER BY s.event_time DESC) AS rn
    FROM clickhouse.stores s
    WHERE s.store_network IS NOT NULL AND trimBoth(s.store_network) != ''
) AS filtered
WHERE rn = 1 AND normalized_name IS NOT NULL;


--------------------------------------------------------5. Справочник менеджеров магазинов----------------------------------------------------------------------
-- Дедупликация данных по manager_name и manager_phone
CREATE TABLE clickhouse.store_managers (
    manager_id UInt64,
    manager_name String,
    manager_phone String,
    manager_email String,
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (manager_name, manager_phone)
SETTINGS allow_nullable_key = 1;

-- MV для таблицы store_managers
-- Материализованное представление для store_managers из таблицы stores
CREATE MATERIALIZED VIEW clickhouse.mv_store_managers_from_stores
TO clickhouse.store_managers
AS
SELECT
    -- Генерируем хэш-ключ manager_id на основе manager_name и manager_phone.
    cityHash64(coalesce(normalized_manager_name, ''), coalesce(normalized_manager_phone, '')) AS manager_id,
    normalized_manager_name AS manager_name,
    normalized_manager_phone AS manager_phone,
    normalized_manager_email AS manager_email,
    event_time
FROM (
    SELECT
        s.event_time,
        nullIf(trim(lowerUTF8(s.manager_name)), '') AS normalized_manager_name,
        nullIf(trim(s.manager_phone), '') AS normalized_manager_phone,
        nullIf(trim(lowerUTF8(s.manager_email)), '') AS normalized_manager_email
    FROM clickhouse.stores s
    WHERE
        (nullIf(trim(s.manager_name), '') IS NOT NULL OR nullIf(trim(s.manager_phone), '') IS NOT NULL)
) AS normalized_managers
WHERE normalized_manager_name IS NOT NULL OR normalized_manager_phone IS NOT NULL;


---------------------------------------------------------6. Таблица измерений покупателей--------------------------------------------------------------------
-- Дедупликация данных по customer_id и loyalty_card_number
CREATE TABLE clickhouse.dim_customers (
    customer_id String,
    first_name String,
    last_name Nullable(String),
    email Nullable(String),
    phone Nullable(String),
    birth_date Nullable(Date),
    gender Nullable(Enum8('male' = 1, 'female' = 2)),
    registration_date DateTime,
    is_loyalty_member Bool,
    loyalty_card_number String,
    address_id Nullable(UInt64),
    delivery_address_id Nullable(UInt64),
    preferred_language String, -- по умолчанию ru
    preferred_payment_method String,
    receive_promotions Bool,
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (customer_id, loyalty_card_number);

-- MV для таблицы dim_customers
-- Материализованное представление для dim_customers из таблицы customers
CREATE MATERIALIZED VIEW clickhouse.mv_dim_customers_from_customers
TO clickhouse.dim_customers
AS
SELECT
    trim(lowerUTF8(c.customer_id)) AS customer_id,
    lowerUTF8(trim(c.first_name)) AS first_name,
    nullIf(lowerUTF8(trim(c.last_name)), '') AS last_name,
    nullIf(lowerUTF8(trim(c.email)), '') AS email,
    nullIf(trim(c.phone), '') AS phone,
    toDateOrNull(c.birth_date) AS birth_date,
    multiIf(lowerUTF8(trim(c.gender)) = 'male', toInt8(1), lowerUTF8(trim(c.gender)) = 'female', toInt8(2), NULL) AS gender,
    parseDateTime64BestEffort(c.registration_date) AS registration_date,
    toUInt8(lowerUTF8(trim(c.is_loyalty_member)) = 'true') AS is_loyalty_member,
    trim(lowerUTF8(c.loyalty_card_number)) AS loyalty_card_number,
    COALESCE(addr.address_id, NULL) AS address_id,
    COALESCE(daddr.address_id, NULL) AS delivery_address_id,
    lowerUTF8(trim(c.preferences_preferred_language)) AS preferred_language,
    lowerUTF8(trim(c.preferences_preferred_payment_method)) AS preferred_payment_method,
    toUInt8(lowerUTF8(trim(c.preferences_receive_promotions)) = 'true') AS receive_promotions,
    c.event_time AS event_time
FROM clickhouse.customers c
LEFT JOIN clickhouse.addresses addr ON
    lowerUTF8(trim(ifNull(addr.country, ''))) = lowerUTF8(trim(ifNull(c.purchase_location_country, ''))) AND
    lowerUTF8(trim(ifNull(addr.city, ''))) = lowerUTF8(trim(ifNull(c.purchase_location_city, ''))) AND
    lowerUTF8(trim(ifNull(addr.street, ''))) = lowerUTF8(trim(ifNull(c.purchase_location_street, ''))) AND
    lowerUTF8(trim(ifNull(addr.house, ''))) = lowerUTF8(trim(ifNull(c.purchase_location_house, ''))) AND
    lowerUTF8(trim(ifNull(addr.postal_code, ''))) = lowerUTF8(trim(ifNull(c.purchase_location_postal_code, '')))
LEFT JOIN clickhouse.addresses daddr ON
    lowerUTF8(trim(ifNull(daddr.country, ''))) = lowerUTF8(trim(ifNull(c.delivery_address_country, ''))) AND
    lowerUTF8(trim(ifNull(daddr.city, ''))) = lowerUTF8(trim(ifNull(c.delivery_address_city, ''))) AND
    lowerUTF8(trim(ifNull(daddr.street, ''))) = lowerUTF8(trim(ifNull(c.delivery_address_street, ''))) AND
    lowerUTF8(trim(ifNull(daddr.house, ''))) = lowerUTF8(trim(ifNull(c.delivery_address_house, ''))) AND
    lowerUTF8(trim(ifNull(daddr.apartment, ''))) = lowerUTF8(trim(ifNull(c.delivery_address_apartment, ''))) AND
    lowerUTF8(trim(ifNull(daddr.postal_code, ''))) = lowerUTF8(trim(ifNull(c.delivery_address_postal_code, '')))
WHERE trim(c.customer_id) != '' AND trim(c.loyalty_card_number) != '';


------------------------------------------------------------------7. Таблица измерений товаров-----------------------------------------------------------------------
-- Дедупликация данных по product_id
CREATE TABLE clickhouse.dim_products (
    product_id String,
    name String,
    category_id Nullable(UInt64),
    description Nullable(String),
    calories Nullable(Float32),
    protein Nullable(Float32),
    fat Nullable(Float32),
    carbohydrates Nullable(Float32),
    price Decimal(16, 2),
    unit String,
    origin_country String,
    expiry_days Int16,
    is_organic Bool,
    barcode String,
    manufacturer_id Nullable(String),
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (product_id);

-- MV для таблицы dim_product
-- Материализованное представление для dim_products из таблицы products
CREATE MATERIALIZED VIEW clickhouse.mv_dim_products_from_products
TO clickhouse.dim_products
AS
SELECT
   nullIf(lowerUTF8(trim(p.id)), '') AS product_id,
   lowerUTF8(trim(p.name)) AS name,
   c.category_id AS category_id,
   nullIf(lowerUTF8(trim(p.description)), '') AS description,
   toFloat32OrNull(nullIf(trim(p.kbju_calories), '')) AS calories,
   toFloat32OrNull(nullIf(trim(p.kbju_protein), '')) AS protein,
   toFloat32OrNull(nullIf(trim(p.kbju_fat), '')) AS fat,
   toFloat32OrNull(nullIf(trim(p.kbju_carbohydrates), '')) AS carbohydrates,
   toDecimal128(trim(p.price), 2) AS price,
   lowerUTF8(trim(p.unit)) AS unit,
   lowerUTF8(trim(p.origin_country)) AS origin_country,
   coalesce(toInt16OrNull(nullIf(trim(p.expiry_days), '')), 0) AS expiry_days,
   toUInt8(lowerUTF8(trim(p.is_organic)) = 'true') AS is_organic,
   lowerUTF8(trim(p.barcode)) AS barcode,
   -- Просто копируем значение String из m.inn в manufacturer_id
   trim(m.inn) AS manufacturer_id,
   p.event_time AS event_time
FROM clickhouse.products p
LEFT JOIN clickhouse.manufacturers m
ON lowerUTF8(trim(ifNull(p.manufacturer_name, ''))) = lowerUTF8(trim(ifNull(m.manufacturer_name, '')))
   AND lowerUTF8(trim(ifNull(p.manufacturer_country, ''))) = lowerUTF8(trim(ifNull(m.manufacturer_country, '')))
   AND lowerUTF8(trim(ifNull(p.manufacturer_website, ''))) = lowerUTF8(trim(ifNull(m.manufacturer_website, '')))
   AND trim(ifNull(p.manufacturer_inn, '')) = trim(ifNull(m.inn, ''))
LEFT JOIN clickhouse.categories c
ON trim(lowerUTF8(normalizeUTF8NFC(replaceRegexpAll(replaceRegexpAll(p.group, CAST('[\\x{1F300}-\\x{1F5FF}\\x{1F900}-\\x{1F9FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{FE00}-\\x{FE0F}]+' AS String), ''), '[^\\p{L}\\p{N}\\s,]+', '')))) = trim(lowerUTF8(c.category_name))
WHERE nullIf(lowerUTF8(trim(p.id)), '') IS NOT NULL;


---------------------------------------------------------------8. Таблица измерений магазинов dim_stores-----------------------------------------------------------------
-- Дедупликация данных по store_id
CREATE TABLE clickhouse.dim_stores (
    store_id String,
    store_name String,
    store_network_id Nullable(UInt64),
    store_type_description String,
    type String,
    manager_id Nullable(UInt64),
    address_id Nullable(UInt64),
    opening_hours_mon_fri String,
    opening_hours_sat String,
    opening_hours_sun String,
    accepts_online_orders Bool,
    delivery_available Bool,
    warehouse_connected Bool,
    last_inventory_date Date,
    event_time DateTime
)  ENGINE = ReplacingMergeTree(event_time)
ORDER BY (store_id)

-- MV для таблицы dim_stores
-- Материализованное представление для dim_stores из таблицы stores
CREATE MATERIALIZED VIEW clickhouse.mv_dim_stores_from_stores
TO clickhouse.dim_stores
AS
SELECT
    nullIf(lowerUTF8(trim(s.store_id)), '') AS store_id,
    nullIf(lowerUTF8(trim(s.store_name)), '') AS store_name,
    sn.store_network_id AS store_network_id,
    nullIf(lowerUTF8(trim(s.store_type_description)), '') AS store_type_description,
    nullIf(lowerUTF8(trim(s.type)), '') AS type,
    sm.manager_id AS manager_id,
    a.address_id AS address_id,
    nullIf(lowerUTF8(trim(s.opening_hours_mon_fri)), '') AS opening_hours_mon_fri,
    nullIf(lowerUTF8(trim(s.opening_hours_sat)), '') AS opening_hours_sat,
    nullIf(lowerUTF8(trim(s.opening_hours_sun)), '') AS opening_hours_sun,
    (lowerUTF8(trim(s.accepts_online_orders)) = 'true') AS accepts_online_orders,
    (lowerUTF8(trim(s.delivery_available)) = 'true') AS delivery_available,
    (lowerUTF8(trim(s.warehouse_connected)) = 'true') AS warehouse_connected,
    toDateOrNull(s.last_inventory_date) AS last_inventory_date,
    s.event_time AS event_time
FROM clickhouse.stores AS s
LEFT JOIN clickhouse.store_networks AS sn ON nullIf(lowerUTF8(trim(s.store_network)), '') = sn.store_network_name
LEFT JOIN clickhouse.store_managers AS sm ON
    (nullIf(lowerUTF8(trim(s.manager_name)), '') = sm.manager_name OR (s.manager_name IS NULL AND sm.manager_name IS NULL)) AND
    (nullIf(trim(s.manager_phone), '') = sm.manager_phone OR (s.manager_phone IS NULL AND sm.manager_phone IS NULL)) AND
    (nullIf(lowerUTF8(trim(s.manager_email)), '') = sm.manager_email OR (s.manager_email IS NULL AND sm.manager_email IS NULL))
LEFT JOIN clickhouse.addresses AS a ON
    (nullIf(lowerUTF8(trim(s.location_country)), '') = a.country OR (s.location_country IS NULL AND a.country IS NULL)) AND
    (nullIf(lowerUTF8(trim(s.location_city)), '') = a.city OR (s.location_city IS NULL AND a.city IS NULL)) AND
    (nullIf(lowerUTF8(trim(s.location_street)), '') = a.street OR (s.location_street IS NULL AND a.street IS NULL)) AND
    (nullIf(lowerUTF8(trim(s.location_house)), '') = a.house OR (s.location_house IS NULL AND a.house IS NULL)) AND
    (if(length(trim(s.location_postal_code)) = 6, trim(s.location_postal_code), NULL) = a.postal_code OR (s.location_postal_code IS NULL AND a.postal_code IS NULL)) AND
    (a.apartment IS NULL) AND
    (toFloat64OrNull(trim(s.location_coordinates_latitude)) = a.latitude OR (s.location_coordinates_latitude IS NULL AND a.latitude IS NULL)) AND
    (toFloat64OrNull(trim(s.location_coordinates_longitude)) = a.longitude OR (s.location_coordinates_longitude IS NULL AND a.longitude IS NULL))
WHERE
    nullIf(trim(s.store_id), '') IS NOT NULL;


-----------------------------------------9. Таблица связи многие-ко-многим между магазинами и категориями store_categories---------------------------------------------
-- Дедупликация данных по store_id и category_id
CREATE TABLE clickhouse.store_categories (
    store_id String,
    category_id UInt64,
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (store_id, category_id);

-- MV для таблицы store_categories
-- Материализованное представление для таблицы связи многие-ко-многим между
CREATE MATERIALIZED VIEW clickhouse.mv_store_categories_from_stores
TO clickhouse.store_categories
AS
SELECT
    s.store_id,
    if(c.category_id IS NULL, 0, c.category_id) AS category_id,
    s.event_time
FROM (
    SELECT
        store_id,
        categories,
        event_time,
        arrayFilter(
            x -> notEmpty(x),
            arrayMap(
                x -> replaceRegexpAll(trim(x), ',+$', ''), -- Удаляем запятые в конце строки
                extractAll(
                    replaceRegexpAll(categories, CAST('[\\x{1F300}-\\x{1F5FF}\\x{1F900}-\\x{1F9FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{FE00}-\\x{FE0F}]+' AS String), ''),
                    '([А-ЯЁA-Z][а-яё0-9\\s&,]*|[A-Z][a-z0-9\\s&,]*)' -- Разрешаем запятые внутри категории
                )
            )
        ) AS category_names
    FROM clickhouse.stores
) AS s
ARRAY JOIN s.category_names AS category_name
LEFT JOIN clickhouse.categories AS c ON trim(lowerUTF8(category_name)) = trim(c.category_name)
WHERE
    s.store_id IS NOT NULL;


----------------------------------------------------------10. Таблица фактов покупок fact_purchases-------------------------------------------------------------
-- Дедупликация данных по purchase_id
CREATE TABLE clickhouse.fact_purchases (
    purchase_id String,
    customer_id String,
    store_id String,
    total_amount Decimal(16, 2),
    payment_method Nullable(String),
    is_delivery Nullable(Bool),
    delivery_address_id Nullable(UInt64),
    purchase_datetime DateTime,
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(purchase_datetime)
ORDER BY (purchase_id, purchase_datetime)
TTL event_time + INTERVAL 360 DAY;

-- MV для таблицы fact_purchases
-- Материализованное представление для fact_purchases из таблицы purchases
CREATE MATERIALIZED VIEW clickhouse.mv_fact_purchases
TO clickhouse.fact_purchases
AS
SELECT
    nullIf(lowerUTF8(trim(p.purchase_id)), '') AS purchase_id,
    nullIf(lowerUTF8(trim(p.customer_customer_id)), '') AS customer_id,
    nullIf(lowerUTF8(trim(p.store_store_id)), '') AS store_id,
    toDecimal64OrNull(nullIf(trim(p.total_amount), ''), 2) AS total_amount,
    nullIf(lowerUTF8(trim(p.payment_method)), '') AS payment_method,
    CASE
        WHEN lowerUTF8(trim(p.is_delivery)) IN ('true', '1', 'yes') THEN true
        WHEN lowerUTF8(trim(p.is_delivery)) IN ('false', '0', 'no') THEN false
        ELSE NULL
    END AS is_delivery,
    a.address_id AS delivery_address_id,
    parseDateTimeBestEffortOrNull(nullIf(trim(p.purchase_datetime), '')) AS purchase_datetime,
    p.event_time AS event_time
FROM purchases AS p
LEFT JOIN clickhouse.addresses AS a
ON
    nullIf(lowerUTF8(trim(p.delivery_address_country)), '') = a.country AND
    nullIf(lowerUTF8(trim(p.delivery_address_city)), '') = a.city AND
    nullIf(lowerUTF8(trim(p.delivery_address_street)), '') = a.street AND
    nullIf(lowerUTF8(trim(p.delivery_address_house)), '') = a.house AND
    nullIf(lowerUTF8(trim(p.delivery_address_apartment)), '') = a.apartment AND
    if(length(trim(p.delivery_address_postal_code)) = 6, trim(p.delivery_address_postal_code), NULL) = a.postal_code
WHERE
    nullIf(trim(p.purchase_id), '') IS NOT NULL AND
    nullIf(lowerUTF8(trim(p.customer_customer_id)), '') IS NOT NULL AND  -- Отбрасываем строки с пустым customer_id
    nullIf(lowerUTF8(trim(p.store_store_id)), '') IS NOT NULL AND     -- Отбрасываем строки с пустым store_id
    toDecimal64OrNull(nullIf(trim(p.total_amount), ''), 2) IS NOT NULL AND -- Отбрасываем строки с невалидным total_amount
    parseDateTimeBestEffortOrNull(nullIf(trim(p.purchase_datetime), '')) IS NOT NULL; -- Отбрасываем строки с невалидной датой


----------------------------------------------------11. Таблица фактов деталей покупок fact_purchase_items ----------------------------------------------------
-- Дедупликация данных по purchase_id и product_id
CREATE TABLE clickhouse.fact_purchase_items (
    purchase_id String,
    product_id String,
    quantity Nullable(Int32),
    unit Nullable(String),
    price_per_unit Nullable(Decimal(16, 2)),
    total_price Nullable(Decimal(16, 2)),
    purchase_datetime DateTime,
    event_time DateTime
) ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(purchase_datetime)
ORDER BY (purchase_id, product_id)
TTL event_time + INTERVAL 360 DAY

-- MV для таблицы fact_purchase_items
-- Материализованное представление для fact_purchase_items из таблицы purchases
CREATE MATERIALIZED VIEW clickhouse.mv_fact_purchase_items
TO clickhouse.fact_purchase_items
AS
SELECT
    nullIf(trim(p.purchase_id), '') AS purchase_id,
    lowerUTF8(nullIf(trim(p.items_product_id), '')) AS product_id,
    toInt32OrNull(nullIf(trim(p.items_quantity), '')) AS quantity,
    lowerUTF8(nullIf(trim(p.items_unit), '')) AS unit,
    toDecimal64OrNull(nullIf(trim(p.items_price_per_unit), ''), 2) AS price_per_unit,
    toDecimal64OrNull(nullIf(trim(p.items_total_price), ''), 2) AS total_price,
    parseDateTimeBestEffortOrNull(nullIf(trim(p.purchase_datetime), '')) AS purchase_datetime,
    p.event_time
FROM purchases AS p
WHERE
    nullIf(trim(p.purchase_id), '') IS NOT NULL AND                    -- purchase_id не может быть NULL
    nullIf(trim(p.items_product_id), '') IS NOT NULL AND               -- product_id не может быть NULL
    parseDateTimeBestEffortOrNull(nullIf(trim(p.purchase_datetime), '')) IS NOT NULL; -- purchase_datetime не может быть NULL


------------------------------------ 12. Таблица для хранения результатов количества дубликатов новых поступивших (последних) данных------------------------------------
-- Должна заполняться либо вручную либо планировщиком
CREATE TABLE clickhouse.duplicate_analysis_results (
    event_time DateTime,
    duplicate_products Int16,
    duplicate_stores Int16,
    duplicate_customers Int16,
    duplicate_purchases Int16
) ENGINE = MergeTree()
ORDER BY toDate(event_time)
TTL event_time + INTERVAL 360 DAY;

-- 12. Заполнение таблицы duplicate_analysis_results
INSERT INTO clickhouse.duplicate_analysis_results (event_time, duplicate_products, duplicate_stores, duplicate_customers, duplicate_purchases)
SELECT
    toStartOfDay(now()) AS event_time,(
    	SELECT toInt16(if(count(id) = 0, 0, round((1 - uniq(id) / count(id)) * 100)))
        FROM clickhouse.products
        WHERE toDate(event_time) = today()
     ) AS duplicate_products,
    (SELECT toInt16(if(count(store_id) = 0, 0, round((1 - uniq(store_id) / count(store_id)) * 100)))
     FROM clickhouse.stores
     WHERE toDate(event_time) = today()
     ) AS duplicate_stores,
    (SELECT toInt16(if(count(customer_id) = 0, 0, round((1 - uniq(customer_id) / count(customer_id)) * 100)))
     FROM clickhouse.customers WHERE toDate(event_time) = today()
     ) AS duplicate_customers,
    (SELECT toInt16(if(count(purchase_id) = 0, 0, round((1 - uniq(purchase_id) / count(purchase_id)) * 100)))
     FROM clickhouse.purchases
	 WHERE toDate(event_time) = today()
	) AS duplicate_purchases;

