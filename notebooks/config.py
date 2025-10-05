# config.py
# --- Конфигурация ClickHouse ---
JDBC_DRIVER_PATH = "/usr/local/spark-3.5.0-bin-hadoop3/examples/jars/clickhouse-jdbc-0.4.6-shaded.jar"
CLICKHOUSE_JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default"
ACTUAL_CLICKHOUSE_DB_NAME = "clickhouse"
CLICKHOUSE_PROPERTIES = {
    "user": "clickhouse",
    "password": "clickhouse",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# --- Конфигурация S3 ---
KEY_ID="5be17b2439d34f49855e97da0d04971a"
SECRET="892ed85050124d0f9e644037f8a9dd3b"
ENDPOINT="https://s3.ru-7.storage.selcloud.ru"
CONTAINER="data-engineer-practice-2025"