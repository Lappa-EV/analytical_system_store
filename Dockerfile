FROM jupyter/pyspark-notebook:latest

USER root

# Убедимся, что wget есть, хотя в jupyter/pyspark-notebook он обычно есть.
# Если нет, эта строка поставит его.
RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Загрузка JDBC драйвера ClickHouse
# Используем конкретную версию для большей стабильности, например 0.4.6.
# Проверяйте на https://github.com/ClickHouse/clickhouse-jdbc/releases/latest/ для самой свежей стабильной.
# Или используйте ссылку на maven.
ARG CLICKHOUSE_JDBC_VERSION="0.4.6"
RUN wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-shaded.jar -O /opt/spark/jars/clickhouse-jdbc-shaded.jar

# Установка дополнительных библиотек Python
RUN pip install --no-cache-dir pandas scikit-learn matplotlib seaborn requests beautifulsoup4 clickhouse-driver

# Вернуться к пользователю jovyan
USER $NB_UID

# Рабочая директория
WORKDIR /home/jovyan/work
