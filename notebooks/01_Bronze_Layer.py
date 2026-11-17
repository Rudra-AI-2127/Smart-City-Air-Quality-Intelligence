# Databricks notebook source

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "air_quality_volume"

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/raw"

print("Raw data location:", RAW_PATH)

display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------


datasets = [
    "city_day",
    "city_hour",
    "station_day",
    "station_hour",
    "stations"
]

dataframes = {}

for name in datasets:
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_PATH}/{name}.csv")
    )

    dataframes[name] = df

    print(f"{name}: {df.count()} rows")
    print(f"Columns: {len(df.columns)}")


# COMMAND ----------


for name, df in dataframes.items():

    table_name = f"{CATALOG}.{SCHEMA}.bronze_{name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

    print(f"Created: {table_name}")


# COMMAND ----------


for name in datasets:

    table_name = f"{CATALOG}.{SCHEMA}.bronze_{name}"

    df = spark.table(table_name)

    print(f"{table_name}: {df.count()} rows")

display(
    spark.table("workspace.default.bronze_city_day")
    .limit(10)
)


# COMMAND ----------

