# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

CATALOG = "workspace"
SCHEMA = "default"

datasets = [
    "city_day",
    "city_hour",
    "station_day",
    "station_hour",
    "stations"
]

bronze = {}

for name in datasets:
    bronze[name] = spark.table(
        f"{CATALOG}.{SCHEMA}.bronze_{name}"
    )

    print(f"Loaded: bronze_{name}")


# COMMAND ----------


from pyspark.sql import functions as F

for name, df in bronze.items():

    print(f"\n========== DATASET: {name} ==========")

    missing = df.select([
        F.sum(
            F.when(
                F.col(f"`{c}`").isNull() |
                (
                    F.trim(
                        F.col(f"`{c}`").cast("string")
                    ) == ""
                ),
                1
            ).otherwise(0)
        ).alias(c)
        for c in df.columns
    ])

    display(missing)


# COMMAND ----------


from pyspark.sql import functions as F

pollutant_columns = [
    "PM2.5", "PM10", "NO", "NO2", "NOx",
    "NH3", "CO", "SO2", "O3",
    "Benzene", "Toluene", "Xylene", "AQI"
]

silver = {}

for name, df in bronze.items():

    print(f"\nCleaning dataset: {name}")

    # 1. Standardize text columns
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            col_ref = F.col(f"`{col_name}`")

            df = df.withColumn(
                col_name,
                F.when(
                    F.trim(col_ref) == "", None
                ).otherwise(F.trim(col_ref))
            )

    # 2. Convert pollutants to numeric
    # and replace negative values with null
    for col_name in pollutant_columns:
        if col_name in df.columns:

            numeric = F.col(f"`{col_name}`").cast("double")

            df = df.withColumn(
                col_name,
                F.when(
                    numeric >= 0, numeric
                ).otherwise(None)
            )

    # 3. Standardize dates
    if "Date" in df.columns:
        df = df.withColumn(
            "Date",
            F.to_date(F.col("`Date`"))
        )

    if "Datetime" in df.columns:
        df = df.withColumn(
            "Datetime",
            F.to_timestamp(F.col("`Datetime`"))
        )

    # 4. Remove identical duplicate records
    df = df.dropDuplicates()

    silver[name] = df

    print(f"Cleaning completed: {name}")


# COMMAND ----------


for name, df in silver.items():

    table_name = f"workspace.default.silver_{name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Created: {table_name}")


# COMMAND ----------


for name in silver:

    bronze_count = bronze[name].count()

    silver_count = spark.table(
        f"workspace.default.silver_{name}"
    ).count()

    print(
        f"{name}: "
        f"Bronze={bronze_count}, "
        f"Silver={silver_count}, "
        f"Removed={bronze_count - silver_count}"
    )


# COMMAND ----------


from pyspark.sql import functions as F

business_keys = {
    "city_day": ["City", "Date"],
    "city_hour": ["City", "Datetime"],
    "station_day": ["StationId", "Date"],
    "station_hour": ["StationId", "Datetime"],
    "stations": ["StationId"]
}

for name, keys in business_keys.items():

    df = spark.table(
        f"workspace.default.silver_{name}"
    )

    if all(k in df.columns for k in keys):

        duplicates = (
            df.groupBy(*[F.col(f"`{k}`") for k in keys])
            .count()
            .filter(F.col("count") > 1)
        )

        print(
            f"{name}: "
            f"{duplicates.count()} duplicate key groups"
        )

    else:
        print(f"{name}: Key columns not found")


# COMMAND ----------


for name in [
    "city_day",
    "city_hour",
    "station_day",
    "station_hour"
]:

    df = spark.table(
        f"workspace.default.silver_{name}"
    )

    date_col = (
        "Datetime"
        if "Datetime" in df.columns
        else "Date"
    )

    print(f"\nDataset: {name}")

    df.select(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                F.col(f"`{date_col}`").isNull(), 1
            ).otherwise(0)
        ).alias("missing_dates"),
        F.min(F.col(f"`{date_col}`")).alias("first_date"),
        F.max(F.col(f"`{date_col}`")).alias("last_date"),
        F.sum(
            F.when(
                F.col("AQI").isNotNull(), 1
            ).otherwise(0)
        ).alias("available_aqi")
    ).show(truncate=False)
