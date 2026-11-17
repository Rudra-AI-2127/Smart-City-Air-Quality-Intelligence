# Databricks notebook source

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"

city_day = spark.table(
    f"{CATALOG}.{SCHEMA}.silver_city_day"
)

city_hour = spark.table(
    f"{CATALOG}.{SCHEMA}.silver_city_hour"
)

station_day = spark.table(
    f"{CATALOG}.{SCHEMA}.silver_station_day"
)

stations = spark.table(
    f"{CATALOG}.{SCHEMA}.silver_stations"
)

print("Silver tables loaded successfully")

city_day.printSchema()


# COMMAND ----------


gold_city_daily = city_day.select(
    F.col("City"),
    F.col("Date"),
    F.col("AQI"),
    F.col("AQI_Bucket"),
    F.col("`PM2.5`").alias("PM25"),
    F.col("PM10"),
    F.col("NO2"),
    F.col("SO2"),
    F.col("CO"),
    F.col("O3")
)

(
    gold_city_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.gold_city_aqi_daily"
    )
)

print("Created gold_city_aqi_daily")


# COMMAND ----------


gold_city_monthly = (
    gold_city_daily
    .withColumn("Year", F.year("Date"))
    .withColumn("Month", F.month("Date"))
    .groupBy("City", "Year", "Month")
    .agg(
        F.round(F.avg("AQI"), 2).alias("Avg_AQI"),
        F.round(F.max("AQI"), 2).alias("Max_AQI"),
        F.round(F.avg("PM25"), 2).alias("Avg_PM25"),
        F.round(F.avg("PM10"), 2).alias("Avg_PM10"),
        F.count("AQI").alias("AQI_Observations"),
        F.count("*").alias("Total_Days")
    )
)

(
    gold_city_monthly.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.gold_city_aqi_monthly"
    )
)

print("Created gold_city_aqi_monthly")


# COMMAND ----------


pollutants = [
    "PM25", "PM10", "NO2", "SO2", "CO", "O3"
]

gold_pollutants = None

for pollutant in pollutants:

    df = gold_city_daily.select(
        "City",
        "Date",
        F.lit(pollutant).alias("Pollutant"),
        F.col(pollutant).alias("Concentration")
    ).filter(
        F.col("Concentration").isNotNull()
    )

    if gold_pollutants is None:
        gold_pollutants = df
    else:
        gold_pollutants = gold_pollutants.unionByName(df)

(
    gold_pollutants.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.gold_pollutant_analysis"
    )
)

print("Created gold_pollutant_analysis")


# COMMAND ----------


gold_hourly = (
    city_hour
    .withColumn(
        "Hour",
        F.hour(F.col("Datetime"))
    )
    .groupBy("City", "Hour")
    .agg(
        F.round(F.avg("AQI"), 2).alias("Avg_AQI"),
        F.count("AQI").alias("AQI_Observations")
    )
)

(
    gold_hourly.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.gold_hourly_patterns"
    )
)

print("Created gold_hourly_patterns")


# COMMAND ----------


print("Station metadata columns:")
print(stations.columns)

print("Station daily columns:")
print(station_day.columns)


# COMMAND ----------


gold_hotspots = (
    station_day
    .groupBy("StationId")
    .agg(
        F.round(F.avg("AQI"), 2).alias("Avg_AQI"),
        F.max("AQI").alias("Max_AQI"),
        F.count("AQI").alias("AQI_Observations")
    )
)

(
    gold_hotspots.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.gold_station_hotspots"
    )
)

print("Created gold_station_hotspots")


# COMMAND ----------


gold_tables = [
    "gold_city_aqi_daily",
    "gold_city_aqi_monthly",
    "gold_pollutant_analysis",
    "gold_hourly_patterns",
    "gold_station_hotspots"
]

for table in gold_tables:

    df = spark.table(
        f"workspace.default.{table}"
    )

    print(f"{table}: {df.count()} rows")


# COMMAND ----------


stations = spark.table(
    "workspace.default.silver_stations"
)

print("Station metadata columns:")
print(stations.columns)

display(stations.limit(5))


# COMMAND ----------


from pyspark.sql import functions as F

hotspots = spark.table(
    "workspace.default.gold_station_hotspots"
)

matched = hotspots.join(
    stations,
    on="StationId",
    how="left"
)

print("Total hotspot stations:", hotspots.count())

print(
    "Matched station IDs:",
    matched.filter(
        F.col("StationId").isNotNull()
    ).select("StationId").distinct().count()
)

display(matched.limit(10))


# COMMAND ----------


from pyspark.sql import functions as F

hotspots = spark.table(
    "workspace.default.gold_station_hotspots"
)

stations = spark.table(
    "workspace.default.silver_stations"
)

# Join station AQI summary with station metadata
station_details = stations.withColumn(
    "_metadata_matched",
    F.lit(True)
)

enriched_hotspots = hotspots.join(
    station_details,
    on="StationId",
    how="left"
)

# Check actual metadata coverage
enriched_hotspots.select(
    F.count("*").alias("total_rows"),
    F.count("StationId").alias("station_ids"),
    F.count("_metadata_matched").alias("matched_rows")
).show()

display(enriched_hotspots.limit(10))


# COMMAND ----------


# Remove temporary validation column
final_hotspots = enriched_hotspots.drop(
    "_metadata_matched"
)

# Save enriched table
(
    final_hotspots.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.gold_station_hotspots"
    )
)

print("Enriched Gold station table saved!")

print("Total stations:", final_hotspots.count())

display(final_hotspots.limit(10))


# COMMAND ----------


df = spark.table(
    "workspace.default.gold_station_hotspots"
)

print("Columns:", df.columns)
print("Total rows:", df.count())

display(df.limit(5))
