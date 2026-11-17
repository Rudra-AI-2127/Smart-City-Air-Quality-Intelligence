# Databricks notebook source

import requests
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"

API_URL = "https://archive-api.open-meteo.com/v1/archive"

print("Weather extraction initialized")


# COMMAND ----------


params = {
    "latitude": 28.6139,
    "longitude": 77.2090,
    "start_date": "2015-01-01",
    "end_date": "2020-07-01",
    "daily": ",".join([
        "temperature_2m_mean",
        "relative_humidity_2m_mean",
        "precipitation_sum",
        "wind_speed_10m_max"
    ]),
    "timezone": "Asia/Kolkata"
}

response = requests.get(
    API_URL,
    params=params,
    timeout=120
)

response.raise_for_status()

weather_data = response.json()

if "daily" not in weather_data:
    raise ValueError("Daily weather data missing from response")

weather_df = pd.DataFrame(weather_data["daily"])

weather_df["City"] = "Delhi"

print("Weather records extracted:", len(weather_df))
display(weather_df.head(10))


# COMMAND ----------


weather_spark = spark.createDataFrame(weather_df)

weather_spark = weather_spark.withColumn(
    "ingestion_timestamp",
    F.current_timestamp()
)

(
    weather_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.bronze_weather"
    )
)

print("Bronze weather table created")

display(
    spark.table("workspace.default.bronze_weather")
    .limit(10)
)


# COMMAND ----------


bronze_weather = spark.table(
    "workspace.default.bronze_weather"
)

silver_weather = (
    bronze_weather
    .withColumn(
        "Date",
        F.to_date(F.col("time"))
    )
    .withColumnRenamed(
        "temperature_2m_mean", "Temperature_C"
    )
    .withColumnRenamed(
        "relative_humidity_2m_mean", "Humidity_Percent"
    )
    .withColumnRenamed(
        "precipitation_sum", "Precipitation_mm"
    )
    .withColumnRenamed(
        "wind_speed_10m_max", "Max_Wind_Speed_kmh"
    )
    .drop("time")
    .dropDuplicates(["City", "Date"])
)

(
    silver_weather.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.silver_weather"
    )
)

print("Silver weather table created")


# COMMAND ----------


aqi = spark.table(
    "workspace.default.gold_city_aqi_daily"
)

weather = spark.table(
    "workspace.default.silver_weather"
)

delhi_aqi = aqi.filter(
    F.col("City") == "Delhi"
)

gold_weather_aqi = delhi_aqi.join(
    weather,
    on=["City", "Date"],
    how="left"
)

print("AQI records:", delhi_aqi.count())
print("Joined records:", gold_weather_aqi.count())

display(gold_weather_aqi.limit(10))


# COMMAND ----------


from pyspark.sql import functions as F

validation = gold_weather_aqi.agg(
    F.count("*").alias("Total_Records"),
    F.count("Temperature_C").alias("Weather_Matched"),
    F.sum(
        F.when(
            F.col("Temperature_C").isNull(), 1
        ).otherwise(0)
    ).alias("Weather_Missing")
)

display(validation)

# Calculate weather match percentage
result = validation.first()

match_percentage = (
    result["Weather_Matched"]
    / result["Total_Records"]
) * 100

print(f"Weather Match Rate: {match_percentage:.2f}%")


# COMMAND ----------


from pyspark.sql import functions as F

aqi = spark.table(
    "workspace.default.gold_city_aqi_daily"
)

cities = [
    row["City"]
    for row in (
        aqi.select("City")
        .where(F.col("City").isNotNull())
        .distinct()
        .orderBy("City")
        .collect()
    )
]

print("Total cities:", len(cities))
print(cities)


# COMMAND ----------


import requests
import time

GEOCODING_URL = (
    "https://geocoding-api.open-meteo.com/v1/search"
)

city_locations = {}
unresolved_cities = []

for city in cities:
    response = requests.get(
        GEOCODING_URL,
        params={
            "name": city,
            "count": 20,
            "language": "en"
        },
        timeout=30
    )

    response.raise_for_status()
    results = response.json().get("results", [])

    # Restrict matches to India
    indian_results = [
        r for r in results
        if r.get("country_code") == "IN"
    ]

    if indian_results:
        location = indian_results[0]

        city_locations[city] = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "matched_name": location["name"]
        }
    else:
        unresolved_cities.append(city)

    time.sleep(0.3)

print("Resolved cities:", len(city_locations))
print("Unresolved cities:", unresolved_cities)

for city, location in city_locations.items():
    print(city, "->", location)


# COMMAND ----------


import pandas as pd
import time

API_URL = "https://archive-api.open-meteo.com/v1/archive"

city_dates = {
    row["City"]: {
        "start": str(row["start_date"]),
        "end": str(row["end_date"])
    }
    for row in (
        aqi.groupBy("City")
        .agg(
            F.min("Date").alias("start_date"),
            F.max("Date").alias("end_date")
        )
        .collect()
    )
}

all_weather = []
failed_cities = []

for city, location in city_locations.items():
    try:
        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "start_date": city_dates[city]["start"],
            "end_date": city_dates[city]["end"],
            "daily": ",".join([
                "temperature_2m_mean",
                "relative_humidity_2m_mean",
                "precipitation_sum",
                "wind_speed_10m_max"
            ]),
            "timezone": "Asia/Kolkata"
        }

        response = requests.get(
            API_URL,
            params=params,
            timeout=120
        )
        response.raise_for_status()

        data = response.json()

        if "daily" not in data:
            raise ValueError("Daily weather data missing")

        df = pd.DataFrame(data["daily"])
        df["City"] = city
        df["Latitude"] = location["latitude"]
        df["Longitude"] = location["longitude"]

        all_weather.append(df)

        print(city, ":", len(df), "records")

    except Exception as e:
        failed_cities.append((city, str(e)))
        print("FAILED:", city, str(e))

    time.sleep(1)

print("Successful cities:", len(all_weather))
print("Failed cities:", failed_cities)

if not all_weather:
    raise RuntimeError("No weather data extracted")

weather_all_df = pd.concat(
    all_weather,
    ignore_index=True
)

print("Total weather records:", len(weather_all_df))


# COMMAND ----------


print("Cities extracted:",
      weather_all_df["City"].nunique())

print("Total records:", len(weather_all_df))

print("Missing values:")
print(weather_all_df.isnull().sum())

display(weather_all_df.head(10))


# COMMAND ----------


from pyspark.sql import functions as F

weather_spark = spark.createDataFrame(weather_all_df)

weather_spark = weather_spark.withColumn(
    "ingestion_timestamp",
    F.current_timestamp()
)

(
    weather_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.bronze_weather")
)

print("Bronze weather table created successfully!")
print("Total records:", spark.table(
    "workspace.default.bronze_weather"
).count())


# COMMAND ----------


bronze_weather = spark.table(
    "workspace.default.bronze_weather"
)

silver_weather = (
    bronze_weather
    .withColumn("Date", F.to_date("time"))
    .withColumnRenamed(
        "temperature_2m_mean", "Temperature_C"
    )
    .withColumnRenamed(
        "relative_humidity_2m_mean", "Humidity_Percent"
    )
    .withColumnRenamed(
        "precipitation_sum", "Precipitation_mm"
    )
    .withColumnRenamed(
        "wind_speed_10m_max", "Max_Wind_Speed_kmh"
    )
    .drop("time")
    .dropDuplicates(["City", "Date"])
)

(
    silver_weather.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.silver_weather")
)

print("Silver weather table created successfully!")
print("Total records:", silver_weather.count())


# COMMAND ----------


aqi = spark.table(
    "workspace.default.gold_city_aqi_daily"
)

weather = spark.table(
    "workspace.default.silver_weather"
)

gold_weather_aqi = aqi.join(
    weather.select(
        "City",
        "Date",
        "Temperature_C",
        "Humidity_Percent",
        "Precipitation_mm",
        "Max_Wind_Speed_kmh",
        "Latitude",
        "Longitude"
    ),
    on=["City", "Date"],
    how="left"
)

# Validate before saving
validation = gold_weather_aqi.agg(
    F.count("*").alias("Total_Records"),
    F.count("Temperature_C").alias("Weather_Matched"),
    F.sum(
        F.when(
            F.col("Temperature_C").isNull(), 1
        ).otherwise(0)
    ).alias("Weather_Missing")
)

display(validation)

print("AQI records:", aqi.count())
print("Joined records:", gold_weather_aqi.count())


# COMMAND ----------


(
    gold_weather_aqi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.gold_weather_air_quality"
    )
)

print("Final Gold weather + AQI table created!")

display(
    spark.table(
        "workspace.default.gold_weather_air_quality"
    ).limit(10)
)
