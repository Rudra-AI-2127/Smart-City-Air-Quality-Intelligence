# Data Sources and Extraction Methodology

## 1. Air Quality Dataset

**Source:** Air Quality Data in India – Kaggle  
**Dataset URL:** https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india/data  
**License:** CC0 1.0 Universal (as displayed on the dataset page)

The dataset contains historical air quality measurements across Indian cities and monitoring stations, including AQI and pollutant concentrations.

Five source files are used:

| Dataset | Description |
|---|---|
| city_day.csv | Daily city-level air quality measurements |
| city_hour.csv | Hourly city-level air quality measurements |
| station_day.csv | Daily station-level air quality measurements |
| station_hour.csv | Hourly station-level air quality measurements |
| stations.csv | Monitoring-station metadata |

The source files were obtained from Kaggle and uploaded into a Databricks Unity Catalog Volume for processing.

**Raw storage location:**

`/Volumes/workspace/default/air_quality_volume/raw/`

**Total air quality records ingested:** 3,434,754, including station metadata.

## 2. Historical Weather Dataset

**Source:** Open-Meteo Historical Weather API  
**Website:** https://open-meteo.com/

Historical weather data was retrieved for 26 cities to complement the air quality dataset.

The extracted weather variables include:

- Temperature (°C)
- Relative humidity (%)
- Precipitation (mm)
- Maximum wind speed (km/h)
- Latitude and longitude

**Extraction method:** Python API requests implemented in `04_Weather_Data_Extraction.py`.

**Weather records processed:** 29,531.

The weather data provides historical environmental context for AQI analysis. It should be interpreted as historical weather-model/reanalysis data rather than direct measurements from each air-quality station.

## 3. Data Ingestion and Processing

The project uses the following workflow:

1. Download air quality CSV datasets from Kaggle.
2. Upload raw datasets to a Databricks Volume.
3. Read CSV files using PySpark.
4. Store raw records in Bronze Delta tables.
5. Clean and standardize records in Silver Delta tables.
6. Aggregate and enrich data into Gold Delta tables.
7. Extract historical weather data using the Open-Meteo API and integrate it into the analytical pipeline.
8. Export six Gold datasets as CSV files for Power BI and GitHub.

## 4. Project Implementation Files

| File | Purpose |
|---|---|
| `01_Bronze_Layer.py` | Raw CSV ingestion and Bronze Delta tables |
| `02_Silver_Layer.py` | Data cleaning, type conversion and validation |
| `03_Gold_Layer.py` | Analytical aggregations and Gold tables |
| `04_Weather_Data_Extraction.py` | Historical weather API extraction and integration |

**GitHub repository:** https://github.com/Rudra-AI-2127/Smart-City-Air-Quality-Intelligence

## 5. Final Analytics Tables

The pipeline produces six Gold tables:

- `gold_city_aqi_daily`
- `gold_city_aqi_monthly`
- `gold_pollutant_analysis`
- `gold_hourly_patterns`
- `gold_station_hotspots`
- `gold_weather_air_quality`

These tables provide the foundation for the final Power BI dashboard.