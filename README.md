# Smart City – Air Quality and Pollution Intelligence

## Project Overview

An end-to-end Data Engineering project that processes historical air quality and weather data across Indian cities to analyze pollution trends, pollutant concentrations, station hotspots, and relationships between weather conditions and air quality.

## Tech Stack

- **Python** – Data extraction and API integration
- **Apache Spark (PySpark)** – Distributed data processing
- **Databricks** – Data engineering platform
- **Delta Lake** – Medallion Architecture storage
- **Open-Meteo API** – Historical weather data
- **Power BI** – Data visualization (planned)
- **GitHub** – Version control and project documentation

## Data Sources

1. **Air Quality Data in India** – Kaggle dataset containing city-level and station-level daily/hourly pollution measurements.
2. **Historical Weather Data** – Open-Meteo historical weather API, providing temperature, humidity, precipitation, and wind speed.

## Data Pipeline Architecture

Raw CSV Files + Weather API → Bronze Layer → Silver Layer → Gold Layer → Power BI Dashboard

### Bronze Layer

Ingests raw air quality datasets into Delta tables and extracts historical weather data.

### Silver Layer

Cleans and standardizes records, handles invalid pollutant values, parses dates, and prepares data for analysis.

### Gold Layer

Produces analytics-ready Delta tables:

- `gold_city_aqi_daily`
- `gold_city_aqi_monthly`
- `gold_pollutant_analysis`
- `gold_hourly_patterns`
- `gold_station_hotspots`
- `gold_weather_air_quality`

## Processing Results

- **3,434,754** air quality records ingested
- **26** cities covered by the weather integration
- **29,531** historical weather records
- **6** Gold analytics tables created
- **0** duplicate City–Date combinations in the final weather-enriched Gold table

## Project Structure

```text
Smart-City-Air-Quality-Intelligence/
├── data/
│   ├── raw/
│   └── weather/
├── notebooks/
│   ├── 01_Bronze_Layer.py
│   ├── 02_Silver_Layer.py
│   ├── 03_Gold_Layer.py
│   └── 04_Weather_Data_Extraction.py
├── dashboard/
├── docs/
├── requirements.txt
├── .gitignore
└── README.md
```

## Project Status

- [x] Air quality data ingestion
- [x] Bronze Layer
- [x] Silver Layer
- [x] Gold Layer
- [x] Historical weather integration
- [ ] Power BI dashboard
- [ ] Final project presentation

## Data Attribution

Air quality dataset: [Air Quality Data in India – Kaggle](https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india/data)

Historical weather: [Open-Meteo](https://open-meteo.com/)

Dataset redistribution is subject to the respective providers' licensing and attribution requirements.