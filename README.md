# Smart City – Air Quality and Pollution Intelligence

An end-to-end **Data Engineering and Business Intelligence project** designed to analyze historical air pollution across Indian cities.

The project integrates air quality measurements with historical weather data, processes large datasets using **Databricks, Apache PySpark, and Delta Lake**, and delivers actionable environmental insights through **four interactive Power BI dashboards** connected to Databricks Gold tables.

## Project Overview

Air pollution is a major environmental concern in rapidly developing urban areas. Understanding pollution patterns requires processing large volumes of environmental data collected across cities and monitoring stations.

This project develops a scalable data engineering pipeline using the **Bronze → Silver → Gold Medallion Architecture**.

The solution combines historical air quality observations from Kaggle with meteorological data from the Open-Meteo API. The processed data is stored in Delta Lake tables and made available to Power BI through the Databricks SQL Warehouse connector.

### Project at a Glance

| Metric | Result |
|---|---:|
| Historical air quality records | 3,434,754 |
| Source air quality CSV files | 5 |
| Cities covered by weather integration | 26 |
| Historical weather records | 29,531 |
| Gold analytical datasets | 6 |
| Monitoring stations represented | 110 |
| Power BI dashboard pages | 4 |
| Average AQI in overview dashboard | 166.5 |
| Automated Databricks workflow tasks | 4 |
| Successful workflow execution time | Approximately 4 min 33 sec |

> Dashboard metrics reflect the historical dataset and may change when interactive filters are applied.

## Problem Statement

Rapid urbanization, industrial growth, and increasing vehicle emissions have contributed to air pollution across Indian cities.

Although environmental monitoring stations generate substantial volumes of data, several challenges limit effective analysis:

- Large and complex environmental datasets
- Missing, inconsistent, and duplicate records
- Multiple data sources and observation frequencies
- Difficulty identifying pollution hotspots
- Limited integration between pollution and weather information
- Need for accessible and interactive analytical dashboards

## Project Objectives

1. **Data Ingestion:** Collect historical air quality and weather datasets from multiple sources.
2. **Data Processing:** Clean, validate, and transform large datasets using PySpark.
3. **Lakehouse Architecture:** Implement Bronze, Silver, and Gold data layers using Databricks and Delta Lake.
4. **Weather Integration:** Combine pollution measurements with meteorological information.
5. **Workflow Automation:** Automate ETL processing through Databricks Workflows.
6. **Business Intelligence:** Develop interactive Power BI dashboards for environmental analysis.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data extraction, API integration, and scripting |
| Apache PySpark | Distributed data cleaning and transformation |
| Databricks | Cloud-based data engineering and workflow orchestration |
| Delta Lake | Reliable storage for processed datasets |
| Unity Catalog | Data organization and governance |
| Databricks SQL Warehouse | SQL access to curated Gold tables |
| Open-Meteo API | Historical meteorological data |
| Power BI | Interactive dashboards and analytics |
| Git & GitHub | Version control and project management |

## Data Sources

### 1. Air Quality Dataset

**Source:** [Air Quality Data in India – Kaggle](https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india/data)

The dataset contains historical city-level and station-level air quality observations.

**Source Files:**

- `city_day.csv`
- `city_hour.csv`
- `station_day.csv`
- `station_hour.csv`
- `stations.csv`

**Key Attributes:**

- City and monitoring station
- Date and timestamp
- Air Quality Index (AQI)
- PM2.5 and PM10
- Nitrogen Dioxide (NO₂)
- Sulfur Dioxide (SO₂)
- Carbon Monoxide (CO)
- Ozone (O₃)

**Total raw records:** 3,434,754

### 2. Historical Weather Dataset

**Source:** [Open-Meteo Historical Weather API](https://open-meteo.com/)

Historical weather observations were collected for 26 Indian cities.

**Period:** January 2015 – July 2020

**Weather Attributes:**

- Temperature (°C)
- Relative humidity (%)
- Precipitation (mm)
- Maximum wind speed (km/h)
- Latitude and longitude

**Total weather records:** 29,531

## System Architecture

![Smart City Air Quality Architecture](docs/Smart_City_Air_Quality_Architecture.svg)

[View Architecture Diagram](docs/Smart_City_Air_Quality_Architecture.svg) | [Read Architecture Documentation](docs/03_Project_Architecture.md)

### End-to-End Data Flow

```text
          DATA SOURCES
                |
    +-----------+-----------+
    |                       |
Kaggle Air Quality     Open-Meteo API
    |                       |
    v                       v
Unity Catalog Volume   Weather Extraction
    |                       |
    v                       |
BRONZE LAYER                |
Raw Delta Tables            |
    |                       |
    v                       |
SILVER LAYER                |
Cleaned Delta Tables        |
    |                       |
    v                       |
GOLD LAYER <----------------+
Analytics & Weather Integration
    |
    v
Six Curated Gold Tables
    |
    v
Databricks SQL Warehouse
    |
    v
Power BI Connector
    |
    v
Four Interactive Dashboards
```

The architecture follows a Medallion Lakehouse design. The diagram illustrates the logical data flow; weather extraction and integration are implemented through the project's Databricks workflow.

## Data Engineering Pipeline

### Bronze Layer – Raw Data Ingestion

The Bronze layer ingests source air quality datasets into Databricks.

**Key Operations:**

- Load raw CSV datasets
- Preserve source information
- Store raw data in Delta tables
- Prepare records for downstream transformations

**Notebook:** `01_Bronze_Layer.py`

### Silver Layer – Data Cleaning and Validation

The Silver layer transforms raw records into standardized datasets.

**Key Operations:**

- Correct data types
- Parse dates and timestamps
- Handle missing and invalid values
- Remove duplicate records using appropriate keys
- Standardize column formats
- Validate pollutant measurements

**Notebook:** `02_Silver_Layer.py`

### Gold Layer – Analytics-Ready Data

The Gold layer generates curated datasets optimized for reporting and analysis.

**Key Operations:**

- Aggregate city-level AQI
- Generate monthly pollution summaries
- Analyze pollutant concentrations
- Calculate hourly pollution patterns
- Identify monitoring station hotspots
- Prepare datasets for weather integration

**Notebook:** `03_Gold_Layer.py`

### Weather Integration

Historical weather data is extracted using Python and the Open-Meteo API and integrated with air quality observations.

**Key Operations:**

- Retrieve historical weather observations
- Standardize city and date fields
- Integrate weather attributes with AQI measurements
- Generate the weather-enriched Gold dataset

**Notebook:** `04_Weather_Data_Extraction.py`

## Gold Analytical Tables

Six curated Gold datasets are available for analysis.

| Gold Table | Description |
|---|---|
| `gold_city_aqi_daily` | Daily city-level AQI trends and pollution categories |
| `gold_city_aqi_monthly` | Monthly AQI summaries and pollutant averages |
| `gold_pollutant_analysis` | Pollutant concentrations across cities and dates |
| `gold_hourly_patterns` | Hourly AQI patterns |
| `gold_station_hotspots` | Monitoring station AQI and hotspot analysis |
| `gold_weather_air_quality` | Integrated air quality and weather observations |

These datasets provide the analytical foundation for the Power BI dashboards.

## Databricks Workflow Automation

The ETL pipeline is orchestrated using **Databricks Workflows**.

### Workflow Execution

```text
Bronze_Layer
     |
     v
Silver_Layer
     |
     v
Gold_Layer
     |
     v
Weather_Integration
```

### Workflow Features

- Four dependent processing tasks
- Automated notebook execution
- Structured task dependencies
- Delta Lake data processing
- Execution monitoring and task status tracking
- Successful end-to-end pipeline execution

**Observed execution time:** Approximately 4 minutes 33 seconds.

## Databricks and Power BI Integration

The project initially used exported Gold CSV datasets for Power BI development.

The Power BI data source was subsequently updated to use the **Databricks connector**, allowing Power BI to retrieve curated Gold tables through Databricks SQL Warehouse.

### Connection Configuration

| Setting | Configuration |
|---|---|
| Data Source | Databricks |
| Authentication | OAuth |
| Catalog | `workspace` |
| Schema | `default` |
| Data Source Tables | Six Gold analytical tables |
| Power BI Connection | Import-based Databricks connector |

### Integration Benefits

- Reduces dependency on manual CSV exports
- Provides a centralized analytical data source
- Enables Power BI dataset refreshes from Databricks
- Maintains consistent Gold-layer business logic
- Simplifies the data engineering and analytics workflow

**Important:** The connection uses imported data refreshed from Databricks. It is not a real-time streaming pipeline or a live DirectQuery implementation.

## Power BI Dashboards

[Open Power BI Dashboard (.pbix)](dashboard/Smart_City_Air_Quality_Dashboard.pbix)

The project includes four interactive Power BI dashboard pages.

### 1. Air Quality Overview

![Air Quality Overview](dashboard/Air%20Quality%20Overview.png)

**Dashboard Features:**

- Average AQI: 166.5
- Total cities: 26
- Monitoring stations: 110
- Historical AQI trends
- Top 10 polluted cities
- AQI category distribution
- Interactive Year and City slicers

**Purpose:** Provide a high-level overview of historical air quality across Indian cities.

### 2. Pollutant Analysis

![Pollutant Analysis](dashboard/Pollutant%20Analysis.png)

**Dashboard Features:**

- Average pollutant concentration comparison
- Historical pollutant trends
- City-wise pollutant analysis
- Pollutant, Year, and City filters

**Pollutants:** PM2.5, PM10, NO₂, SO₂, CO, and O₃.

**Purpose:** Identify pollutant patterns and compare concentrations across locations and time.

### 3. Weather & Air Quality

![Weather and Air Quality](dashboard/Weather%20%26%20Air%20Quality.png)

**Dashboard Features:**

- Temperature versus AQI
- Humidity versus AQI
- Precipitation versus AQI
- Wind speed versus AQI
- Interactive Year and City filters

**Purpose:** Explore associations between meteorological conditions and historical pollution levels.

> Weather and AQI visualizations are exploratory; correlation does not establish causation.

### 4. Station Hotspot Analysis

![Station Hotspot Analysis](dashboard/Station%20Hotspot%20Analysis.png)

**Dashboard Features:**

- Total monitoring stations: 110
- Average station AQI: 163.6
- Top 10 polluted monitoring stations
- City-wise station AQI comparison
- Maximum station AQI
- Station operational status
- Interactive City filter

**Purpose:** Identify localized pollution hotspots and compare monitoring stations.

## Key Findings and Insights

### 1. Urban Pollution Hotspots

Ahmedabad and Delhi emerge among the most polluted cities in the historical analysis, highlighting variations in pollution levels across urban regions.

### 2. Historical AQI Trends

AQI levels vary across cities and time periods, demonstrating the importance of long-term environmental monitoring.

### 3. Pollutant Concentration Differences

Pollutant analysis reveals variations in concentrations across locations and observation periods.

### 4. Weather and Air Quality Relationships

Integrating historical weather observations allows exploration of potential relationships between environmental conditions and pollution levels.

### 5. Station-Level Pollution Hotspots

Station-level analysis helps identify localized pollution patterns that may not be visible in city-level averages.

### Selected Dashboard Results

| Metric | Result |
|---|---:|
| Average AQI | 166.5 |
| Cities analyzed | 26 |
| Monitoring stations | 110 |
| Average station AQI | 163.6 |
| Delhi monitoring stations (filtered) | 38 |
| Delhi average station AQI (filtered) | 235.0 |
| Duplicate City–Date keys in weather-enriched Gold table | 0 |

## Challenges and Solutions

| Challenge | Implemented Solution |
|---|---|
| Large environmental datasets | Distributed processing using PySpark |
| Missing and inconsistent values | Silver-layer cleaning and validation |
| Multiple data sources | Python extraction and structured integration |
| Complex analytical requirements | Six curated Gold datasets |
| Repetitive ETL processing | Databricks Workflow automation |
| Manual CSV-based reporting | Databricks SQL Warehouse integration with Power BI |

## Future Scope

The project provides a foundation for additional smart-city environmental monitoring capabilities.

**Potential Enhancements:**

1. **Real-Time Air Quality Monitoring:** Integrate live environmental data using streaming technologies.
2. **AQI Forecasting:** Develop machine learning models for future pollution prediction.
3. **Automated Alerts:** Notify users when pollution levels exceed defined thresholds.
4. **Geospatial Analysis:** Visualize pollution hotspots using interactive maps.
5. **Expanded Coverage:** Include additional cities, stations, and environmental data sources.

## Project Presentation

The final presentation summarizes the project architecture, implementation, datasets, Databricks workflow, Power BI dashboards, key findings, and future scope.

**[View Final Project Presentation (PDF)](presentation/Smart%20City%20-%20Air%20Quality%20%26%20Pollution%20Intelligence.pdf)**

The presentation includes:

- Problem statement and proposed solution
- Project objectives and technology stack
- Data sources and dataset overview
- Medallion Architecture
- Databricks ETL workflow
- Power BI dashboard analysis
- Key findings and future scope
- Project conclusion

## Repository Structure

```text
Smart-City-Air-Quality-Intelligence/
├── data/
│   ├── raw/
│   ├── weather/
│   └── gold/
│
├── notebooks/
│   ├── 01_Bronze_Layer.py
│   ├── 02_Silver_Layer.py
│   ├── 03_Gold_Layer.py
│   └── 04_Weather_Data_Extraction.py
│
├── dashboard/
│   ├── Smart_City_Air_Quality_Dashboard.pbix
│   ├── Air Quality Overview.png
│   ├── Pollutant Analysis.png
│   ├── Weather & Air Quality.png
│   └── Station Hotspot Analysis.png
│
├── docs/
│   ├── 03_Project_Architecture.md
│   └── Smart_City_Air_Quality_Architecture.svg
│
├── presentation/
│   └── Smart City - Air Quality & Pollution Intelligence.pdf
│
├── requirements.txt
├── .gitignore
└── README.md
```

> This structure highlights the main project assets. The presentation PDF is stored in the `presentation/` directory.

## Project Status

- [x] Project scope and problem statement
- [x] Historical air quality dataset collection
- [x] Weather API extraction and integration
- [x] Bronze Layer implementation
- [x] Silver Layer implementation
- [x] Gold Layer implementation
- [x] Six curated analytical datasets
- [x] Automated Databricks ETL workflow
- [x] Four interactive Power BI dashboards
- [x] Databricks SQL Warehouse connection configured
- [x] Power BI dashboard functionality verified
- [x] Architecture diagram and technical documentation
- [x] GitHub repository created and synchronized
- [x] Final project presentation prepared and uploaded

## Data Attribution

**Air Quality Dataset:** [Air Quality Data in India – Kaggle](https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india/data)

**Historical Weather Data:** [Open-Meteo](https://open-meteo.com/)

Dataset use and redistribution are subject to the respective providers' licensing, usage, and attribution requirements.

---

**Project:** Smart City – Air Quality and Pollution Intelligence

**Category:** Data Engineering | Databricks Lakehouse | Business Intelligence | Environmental Analytics

**Repository:** [Smart City Air Quality Intelligence](https://github.com/Rudra-AI-2127/Smart-City-Air-Quality-Intelligence)