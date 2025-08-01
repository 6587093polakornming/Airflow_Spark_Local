# TMDB Recommendation Flow

**Author**: Mr. Polakorn Anatapakorn  
**Role**: Data Engineer Intern, Bluebik Vulcan

---

## Description

TMDB Recommendation Flow is a modern data pipeline project designed to automate and streamline movie recommendation dataset generation using **Apache Airflow**, **Apache Spark**, and **Google Cloud Platform (BigQuery)**. The pipeline processes TMDB movie data through several ETL stages—cleansing, transformation, exporting, and modeling—to produce ready-to-use data for recommendation systems.

---

## Project Overview

The pipeline is orchestrated using **Airflow DAGs**, processes data using **Apache Spark**, and stores it in **BigQuery**. Output can be used for content-based or collaborative filtering movie recommendation systems.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Architecture Overview](#architecture-overview)
- [Tools & Versions](#tools--versions)
- [Installation](#installation)
- [Usage](#usage)
- [Airflow Connection](#airflow-connection)
- [Credits & Use Case](#credits--use-case)
- [Python Dependencies](#python-dependencies)
- [License](#license)

---

## Dataset

### Full TMDB Movies Dataset 2024 (1M Movies)

This project utilizes the comprehensive **Full TMDB Movies Dataset 2024** containing over 1 million movies from The Movie Database (TMDB). The dataset provides rich metadata essential for building robust recommendation systems and is updated daily.

**Dataset Source**: [Full TMDB Movies Dataset 2024 (1M Movies)](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies)

#### Dataset Features

The dataset includes the following key attributes:

- **Basic Movie Information**:
  - Movie ID, Title, Original Title
  - Release Date, Runtime
  - Status (Released, Rumored, etc.)
  - Homepage URL

- **Content Metadata**:
  - Overview/Plot Summary
  - Genres (Action, Comedy, Drama, etc.)
  - Keywords and Tags
  - Original Language
  - Adult Content Flag

- **Production Details**:
  - Production Companies
  - Production Countries
  - Budget and Revenue Information
  - Spoken Languages

- **Popularity & Ratings**:
  - TMDB Popularity Score
  - Vote Average (User Ratings)
  - Vote Count
  - Poster and Backdrop Image Paths

#### Dataset Statistics

- **Total Movies**: 1,000,000+
- **Time Range**: Movies from early cinema to 2024
- **Update Frequency**: Daily updates
- **Languages**: Multiple languages with English predominant
- **File Format**: CSV
- **File Size**: Approximately 600MB+ compressed

#### Data Quality & Preprocessing

The dataset requires preprocessing for recommendation systems:

- **Missing Values**: Some movies may have incomplete metadata
- **Data Types**: Mixed data types requiring transformation
- **Normalization**: Popularity scores and ratings need scaling
- **Feature Engineering**: Genre encoding, keyword extraction, and text processing

This comprehensive dataset enables both **content-based filtering** (using genres, keywords, overview) and **collaborative filtering** (using user ratings and popularity metrics) approaches for movie recommendations.

---

## Project Structure

```
TMDB_RecoFlow/
├── airflow/                 # Contains Airflow DAGs and utility scripts
├── spark/                   # Spark ETL scripts for preprocessing and transformation
├── eda/                     # Jupyter notebooks for data exploration and modeling
├── model/                   # Modeling scripts for recommendations
├── Dockerfile               # Dockerfile for Airflow and Spark services
├── Dockerfile.jupyter       # Dockerfile for JupyterLab with PySpark
├── docker-compose.yml       # Orchestration of all services
├── requirements.txt         # Python dependencies
```

---

## Architecture Overview
![Architecture Overview Figure](doc/diagram/TMDB_arch_ov.drawio.png)

---

## Tools & Versions

- ![Airflow](https://img.shields.io/badge/Airflow-2.10.5-blue?logo=apache-airflow&logoColor=white)  
  via provider: `apache-airflow-providers-apache-spark==4.3.0`

- ![Spark](https://img.shields.io/badge/Apache_Spark-3.5.2-FD7F20?logo=apache-spark&logoColor=white)

- ![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)

- ![Hadoop](https://img.shields.io/badge/Hadoop-3.0-yellow?logo=apache-hadoop&logoColor=black)

- ![Docker Compose](https://img.shields.io/badge/Docker--Compose-blue?logo=docker&logoColor=white)

- ![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue?logo=google-cloud&logoColor=white)  
  ![GCS](https://img.shields.io/badge/Cloud_Storage-GCP-blue?logo=google-cloud&logoColor=white)

---

## Installation

### Prerequisites
- Docker & Docker Compose
- GCP service account credentials (for BigQuery & GCS)

### Steps

```bash
# Clone the repository
git clone https://github.com/6587093polakornming/TMDB_RecoFlow.git
cd TMDB_RecoFlow

# Copy and configure your environment variables
cp example.env .env

# Build and start the services
docker-compose up --build
```

Ensure your GCP credentials and environment variables (e.g., project ID, bucket name) are correctly configured in `.env`.

---

## Usage

1. **Airflow UI**:
   - Access at `http://localhost:8080`
   - Trigger DAGs manually or set a schedule

2. **Workflow Path**:
   - Airflow DAGs: `airflow/dags/`
   - Spark Scripts: `spark/app/`
   - Notebooks: `eda/`

3. **ETL Process**:
   - Ingest and validate movie data
   - Clean and transform using Spark
   - Export to BigQuery
   - Run model to generate recommendations

---

## Airflow Connection
Airflow uses the following external service connections:

### 🧠 Spark Connection
- Airflow uses `SparkSubmitOperator` to submit Spark jobs.
- Connection string:
  ```bash
  spark://spark-master:7077
  ```
- This is defined as `spark_default` in `docker-compose.yml`.

### ☁️ Google Cloud Connection
- Airflow interacts with BigQuery and GCS using the connection ID:
  ```bash
  google_cloud_default
  ```
- This connection must be configured in the Airflow UI or via environment variables.
- The service account must have permission to:
  - Access BigQuery project: `YOUR-GCP-PROJECT-ID`
  - Read/write from GCS bucket: `YOUR-GCS-BUCKET-NAME`
- Ensure the service account key is available in your container and properly mounted..

---

## Credits & Use Case

### Dataset
- **Primary Dataset**: [Full TMDB Movies Dataset 2024 (1M Movies)](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies) by asaniczka
- **Alternative Reference**: [TMDB Movies Dataset - Daily Updates](https://www.kaggle.com/code/asaniczka/tmdb-movies-daily-updates)

### Implementation Reference
- **Recommendation System Approach**: [Recommendation System by moridata](https://www.kaggle.com/code/moridata/recommendation-system-movie-recommendation)

### Acknowledgments
Special thanks to the TMDB community and Kaggle contributors for maintaining and sharing this comprehensive movie dataset that enables advanced recommendation system development.

---

## Python Dependencies

Install locally (if needed outside Docker):

```bash
pip install --no-cache-dir \
  "apache-airflow-providers-apache-spark==4.3.0" \
  "apache-airflow-providers-google" \
  "google-cloud-bigquery" \
  "pyarrow" \
  "fastparquet"
```

---

## License

This project is licensed under the **Apache License**. See the `LICENSE` file for more details.