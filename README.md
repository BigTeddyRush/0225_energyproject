# ⚡ Energy Timeseries Dashboard  

This project provides an **ETL pipeline** and a **Streamlit dashboard** for working with German energy data from the **SMARD API**.  
It allows you to:  

- Extract and load timeseries energy data into a PostgreSQL database.  
- Explore and visualize the data interactively in a web dashboard.  
- Upload PDFs and summarize them with **Ollama AI models**.  
- Get AI-generated descriptions of your filtered data.  

---

## 🚀 Features  

- **ETL Service** (`data_ingestion.py`)  
  - Fetches timeseries data from the SMARD API.  
  - Cleans and inserts it into PostgreSQL with UPSERT logic.  
  - Configurable filters, regions, and resolutions via `config.yaml`.  

- **Streamlit Dashboard** (`app.py`)  
  - Interactive filtering by `filter_label`, `region`, `resolution`, and date range.  
  - Data preview and downloadable dataframe.  
  - Altair bar chart visualization.  
  - PDF upload with **AI summarization** (via Ollama).  
  - AI-powered data descriptions of the selected dataset.  

- **Dockerized**  
  - Uses `docker-compose` for easy deployment.  
  - Includes `etl`, `db`, and `streamlit` services.  

---

## 🏗 Project Structure  

├── app.py # Streamlit dashboard  
├── data_ingestion.py # ETL script for fetching + loading SMARD data  
├── config.yaml # Config for filters, regions, and resolutions  
├── docker_compose.yml # Multi-service setup  
├── Dockerfile # Shared base image for etl and streamlit  
├── requirements.txt # Python dependencies  
└── pgdata/ # Postgres volume (created after first run)

## ⚙️ Setup & Installation  

### 1. Prerequisites  
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/)  
- [Ollama](https://ollama.com/) installed **on your host machine**, since AI calls are made via `http://host.docker.internal:11434`.  

### 2. Configute yaml
FILTER_IDS:
  - ["Total Load", 1001226]
  - ["Renewable Generation", 1004066]

REGIONS:
  - DE
  - DE_LU

RESOLUTIONS:
  - quarterhour
  - day

TIMESTAMP_MODE: newest   # Options: newest | all | specific
SPECIFIC_TIMESTAMP: null

### 3. Start
docker compose up --build
