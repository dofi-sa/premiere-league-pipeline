#  ETL Pipeline: Premier League Football Data with Apache Airflow

This project is an end-to-end data pipeline (ETL) designed to extract match data from the English Premier League using the [Football-Data.org](https://www.football-data.org/) API, transform it, and load it into a PostgreSQL database. The data is then ready to be visualized and analyzed in **Power BI**.

## 🏗️ Project Architecture

The architecture is fully containerized using **Docker** and orchestrated by **Apache Airflow**.

1. **Extract**: Fetch raw JSON data from the Football-Data API.
2. **Transform**: Clean and structure the data using the `pandas` library.
3. **Load**: Insert the transformed data into a PostgreSQL table (`stg_matches`).
4. **Visualize**: Connect the PostgreSQL database to Power BI to build dashboards.

## 🛠️ Tech Stack

*   **Orchestration**: Apache Airflow (2.7.1)
*   **Containerization**: Docker & Docker Compose
*   **Language**: Python 3.10
*   **Database**: PostgreSQL 13
*   **Data Manipulation**: Pandas, SQLAlchemy
*   **Business Intelligence**: Power BI

## 📂 Repository Structure

```text
football_data_project/
│
├── dags/
│   └── football_pipeline_dag.py   # Main script containing the Airflow DAG
├── docker-compose.yaml            # Docker services configuration
├── .env.example                   # Example environment variables (copy to .env)
├── .gitignore                     # Files to be ignored by Git
└── README.md                      # Project documentation
## 🚀 How to Run the Project
```
### Prerequisites

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* A free API key from [Football-Data.org](https://www.football-data.org/client/register).
### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/football-data-pipeline.git
   cd football-data-pipeline
   ```
2. **Set up environment variables:**
   ```bash
     FOOTBALL_API_TOKEN=your_api_key_here
   ````
3. **Start the Docker containers:**
 ```bash
  docker compose up -d
```
4. **Access the Airflow UI:**

Open your browser and go to: http://localhost:8081

Username: admin

Password: admin   
5. ** run the pipeline :**
Unpause the DAG named football_data_pipeline
### 📊 Connecting to Power BI
Server: 127.0.0.1:5433 (Note the port 5433, as port 5432 is often reserved by local PostgreSQL instances).

Database: airflow

Username: airflow

Password: airflow

Table to import: public.stg_matches
