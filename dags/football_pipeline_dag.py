
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import io
import os

# ------------------------------------------------------------------
# CONFIGURATION ET ARGUMENTS PAR DÉFAUT
# ------------------------------------------------------------------
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ------------------------------------------------------------------
# 1. ÉTAPE EXTRACT (Extraction depuis l'API Football-Data.org)
# ------------------------------------------------------------------
def extract_data(**kwargs):
    """Récupère les données brutes des matchs via l'API HTTP."""
    url = "https://api.football-data.org/v4/competitions/PL/matches"
    
    # Remplacez par votre clé API Football-Data si nécessaire
    headers = {"X-Auth-Token": os.getenv("FOOTBALL_API_TOKEN")}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # Extraire la liste des matchs au format JSON
    matches = response.json().get('matches', [])
    
    # Transmission de la donnée brute à la tâche suivante via Airflow XCom
    kwargs['ti'].xcom_push(key='raw_matches', value=matches)

# ------------------------------------------------------------------
# 2. ÉTAPE TRANSFORM (Nettoyage et structuration avec Pandas)
# ------------------------------------------------------------------
def transform_data(**kwargs):
    """Nettoie le JSON brut et le transforme en structure tabulaire."""
    ti = kwargs['ti']
    matches = ti.xcom_pull(key='raw_matches', task_ids='extract_data')
    
    transformed_list = []
    for match in matches:
        transformed_list.append({
            'match_id': match.get('id'),
            'utc_date': match.get('utcDate'),
            'status': match.get('status'),
            'home_team_name': match.get('homeTeam', {}).get('name'),
            'away_team_name': match.get('awayTeam', {}).get('name'),
            'home_score': match.get('score', {}).get('fullTime', {}).get('home'),
            'away_score': match.get('score', {}).get('fullTime', {}).get('away'),
        })
    
    # Création du DataFrame Pandas
    df = pd.DataFrame(transformed_list)
    
    # Transmission des données transformées au format JSON via XCom
    ti.xcom_push(key='transformed_matches', value=df.to_json(orient='records'))

# ------------------------------------------------------------------
# 3. ÉTAPE LOAD (Chargement dans la base PostgreSQL Docker)
# ------------------------------------------------------------------
def load_to_postgres(**kwargs):
    """Persiste le DataFrame dans la table PostgreSQL stg_matches."""
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(key='transformed_matches', task_ids='transform_data')
    
    # Reconstitution du DataFrame à partir du JSON
    df = pd.read_json(io.StringIO(transformed_json), orient='records')
    
    # Connexion au conteneur PostgreSQL via SQLAlchemy
    # Identifiants: user=airflow, password=airflow, host=postgres (nom du service Docker), port=5432, db=airflow
    db_url = "postgresql://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(db_url)
    
    # Insertion dans la table stg_matches
    df.to_sql(
        name='stg_matches',
        con=engine,
        if_exists='replace',  # Remplace la table si elle existe déjà
        index=False
    )

# ------------------------------------------------------------------
# DÉFINITION ET ENCHAÎNEMENT DU DAG AIRFLOW
# ------------------------------------------------------------------
with DAG(
    'football_data_pipeline',
    default_args=DEFAULT_ARGS,
    description='Pipeline ETL quotidien des résultats de football',
    schedule_interval='0 6 * * *',  # Exécution quotidienne à 06h00 UTC
    catchup=False,
) as dag:

    # Déclaration des opérateurs Airflow
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_to_postgres
    )

    # Ordre d'exécution du pipeline (Extract -> Transform -> Load)
    task_extract >> task_transform >> task_load