from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# ================================
# Caminhos dos arquivos
# ================================
DATA_DIR = "/home/project/airflow/data"
RAW_FILE = f"{DATA_DIR}/raw_sensor.csv"
CLEAN_FILE = f"{DATA_DIR}/dado_pocos_guardados.csv"
QUERY_FILE = f"{DATA_DIR}/pesquisa_varios_pocos_hora.csv"

# ================================
# EXTRACT
# ================================
def extract_sensor_data():
    """Lê dados de sensores do CSV local"""
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(f"❌ Arquivo não encontrado: {RAW_FILE}")
    
    df = pd.read_csv(RAW_FILE)
    print("✔ Dados brutos carregados:", len(df))
    print(df.head())

# ================================
# TRANSFORM & LOAD
# ================================
def transform_and_load():
    """Limpa, agrega por hora e salva o arquivo final"""
    df = pd.read_csv(RAW_FILE)
    
    # Converter timestamp e criar coluna hora
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.floor('H')
    
    # Agregar média por poço e hora
    df_clean = df.groupby(['well_id', 'hour']).agg({
        'temperature': 'mean',
        'pressure': 'mean',
        'flow_rate': 'mean'
    }).reset_index()
    
    # Salvar CSV final
    df_clean.to_csv(CLEAN_FILE, index=False)
    print("✔ Transformação e carregamento concluídos. Arquivo salvo:", CLEAN_FILE)
    print(df_clean.head())

# ================================
# QUERY
# ================================
def query_wells_by_hour():
    """Seleciona dados de múltiplos poços em determinada hora"""
    df = pd.read_csv(CLEAN_FILE)
    
    # Exemplo: selecionar dados de vários poços numa hora específica
    # Aqui você pode mudar a hora para a desejada
    hora_especifica = pd.to_datetime("2025-10-23 14:00:00")
    
    # Selecionar dados da hora específica
    df_query = df[df['hour'] == hora_especifica]
    
    # Salvar CSV da consulta
    df_query.to_csv(QUERY_FILE, index=False)
    print("✔ Consulta realizada e salva em:", QUERY_FILE)
    print(df_query)

# ================================
# DAG
# ================================
default_args = {
    'owner': 'PetroETL',
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

with DAG('etl_petroleum_sensor_csv_local',
         default_args=default_args,
         description='ETL sensores de poços petrolíferos salvando em CSV e consultas por hora',
         schedule_interval='@hourly',
         start_date=datetime(2025,10,23),
         catchup=False,
         tags=['petroleum', 'IoT', 'ETL', 'CSV']) as dag:

    task_extract = PythonOperator(
        task_id='extract_sensor_data', 
        python_callable=extract_sensor_data
    )
    
    task_transform_load = PythonOperator(
        task_id='transform_and_load', 
        python_callable=transform_and_load
    )
    
    task_query = PythonOperator(
        task_id='query_wells_by_hour', 
        python_callable=query_wells_by_hour
    )

    # Ordem de execução
    task_extract >> task_transform_load >> task_query
