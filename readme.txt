TEMA: ETL Sensores de Poços Petrolíferos (Airflow CSV Local)
Este projeto implementa um pipeline ETL utilizando Apache Airflow para processar dados de sensores IoT de poços petrolíferos, salvando os resultados finais em arquivos CSV e permitindo consultas por hora.


ARQUITECTURA DO PROJECTO: 
O pipeline realiza três etapas principais:
    1. EXTRACT
Lê os dados brutos dos sensores a partir de um arquivo CSV local (raw_sensor.csv).
    2. TRANSFORM & LOAD
        ◦ Converte timestamps e cria colunas de hora (hour).

        ◦ Agrega os dados de cada poço por hora, calculando médias de temperatura, pressão e vazão.

        ◦ Salva o arquivo final em dado_pocos_guardados.csv.
    3. QUERY
        ◦ Permite consultar os dados de múltiplos poços em uma hora específica.

        ◦ Salva a consulta em pesquisa_varios_pocos_hora.csv.


ESTRUTURA DO PROJECTO:

home/project/airflow/
│
├─ dags/
│   └─ etl_petroleum_sensor_csv_local.py  # DAG principal
│
├─ data/
│   ├─ raw_sensor.csv                     # Dados brutos dos sensores
│   ├─ dado_pocos_guardados.csv          # Dados processados (transformados e agregados)
│   └─ pesquisa_varios_pocos_hora.csv    # Resultado da consulta por hora
│
└─ README.docx

Como Executar
    1. Coloque os dados brutos no diretório data/raw_sensor.csv.
    2. Copie a DAG para o diretório dags/ do Airflow.
    3. Inicie o Airflow (webserver e scheduler):
airflow db init
airflow webserver --port 8080
airflow scheduler
    4. Execute a DAG via linha de comando:
airflow dags trigger etl_petroleum_sensor_csv_local
    5. Os arquivos finais serão gerados em data/dado_pocos_guardados.csv e data/pesquisa_varios_pocos_hora.csv.

Requisitos
    • Python 3.9+
    • Pandas
    • Apache Airflow 2.x
    • Arquivo CSV de entrada (raw_sensor.csv) com colunas: well_id, timestamp, temperature, pressure, flow_rate

Observações
    • O horário usado nas consultas pode ser alterado diretamente no código da função query_wells_by_hour().
    • Este projeto usa arquivos CSV locais, mas pode ser adaptado para integração com S3 e Redshift.

Tags
ETL | Airflow | IoT | Sensores | Poços Petrolíferos | CSV Local

Autor 
Cláudio Agostinho Rodrigues da Costa
