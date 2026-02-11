import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag
# Importamos las funciones que ya tienen el @task puesto
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

local_tz = pendulum.timezone('America/Mexico_City')

default_args = {
    'owner': 'rbnalexs',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#This is the best code that I've ever written
@dag(
    dag_id="produce_json_mx_v2",
    default_args=default_args,
    description="DAG to produce JSON file with raw data - CDMX Time",
    schedule="33 22 * * *", 
    start_date=datetime(2026, 2, 9, tzinfo=local_tz),
    catchup=False,
)
def video_data_pipeline():

    # NO definas @task aquí adentro. 
    # Simplemente ejecuta las que ya importaste.
    
    p_id = get_playlist_id() # Estas ya son tareas de Airflow
    v_ids = get_video_ids(p_id)
    data = extract_video_data(v_ids)
    save_to_json(data)

video_data_pipeline()