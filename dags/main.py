import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
# Ensure these imports are correct based on your file structure
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

local_tz = pendulum.timezone('Europe/Malta')

default_args = {
    'owner': 'rbnalexs',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *", 
    start_date=datetime(2026, 2, 14, tzinfo=local_tz),
    catchup=False,
)
def video_data_pipeline():

    # Using the @task decorator turns these into Airflow tasks
    @task
    def get_playlist_task():
        return get_playlist_id()

    @task
    def get_video_ids_task(playlist_id):
        return get_video_ids(playlist_id)

    @task
    def extract_data_task(video_ids):
        return extract_video_data(video_ids)

    @task
    def save_json_task(data):
        return save_to_json(data)

    # Define the dependency flow
    p_id = get_playlist_task()
    v_ids = get_video_ids_task(p_id)
    data = extract_data_task(v_ids)
    save_json_task(data)

# Instantiate the DAG
video_data_pipeline()