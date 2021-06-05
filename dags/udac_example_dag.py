from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SQLQueries

"""
CONSTANTS
"""
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

DEFAULT_ARGS = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(
        seconds=1
    ),
    'catchup': False
}

AWS_CONN_ID: str = "aws"
REDSHIFT_CONN_ID: str = "redshift"
S3_BUCKET_NAME: str = "udacity-dend"

STAGING_EVENTS_TABLE: str = "staging_events"
STAGING_SONGS_TABLES: str = "staging_songs"
FACT_TABLE: str = "songplays"
USERS_TABLE: str = "users"
SONGS_TABLE: str = "songs"
TIME_TABLE: str = "time"
ARTIST_TABLE: str = "artists"


"""
AIRFLOW API
"""

dag = DAG(
    'udac_example_dag',
    default_args=DEFAULT_ARGS,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    bucket=f"{S3_BUCKET_NAME}/log-data",
    s3_conn_id=AWS_CONN_ID,
    redshift_table=STAGING_EVENTS_TABLE,
    redshift_conn_id=REDSHIFT_CONN_ID,
    format='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    bucket=f"{S3_BUCKET_NAME}/song-data",
    s3_conn_id=AWS_CONN_ID,
    redshift_table=STAGING_SONGS_TABLES,
    redshift_conn_id=REDSHIFT_CONN_ID,
    format='auto',
    region='us-west-2',
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    redshift_table=FACT_TABLE,
    query=SQLQueries.songplay_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    redshift_table=USERS_TABLE,
    query=SQLQueries.user_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    redshift_table=SONGS_TABLE,
    query=SQLQueries.song_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    redshift_table=ARTIST_TABLE,
    query=SQLQueries.artist_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    redshift_table=TIME_TABLE,
    query=SQLQueries.time_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    tables=[
        FACT_TABLE,
        SONGS_TABLE,
        ARTIST_TABLE,
        TIME_TABLE,
        USERS_TABLE
    ],
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
