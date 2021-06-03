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
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(
        minutes=1
    ),
    'catchup': False
}

AWS_CONN_ID: str = "aws"
REDSHIFT_CONN_ID: str = "redshift"
S3_BUCKET_NAME: str = "udacity-dend"


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
    bucket=f"{S3_BUCKET_NAME}/log_data",
    s3_conn_id=AWS_CONN_ID,
    redshift_table='staging_events',
    redshift_conn_id=REDSHIFT_CONN_ID,
    region='us-west-2',
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    bucket=f"{S3_BUCKET_NAME}/song_data",
    s3_conn_id=AWS_CONN_ID,
    redshift_table='staging_songs',
    redshift_conn_id=REDSHIFT_CONN_ID,
    region='us-west-2',
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
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
