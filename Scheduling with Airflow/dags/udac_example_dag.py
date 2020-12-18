from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from subdag import check_qualities
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = 's3://udacity-dend/'
s3_key_song = 'song_data'
s3_key_log = 'log_data'

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
}

dag = DAG(
    'udac_example_dag',
    description='Load and transform data in Redshift with Airflow',
    default_args=default_args,
    # schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credential_id='aws_credentials',
    redshift_conn_id='redshift',
    table='stage_events',
    s3_bucket=s3_bucket,
    s3_key=s3_key_log,
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credential_id='aws_credentials',
    redshift_conn_id='redshift',
    table='stage_songs',
    s3_bucket=s3_bucket,
    s3_key=s3_key_song,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     table_sql=SqlQueries.user_table_insert,
#     redshift_conn_id='redshift',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     table_sql=SqlQueries.song_table_insert,
#     redshift_conn_id='redshift',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     table_sql=SqlQueries.artist_table_insert,
#     redshift_conn_id='redshift',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     table_sql=SqlQueries.time_table_insert,
#     redshift_conn_id='redshift',
#     dag=dag
# )

# quality_check_id = 'quality_check_subdags'
# run_quality_checks = SubDagOperator(
#     subdag=check_qualities(
#         'udac_example_dag',
#         quality_check_id,
#         'redshift',
#         start_date=default_args['start_date']
#     ),
#     task_id=quality_check_id,
#     dag=dag
# )
    
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> end_operator
