from airflow import DAG
from datetime import datetime
from operators.data_quality import DataQualityOperator


def check_qualities(parent_dag_name,
                    task_id,
                    redshift_conn_id,
                    *args, **kwargs):
    dag = DAG(
        f'{parent_dag_name}.{task_id}',
        **kwargs
    )
    
    check_users = DataQualityOperator(
        task_id='check_users',
        redshift_conn_id=redshift_conn_id,
        table='users',
        dag=dag
    )
    
    check_songs = DataQualityOperator(
        task_id='check_songs',
        redshift_conn_id=redshift_conn_id,
        table='songs',
        dag=dag
    )
    
    check_artists = DataQualityOperator(
        task_id='check_artists',
        redshift_conn_id=redshift_conn_id,
        table='artists',
        dag=dag
    )
    
    check_time = DataQualityOperator(
        task_id='check_time',
        redshift_conn_id=redshift_conn_id,
        table='time',
        dag=dag
    )
    
    return dag
