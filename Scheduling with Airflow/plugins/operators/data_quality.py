from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None,
                 table=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        
    def execute(self, context):
        self.log.info('checking data quality')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        sql = f"SELECT COUNT(*) FROM {table} LIMIT 10"
        records = redshift_hook.get_records(sql)
                
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'FAILED: no records in {self.table}')
        
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f'FAILED: no rows in {self.table}')
