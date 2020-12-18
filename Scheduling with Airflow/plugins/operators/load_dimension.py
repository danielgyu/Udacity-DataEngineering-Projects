from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None,
                 table_sql=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_sql = table_sql

    def execute(self, context):
        self.log.info(f'loading using {self.table_sql}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift.run(self.table_sql)
