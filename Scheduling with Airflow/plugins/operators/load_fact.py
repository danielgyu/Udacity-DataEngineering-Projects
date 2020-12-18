from airflow.hooks.postgres_hook import PostgresHook
from airflow.models              import BaseOperator
from airflow.utils.decorators    import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None
                 sql=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        self.log.info('loading to fact table : songplays')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.sql)

