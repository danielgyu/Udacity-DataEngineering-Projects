from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook    import PostgresHook
from airflow.models                 import BaseOperator
from airflow.utils.decorators       import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Add Description !!!
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    s3_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_credential_id=None,
                 redshift_conn_id=None,
                 table=None,
                 s3_bucket=None,
                 s3_key=None,
                 ignore_header=1,
                 delimiter=',',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credential = aws_credential_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_header = ignore_header
        self.delimiter = delimiter

    def execute(self, context):
        """"""
        # establish connections
        self.log.info('establishing connections')
        aws_hook = AwsHook(self.aws_credential)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # delete if table exists
        self.log.info('deleting table if exists')
        redshift_hook.run(f"""TRUNCATE {self.table}""")

        # copy data from s3 to redshift
        rendered_s3_key = self.s3_key.format(**context)
        s3_bucket = "s3a://{}/{}".format(self.s3_bucket, rendered_s3_key)
        sql = StageToRedshiftOperator.s3_copy_sql.format(
            self.table,
            s3_bucket,
            credentials.access_key(),
            credentials.secret_key(),
            self.ignore_header,
            self.delimiter
        )
        redshift.run(sql)
