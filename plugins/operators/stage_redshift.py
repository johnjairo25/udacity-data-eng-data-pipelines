from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    copy_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_type="",
                 ignore_headers=1,
                 delimiter=",",
                 json_parameter="auto",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter
        self.json_parameter = json_parameter

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info(f'The source path is: {s3_path}')
        if self.file_type.lower() == 'json':
            formatted_sql = StageToRedshiftOperator.copy_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_parameter
            )
        elif self.file_type.lower() == 'csv':
            formatted_sql = StageToRedshiftOperator.copy_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
        else:
            raise ValueError('file_type must be either json or csv')

        self.log.info(f'About to execute the command: {formatted_sql}')
        redshift.run(formatted_sql)
