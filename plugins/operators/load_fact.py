from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    sql_command = """
        INSERT INTO {} 
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 source_sql_command="",
                 delete_data=False,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.source_sql_command = source_sql_command
        self.delete_data = delete_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_data:
            self.log.info(f'About to delete all data from: {self.table}')
            redshift.run(f'DELETE FROM {self.table}')

        formatted_sql = LoadFactOperator.sql_command.format(
            self.table,
            self.source_sql_command
        )

        self.log.info(f'About to execute: {formatted_sql}')
        redshift.run(formatted_sql)
