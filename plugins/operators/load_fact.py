from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    EXECUTE_STATEMENT: str = "INSERT INTO {table} {query};"

    @apply_defaults
    def __init__(
            self,
            redshift_table: str = "",
            query: str = "",
            redshift_conn_id: str = "",
            *args,
            **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self._redshift_table = redshift_table
        self._query = query
        self._redshift_conn_id = redshift_conn_id

    def execute(self, context: dict):
        self.log.info(
            f"Loading fact '{self._redshift_table}' table"
        )

        PostgresHook(
            postgres_conn_id=self._redshift_conn_id
        ).run(
            self.EXECUTE_STATEMENT.format(
                table=self._redshift_table,
                query=self._query
            )
        )
