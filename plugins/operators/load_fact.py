from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Class that inserts the Redshift data into the table using the query provided as a parameter

    """
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
        """

        Parameters
        ----------
        redshift_table:
            table in which the data will be uploaded
        query:
            SQL statement with the values to insert
        redshift_conn_id:
            reference to the Airflow redshift connection
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self._redshift_table = redshift_table
        self._query = query
        self._redshift_conn_id = redshift_conn_id

    def execute(self, context: dict):
        """
        Method that executes the insertion procedure

        Parameters
        ----------
        context:
            Airflow context
        """
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
