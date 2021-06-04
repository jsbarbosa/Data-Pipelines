from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Class that reads the Redshift data in order to verify that the pipeline
    was correct

    """
    ui_color = '#89DA59'

    QUERY: str = "SELECT COUNT(*) FROM {table};"

    @apply_defaults
    def __init__(
            self,
            tables: list,
            redshift_conn_id: str = "",
            *args,
            **kwargs
    ):
        """
        Initializer

        Parameters
        ----------
        tables:
            redshift tables to be verified
        redshift_conn_id:
            reference to the Airflow redshift connection
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self._tables = tables
        self._redshift_conn_id = redshift_conn_id

    @property
    def tables(self) -> list:
        return self._tables

    @property
    def redshift_conn_id(self) -> str:
        return self._redshift_conn_id

    def check_records(self, table: str, records: list):
        """
        Method that verifies if the query was successful and if records exist
        in the table parameter
        """
        if len(records) == 0 or len(records[0]) == 0 or records[0][0] == 0:
            self.log.error(f"Table '{table}' failed check.")
            raise ValueError(f"Table '{table}' failed check.")

    def execute(self, context: dict):
        """
        Method that executes the data quality procedure

        Parameters
        ----------
        context:
            Airflow context
        """

        self.log.info(
            f"Checks for tables: {self._tables}"
        )

        hook = PostgresHook(
            postgres_conn_id=self._redshift_conn_id
        )

        for table in self._tables:
            self.check_records(
                table,
                hook.get_records(
                    self.QUERY.format(
                        table=table
                    )
                )
            )

            self.log.info(
                f"Check for table '{self._tables}' was successful"
            )
