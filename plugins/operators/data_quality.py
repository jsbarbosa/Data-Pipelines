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
            redshift_conn_id: str,
            tests: dict,
            *args,
            **kwargs
    ):
        """
        Initializer

        Parameters
        ----------
        tests:
            redshift tables and queries to be verified
        redshift_conn_id:
            reference to the Airflow redshift connection
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self._redshift_conn_id = redshift_conn_id
        self._tests = tests

    @property
    def tests(self) -> dict:
        return self._tests

    @property
    def redshift_conn_id(self) -> str:
        return self._redshift_conn_id

    def check_records(self, table: str, tests: list, hook: PostgresHook):
        """
        Method that verifies if the query was successful and if records exist
        in the table parameter
        """
        records = hook.get_records(
            self.QUERY.format(
                table=table
            )
        )

        for test in tests:
            if not eval(test):
                self.log.error(
                    f"Table '{table}' failed check. When running: '{test}'"
                )
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
            f"Checks for tables: {list(self._tests.keys())}"
        )

        hook = PostgresHook(
            postgres_conn_id=self._redshift_conn_id
        )

        for table, tests in self._tests.items():
            self.check_records(
                table,
                tests,
                hook
            )

            self.log.info(
                f"Check for table '{table}' was successful"
            )
