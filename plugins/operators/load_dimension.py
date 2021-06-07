from airflow.hooks.postgres_hook import PostgresHook
from operators import LoadFactOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(LoadFactOperator):
    """
    Class that inserts the Redshift data into the table using the query provided
    as a parameter

    """

    ui_color = '#80BD9E'

    _NAME = 'dimension'

    @apply_defaults
    def __init__(self, append_data: bool, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(
            *args,
            **kwargs
        )

        self._append_data = append_data

    @property
    def append_data(self) -> bool:
        return self._append_data

    def execute(self, context: dict):
        if self.append_data:
            self.log.info(f"Appending data to table {self._redshift_table}")
        else:
            self.log.info(
                f"Truncating table {self._redshift_table}."
            )

            PostgresHook(
                postgres_conn_id=self._redshift_conn_id
            ).run(
                f"truncate table {self._redshift_table};"
            )

        super(LoadDimensionOperator, self).execute(
            context
        )
