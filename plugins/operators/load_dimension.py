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
