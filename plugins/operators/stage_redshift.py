from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Operator that copies data from S3 into redshift
    """

    ui_color = '#358140'

    EXECUTE_STATEMENT: str = """
        COPY {table}
        FROM 's3://{path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION AS '{region}';
    """

    @apply_defaults
    def __init__(
            self,
            bucket: str,
            s3_conn_id: str,
            redshift_table: str,
            redshift_conn_id: str,
            region: str,
            *args,
            **kwargs
    ):
        """
        Initialize StageToRedshiftOperator

        Parameters
        ----------
        bucket:
            path of the S3 data to copy
        s3_conn_id:
            id of the S3 Hook
        redshift_table:
            table in which data will be copied
        redshift_conn_id:
            id of the Redshift Hook
        region:
            region of the S3 bucket
        args
        kwargs
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self._bucket = bucket
        self._s3_conn_id = s3_conn_id
        self._redshift_table = redshift_table
        self._redshift_conn_id = redshift_conn_id
        self._region = region

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def s3_conn_id(self) -> str:
        return self._s3_conn_id

    @property
    def redshift_table(self) -> str:
        return self._redshift_table

    @property
    def redshift_conn_id(self) -> str:
        return self._redshift_conn_id

    def execute(self, context: dict):
        """
        Method that executes the AWS CLI COPY statement

        Parameters
        ----------
        context:
            Airflow context

        Returns
        -------

        """
        aws = AwsHook(
            aws_conn_id=self._s3_conn_id,
        ).get_credentials()

        self.log.info(
            f"Retrieving credentials from S3"
        )

        hook = PostgresHook(
            postgres_conn_id=self._redshift_conn_id
        )

        self.log.info(
            f"Building Redshift Hook"
        )

        hook.run(
            self.EXECUTE_STATEMENT.format(
                table=self._redshift_table,
                path=self._bucket,
                access_key=aws.access_key,
                secret_key=aws.secret_key,
                region=self._region
            ),
            autocommit=True
        )

        self.log.info(
            f"Data from '{self._bucket}' staged to '{self._redshift_table}'"
        )
