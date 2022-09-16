from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ['s3_key']
    
    copy_sql= """
          COPY {}
          FROM '{}'
          ACCESS_KEY_ID '{}'
          SECRET_ACCESS_KEY '{}'
          REGION 'us-west-2'
          JSON '{}'
              """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 bucket_directory = "",
                 s3_jsonpath = "auto",
                 s3_key = "",
                 *args, **kwargs):
        """ 
        Constructor function. This function gets the inputs to export data from S3 bucket to our staging tables in redshift cluster:
        
        Args:
        redshift_conn_id: redshift connection id established in airflow UI.
        aws_credentials_id: aws credentials established in airflow UI.
        table: name of the staging table we wish to fill.
        s3_jsonpath: s3 bucket directory with json files we wish to import to our redshift cluster.
        s3_key: path key to partition json files ingestion for events staging table
        
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.bucket_directory = bucket_directory
        self.s3_jsonpath = s3_jsonpath
        self.s3_key = s3_key
        
        
    def execute(self, context):
        """ 
        Function that executes our custom operator. This operator's job is to delete records if existing and write json files again in staging tables.
        
        Args:
        
        context: environment variables from airflow.
        
        """
        
        
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(f'Clearing records from {self.table}')
        
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Copying records from S3 to redshift.')
        
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "{}/{}".format(self.bucket_directory, rendered_key)
        
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_jsonpath
        )
        
        redshift.run(formatted_sql)
            
                                        
        
        
        





