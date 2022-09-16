from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):
        """ 
        Constructor function. This function gets the inputs to check data quality on all our tables:
        
        Args:
        redshift_conn_id: redshift connection id established in airflow UI.
        tables: list with the names of all the tables that require a data quality check
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        
        """ 
        Function that executes our custom operator. This operator's job is to check on data quality of the tables list used as input.
        
        Args:
        
        context: environment variables from airflow.
        
        """
        
        redshift = PostgresHook(redshift_conn_id)
        
        for table in self.tables:
            
            self.log.info(f"Checking data quality of table: {table}")
            
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table};")
            
            num_records = records[0][0]
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data Quality check failed. {table} returned no records."
            
            
            if num_records < 1:
                raise ValueError(f"Data Quality check failed. {table} contained 0 rows.")
                                 
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")                                
        