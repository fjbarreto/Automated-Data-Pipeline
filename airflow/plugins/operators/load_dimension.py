from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 insert_sql = "",
                 dimension_table = "",
                 *args, **kwargs):
        """ 
        Constructor function. This function gets the inputs to load data from staging tables to fact table 'songplays':
        
        Args:
        redshift_conn_id: redshift connection id established in airflow UI.
        insert_sql: insert sql statement needed to insert data into dimension tables.
        dimension_table: name of dimension table, used for logging.
        
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql = insert_sql

    def execute(self, context):
        """ 
        Function that executes our custom operator. This operator's job is to load data into dimension tables from staging tables.
        
        Args:
        
        context: environment variables from airflow.
        
        """
        
        self.log.info('Loading dimension table {self.dimension_table}')
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        redshift.run(self.insert_sql)
