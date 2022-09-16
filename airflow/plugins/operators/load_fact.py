from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 insert_sql = "",
                 fact_table = "",
                 *args, **kwargs):
        """ 
        Constructor function. This function gets the inputs to load data from staging tables to fact table 'songplays':
        
        Args:
        
        redshift_conn_id: redshift connection id established in airflow UI.
        insert_sql: insert sql statement needed to insert data in fact table.
        fact_table: name of fact table, used for logging.
        
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql = insert_sql

    def execute(self, context):
        """ 
        Function that executes our custom operator. This operator's job is to load data into fact table from staging tables.
        
        Args:
        
        context: environment variables from airflow.
        
        """
        
        self.log.info('Loading Fact Table {self.fact_table}')
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        redshift.run(self.insert_sql)
        
        
                        
