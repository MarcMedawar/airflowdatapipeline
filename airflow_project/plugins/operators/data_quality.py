from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    data_quality_checks = [
        {'check_sql' : "WITH users_null AS ( \
                       SELECT COUNT(*) \
                       FROM users \
                       WHERE userid IS NULL )",
         'expected_result' : "SELECT 0"},
        {'check_sql' : "WITH songs_null AS ( \
                       SELECT COUNT(*) \
                       FROM songs \
                       WHERE songid IS NULL )",
         'expected_result' : "SELECT 0"},
        {'check_sql' : "WITH time_null AS ( \
                        SELECT COUNT(*) \
                        FROM time \
                        WHERE start_time IS NULL )",
         'expected_sql' : "SELECT 0"}]
        
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "redshift",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('DataQualityOperator not implemented yet')
        failureCount = 0
        failingTest = []
        
        for check in DataQualityOperator.data_quality_checks:
            sql_query = check.get('check_sql')
            expect_result = check.get('expected_result')
            records = redshift_hook.get_records(sql_query)[0]
            
            if expect_result != records[0]:
                failureCount += 1
                failingTest.append(sql_query)
                
            if failureCount > 0:
                self.log.info("bad data quality")
                self.log.info("number of failed test: {}".format(errorCount))
                self.log.info(failingTest)
                raise ValueError("Fail")
                
            if failureCount == 0:
                self.log.info("Pass")
