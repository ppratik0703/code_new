import datetime
from airflow import DAG
from operators.bigquery_to_db_operator import BigQueryToDBOperator
from operators.incremental_load_range_db_operator import IncrementalLoadRangeDBOperator
from operators.bq_to_db_recon_operator import BQtoDBReconOperator
from operators.db_sql_statement_operator import DBSQLStatementOperator



incremental_load_range_dict ={}

#Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 4, 13),
    'retries': 0
}

#Schedule the dag
schedule_interval = None

# TODO : Enter Correct Values
variable_dict = {
    '{env}': '',
    '{data_tracking_conn_name}': '',
    '{bq_project_id}':'e-might-310422'
}

xcom_recon_dict={}

### DAG for BigQuery to SQL Export ####

with DAG('bq_to_cloudsql_dag', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

# Get load start datetime for BQ to SQL Data Transfer Load from Cloud SQL
    get_start_datetime = IncrementalLoadRangeDBOperator(
        task_id="get_start_datetime_for_data_transfer",
        db_conn_id="sql_server",
        db_table_name="test_table_final"
    )

# BigQueryToSQLOperator will pull data from BigQuery table and push it to Custom SQL hook for inserting into staging table according to the load start date
    bigquery_to_sql_data_dump = BigQueryToDBOperator(
        task_id='bigquery_to_sql_data_dump',
        bq_conn_id="bigquery_default",
        bq_select_query= "/home/airflow/gcs/data/bq_pull.sql",
        db_conn_type = 'SQL',
        db_conn_id="sql_server",
        db_table_name="test_table_staging",
        variable_dict=variable_dict,
        truncate_staging = True,
        xcom_recon_dict=xcom_recon_dict
    )

# ETL Recon DAG will check the number the rows pulled from BQ and number of records inserted in sql server staging table
# if row count doesn't match , DAG will mark it as fail
    etl_recon = BQtoDBReconOperator(
        task_id='run_etl_recon',
        db_conn_id="sql_server",
        db_type = "SQL",
        db_table_name="test_table_staging"
    )

#staging_to_operational will insert all data from staging table in sql server to Final table
    staging_to_operational = DBSQLStatementOperator(
        task_id='staging_to_operational',
        db_conn_id="sql_server",
        db_type="SQL",
        db_query="/home/airflow/gcs/data/staging_to_operational.sql"
    )


#Task Order
get_start_datetime >> bigquery_to_sql_data_dump >> etl_recon >> staging_to_operational
# >>  ilr_write