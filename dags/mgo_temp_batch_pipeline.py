from airflow import DAG
import os
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from airflow.contrib.operators.dataflow_operator import *

airflow_env = os.environ.get('AIRFLOW_ENV')
airflow_gcs = os.environ.get('AIRFLOW_GCS')
google_project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
df_jar =  os.environ.get('DF_JAR_LOCATION')

default_args = {
    'owner': 'mgo_couchbase_streaming',
    'depends_on_past': True,
    'start_date': datetime(2020, 8, 6),
    #'end_date': datetime(2019, 12, 31),
    # 'retry_delay': timedelta(minutes=5)
    'dataflow_default_options': {
        #'project': 'mgi-edw-dev',
        'zone': 'us-central1-f',
        #'stagingLocation':  os.environ.get('DF_STG_LOCATION'),
        'stagingLocation': 'gs://us-central1-edw-dev-04c8db8d-bucket/dataflow/stg/'
    }
}

df_options = {
    'project': google_project_id,
    'stagingLocation':  os.environ.get('DF_STG_LOCATION'),
    'gcpTempLocation':  os.environ.get('DF_TEMP_LOCATION'),
    'serviceAccount':  os.environ.get('DF_SERVICE_ACCOUNT'),
    'subnetwork': 'https://www.googleapis.com/compute/v1/projects/{host_id}/regions/us-central1/subnetworks/{subnet}'.format(host_id = os.environ.get('DF_HOSTNET'), subnet = os.environ.get('DF_SUBNET')),
    'maxNumWorkers': '10',
    'start': '{{ds}}',
    'env_properties' : 'gs://us-central1-edw-dev-04c8db8d-bucket/dataflow/streaming/mgo-couchbase/options-error-handling-dev.properties'

}

dag = DAG('mgo_couchbase_error_processing_batch_pipeline_v1', default_args=default_args,
          schedule_interval=None
          )

task = DataFlowJavaOperator(
    #gcp_conn_id='gcp_default',
    task_id='mgo_couchbase_streaming_error_handling-v1',
    jar='gs://us-central1-edw-dev-04c8db8d-bucket/dataflow/streaming/mgo-couchbase/dataflow-0.0.1-SNAPSHOT-shaded.jar',
    #df_jar =  os.environ.get('DF_JAR_LOCATION'),
    job_class='com.mgi.datalake.dataflow.pipeline.ErrorHandlingDataPipeline',
    #gcp_conn_id=None,
    options=df_options,
    dag=dag)