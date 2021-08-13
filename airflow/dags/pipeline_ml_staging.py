import os

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
from google.protobuf.duration_pb2 import Duration

from airflow.utils import trigger_rule
from airflow.providers.google.cloud.operators.dataproc import(
	DataprocCreateClusterOperator,
	DataprocDeleteClusterOperator,
	DataprocSubmitPySparkJobOperator
)

DAG_NAME = os.path.basename(__file__).replace('.py', '')
BUCKET = 'ml-eng-blent'
CLUSTER_NAME_TEMPLATE = 'purchase-predict-cluster-staging-'
CLUSTER_CONFIG = {
	'software_config': {
		'image_version': '2.0-debian10'
	},
	'master_config': {
		'num_instances': 1,
		'machine_type_uri': 'n1-standard-2',
		'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512}
	},
	'worker_config': {
		'num_instances': 2,
		'machine_type_uri': 'n1-standard-2',
		'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512}	
	},
	'lifecycle_config': {
		'idle_delete_ttl': Duration(seconds=3600)
	}
}

default_args = {
	'owner': 'samanth',
	'start_date': days_ago(2),
	'retries': 0,
	'retry_delay': timedelta(minutes=10),
	'project_id': Variable.get('PROJECT_ID'),
	'region': 'us-central1'
}

with DAG(DAG_NAME, default_args=default_args, schedule_interval='0 5 * * 1') as dag:

	task_create_dataproc = DataprocCreateClusterOperator(
		task_id='create_dataproc',
		cluster_name=CLUSTER_NAME_TEMPLATE + "{{ ds_nodash }}",
		region= 'us-central1',
		cluster_config=CLUSTER_CONFIG
	)

	task_job = DataprocSubmitPySparkJobOperator(
		task_id='submit_job',
		job_name='query_user_events',
		cluster_name=CLUSTER_NAME_TEMPLATE + '{{ ds_nodash }}',
		main='gs://{}/scripts/create_events_data_file.py'.format(BUCKET),
		dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
		arguments=['{{ ds }}', '{{ next_ds }}']
	)


	task_delete_dataproc = DataprocDeleteClusterOperator(
		task_id='delete_dataproc',
		cluster_name=CLUSTER_NAME_TEMPLATE + "{{ ds_nodash }}",
		trigger_rule=trigger_rule.TriggerRule.ALL_DONE
	)

	task_create_dataproc.set_downstream(task_job)
	task_job.set_downstream(task_delete_dataproc)