import os
import secrets

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

from airflow.providers.google.cloud.operators.cloud_build import (
	CloudBuildCreateBuildOperator
)

CLOUD_BUILD_STEP_ARGS= """
gcloud source repos clone purchase_predict_2 /tmp/purchase_predict_2 --project=$PROJECT_ID
git --git-dir=/tmp/purchase_predict_2/.git --work-tree=/tmp/purchase_predict_2 checkout staging
tar -C /tmp/purchase_predict_2 -zcf /tmp/purchase_predict_2.tar.gz .
gcloud builds submit \
--config /tmp/purchase_predict_2/cloudbuild.yaml /tmp/purchase_predict_2.tar.gz \
--substitutions SHORT_SHA=$SHORT_SHA,_MLFLOW_SERVER=$_MLFLOW_SERVER,BRANCH_NAME=staging
"""

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
	'region': 'europe-west1'
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

	task_trigger_build = CloudBuildCreateBuildOperator(
		task_id="trigger_ml_build",
		body={
			"steps": [
				{
					"name": "gcr.io/google.com/cloudsdktool/cloud-sdk",
					"entrypoint": "bash",
					"args": ["-c", CLOUD_BUILD_STEP_ARGS]
				}
			],
			"timeout": "1800s",
			"substitutions": {"_MLFLOW_SERVER": Variable.get("MLFLOW_SERVER"), "SHORT_SHA": str(secrets.token_hex(4))[:7]}
		}
	)

	task_create_dataproc.set_downstream(task_job)
	task_job.set_downstream([task_delete_dataproc, task_trigger_build])