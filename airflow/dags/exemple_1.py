import os
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DAG_NAME = os.path.basename(__file__).replace(".py", "")

default_args = {
	'owner': 'samanth',
	'retries': 1,
	'retry_delay': timedelta(seconds=10)
}


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2))
def dag_exemple():
  """
  Ce DAG est un exemple pour apprendre à utiliser airflow.
  """
  @task()
  def date(**kwargs):
    print("La date d'exécution est : {}".format(kwargs["date"]))

  @task()
  def yesterday(**kwargs):
    print("La veille de la date d'exécution est :{}".format(kwargs["yesterday"]))

  task_date = date(date="{{ ds }}")
  task_bash = BashOperator(task_id="echo", bash_command="echo 'Hello!'")
  task_yesterday = yesterday(yesterday="{{ yesterday_ds }}")

  task_date.set_downstream(task_bash)
  task_bash.set_downstream(task_yesterday)

dag_exemple_instance = dag_exemple()
