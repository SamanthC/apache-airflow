import os
import random

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash import BashOperator

DAG_NAME=os.path.basename(__file__).replace(".py", "")

default_args = {
  'owner': 'samanth',
  'retries': 1,
  'retry_delay': timedelta(seconds=10)
}

@dag(DAG_NAME, default_args=default_args, schedule_interval='0 0 * * *', start_date=days_ago(2))
def dag_exple2():
  @task()
  def generate():
    context=get_current_context()
    number = random.randint(0,10)
    context['ti'].xcom_push(key='number', value=number)

  @task()
  def echo():
    context = get_current_context()
    number = context['ti'].xcom_pull(key='number', task_ids='generate')
    print('Le nombre généré est {}'.format(number))

  @task(trigger_rule='all_done')
  def compute():
    context = get_current_context()
    number = context['ti'].xcom_pull(key='number', task_ids='generate')
    cumsum = sum([i for i in range (1, number+1)])
    print('La somme cumulée de 1 à {} est {}'.format(number, cumsum))


  task_generate = generate()
  task_compute = compute()
  task_echo = echo()

  task_sleep = BashOperator(task_id='sleep', bash_command='sleep 3')
  task_error = BashOperator(task_id='sleeeep', bash_command='sleeeeep 3')

  task_generate.set_downstream(task_echo)
  task_generate.set_downstream(task_sleep)
  task_sleep.set_downstream(task_error)
  task_error.set_downstream(task_compute)

dag_2 = dag_exple2()
