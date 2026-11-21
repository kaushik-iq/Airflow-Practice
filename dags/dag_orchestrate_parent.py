from dag_orchestrate_1 import first_orchestrate_dag
from dag_orchestrate_2 import second_orchestrate_dag
from airflow.sdk import dag, task

@dag
def dag_orchestrate_parents():

    @task
    def first_orchestrate_task():
        first_orchestrate_dag()



    @task
    def second_orchestrate_task():
        second_orchestrate_dag()

    first_orchestrate_task() >> second_orchestrate_task()


dag_orchestrate_parents()