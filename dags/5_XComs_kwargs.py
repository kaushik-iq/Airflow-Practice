from airflow.sdk import dag, task

@dag(
    dag_id= "xcoms_dag_manual",

)
def xcoms_dag_manual():

    @task.python
    def first_task(**kwargs):

        ti = kwargs['ti']
        print("Exacting data...... this is the first task")
        fetched_data = {"data": [1,2,3,4,5]}
        ti.xcom_push(key='return_result', value = fetched_data)


    @task.python
    def second_task(**kwargs):

        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(key='return_result', task_ids='first_task')['data']
        print("Transformed data.... this is my second task")
        transformed_data  = fetched_data*2
        transformed_data_dict = {"transf_data": transformed_data}

        ti.xcom_push(key='return_result', value = transformed_data_dict)
        
    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data = ti.xcom_pull(key='return_result', task_ids='second_task')
        return load_data


    #Defining task dependencies 

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


# Instantiating the dag
xcoms_dag_manual()