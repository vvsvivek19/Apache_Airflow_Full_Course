from airflow.sdk import dag, task 


@dag(
        dag_id="xcoms_dag_kwargs",
)
def xcoms_dag_kwargs():

    @task.python
    def first_task(**kwargs):
        
        # Extracting ti from kawargs to push XComs manually
        ti = kwargs["ti"]

        print("Extracting the data, this is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key="fetched_data", value=fetched_data)

    @task.python
    def second_task(**kwargs):

        ti = kwargs["ti"]
        # Pulling the XComs value from the first task
        fetched_data = ti.xcom_pull(key="fetched_data", task_ids="first_task")["data"]
        print("Transforming the data, this is the second task")
        transformed_data = [x * 2 for x in fetched_data]
        ti.xcom_push(key="transformed_data", value={"transformed_data": transformed_data})

    @task.python
    def third_task(**kwargs):
        ti = kwargs["ti"]
        # Pulling the XComs value from the second task
        loaded_data = ti.xcom_pull(key="transformed_data", task_ids="second_task")["transformed_data"]
        return loaded_data
    
    
    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    
    first >> second >> third

# Instantiating the DAG
xcoms_dag_kwargs()