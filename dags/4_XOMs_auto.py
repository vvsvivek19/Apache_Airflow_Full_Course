from airflow.sdk import dag, task 


@dag(
        dag_id="xcoms_dag_auto",
)
def xcoms_dag_auto():

    @task.python
    def first_task():
        print("Extracting the data, this is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task.python
    def second_task(data: dict):
        print("Transforming the data, this is the second task")
        fetched_data = data["data"]
        transformed_data = [x * 2 for x in fetched_data]
        return {"transformed_data": transformed_data}

    @task.python
    def third_task(data: dict):
        loaded_data = data["transformed_data"]
        return loaded_data
    
    
    # Defining task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)
    
    first >> second >> third

# Instantiating the DAG
xcoms_dag_auto()