from airflow.sdk import dag, task
from more_itertools import first 
from pendulum import datetime, duration, timezone
from airflow.timetables.trigger import DeltaTriggerTimetable, CronTriggerTimetable


@dag(
        dag_id="delta_scheduled_dag",
        start_date=datetime(year=2026, month=11, day=5, tz="Asia/Kolkata"),
        catchup=True,
        is_paused_upon_creation=True,
        schedule=DeltaTriggerTimetable(duration(days=3)), # This is a delta expression for 3 days
        end_date=datetime(year=2026, month=11, day=10, tz="Asia/Kolkata")  # Optional: Set an end date for the schedule
)
def delta_scheduled_dag():

    @task.python
    def extract_task(**kwargs):
        print("Extracting the data, this is the first task")
        ti = kwargs["ti"]
        extracted_data = {"api_extracted_data": [1, 2, 3, 4, 5],
                          "db_extracted_data": [6, 7, 8, 9, 10],
                          "s3_extracted_data": [11, 12, 13, 14, 15],
                          "weekend_flag": False}
        ti.xcom_push(key="return_value", value=extracted_data)

    @task.python
    def transform_task_api(**kwargs):
        print("Transforming the API data, this is the second task")
        ti = kwargs["ti"]
        extracted_data = ti.xcom_pull(key="return_value", task_ids="extract_task")
        api_data = extracted_data["api_extracted_data"]
        transformed_api_data = [x * 2 for x in api_data]
        ti.xcom_push(key="return_value", value=transformed_api_data)
    
    @task.python
    def transform_task_db(**kwargs):
        print("Transforming the DB data, this is the third task")
        ti = kwargs["ti"]
        extracted_data = ti.xcom_pull(key="return_value", task_ids="extract_task")
        db_data = extracted_data["db_extracted_data"]
        transformed_db_data = [x * 2 for x in db_data]
        ti.xcom_push(key="return_value", value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        print("Transforming the S3 data, this is the fourth task")
        ti = kwargs["ti"]
        extracted_data = ti.xcom_pull(key="return_value", task_ids="extract_task")
        s3_data = extracted_data["s3_extracted_data"]
        transformed_s3_data = [x * 2 for x in s3_data]
        ti.xcom_push(key="return_value", value=transformed_s3_data)

    @task.bash
    def load_task(**kwargs):
        print("Loading data to destination, this is the fifth task")
        api_data = kwargs["ti"].xcom_pull(key="return_value", task_ids="transform_task_api")
        db_data = kwargs["ti"].xcom_pull(key="return_value", task_ids="transform_task_db")
        s3_data = kwargs["ti"].xcom_pull(key="return_value", task_ids="transform_task_s3")
        return f"echo 'Loaded data: API: {api_data}, DB: {db_data}, S3: {s3_data}'"

    @task.bash
    def no_load_task(**kwargs):
        print("This is a branch task that does not load data")
        return "echo 'No data to load since it is weekend, this is a branch task'"


    # creating decider based on weekend_flag
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs["ti"]
        extracted_data = ti.xcom_pull(key="return_value", task_ids="extract_task")
        weekend_flag = extracted_data["weekend_flag"]
        if weekend_flag:
            return "no_load_task"
        else:
            return "load_task"
    
    # Defining task dependencies
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    decider = decider_task()
    load = load_task()
    no_load = no_load_task()
    
    extract >> [transform_api, transform_db, transform_s3] >> decider >> [load, no_load]

# Instantiating the DAG
delta_scheduled_dag()