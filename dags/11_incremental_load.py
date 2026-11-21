from airflow.sdk import dag, task
from pendulum import datetime 
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    schedule= CronDataIntervalTimetable("@daily", timezone= "Asia/Kolkata"),
    start_date=datetime(year=2026, month=11, day=20, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=11, day=23, tz="Asia/Kolkata"),
    catchup= True
)
def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_data_process():
        return "echo 'processing incremental data from {{date_interval_start}} to {{date_interval_end}}"


    fetch_task = incremental_data_fetch()
    process = incremental_data_process()
    fetch_task >> process


#initializing the dag
incremental_load_dag()