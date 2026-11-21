
import pendulum

from airflow.sdk import dag, task
from airflow.timetables.events import EventsTimetable


special_dates = EventsTimetable(
    event_dates=[
        pendulum.datetime(2026, 11, 21, tz="America/Halifax"),
        pendulum.datetime(2026, 11, 22, tz="America/Halifax"),
        pendulum.datetime(2026, 11, 23, tz="America/Halifax"),
        pendulum.datetime(2026, 11, 24, tz="America/Halifax")
    ]
)


@dag(
    dag_id="special_dates_dag",
    schedule=special_dates,
    start_date=pendulum.datetime(
        2026, 11, 20, tz="America/Halifax"
    ),
    end_date=pendulum.datetime(
        2026, 11, 30, tz="America/Halifax"
    ),
    catchup=True
)
def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']

        print(
            f"Running task for special event on {execution_date}"
        )

    special_event = special_event_task()


special_dates_dag()
