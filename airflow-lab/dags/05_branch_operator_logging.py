from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime

log = LoggingMixin().log

def choose_path(**context):
    logical_date = context["logical_date"]
    weekday = logical_date.weekday()
    log.info("logical_date=%s weekday=%s", logical_date, weekday)

    if weekday < 5:
        return "weekday_task"
    return "weekend_task"

@dag(
    dag_id="op_branch_logging_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "tutorial"],
)
def branch_logging_demo():
    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_path,
    )

    weekday = BashOperator(task_id="weekday_task", bash_command="echo 'weekday'")
    weekend = BashOperator(task_id="weekend_task", bash_command="echo 'weekend'")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> branch >> [weekday, weekend] >> end

branch_logging_demo()