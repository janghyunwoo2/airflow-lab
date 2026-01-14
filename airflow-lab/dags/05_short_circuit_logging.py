from airflow.decorators import dag
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime

log = LoggingMixin().log

def should_run(**context):
    logical_date = context["logical_date"]
    result = (logical_date.minute % 2) == 0
    log.info("logical_date=%s should_run=%s", logical_date, result)
    return result

@dag(
    dag_id="op_short_circuit_logging_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "tutorial"],
)
def short_circuit_logging_demo():
    start = EmptyOperator(task_id="start")

    gate = ShortCircuitOperator(
        task_id="gatekeeper",
        python_callable=should_run,
    )

    work = BashOperator(
        task_id="do_work",
        bash_command="echo 'work running'",
    )

    end = EmptyOperator(task_id="end")

    start >> gate >> work >> end

short_circuit_logging_demo()