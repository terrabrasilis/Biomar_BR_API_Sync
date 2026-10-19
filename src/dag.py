import os
import sys
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(PROJECT_DIR))

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonVirtualenvOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

from biomasbr_utils.utils import Utils

from tasks_dags.task_01_trigger import task_callable_01
from tasks_dags.task_02_sync_data import task_callable_02

requirements = [
    "requests",
    "psycopg2-binary",
    "sqlalchemy",
    "pydantic"
]

venv_path = "/opt/airflow/venv/inpe/biomasbr_api_sync/venv"

with DAG('biomasbr_api_sync',
    schedule_interval='@daily',
    default_args={'start_date': datetime(2025, 1, 1)},
    tags=['biomasbr', 'api', 'sync'],
    max_active_runs=1,
    catchup=False) as dag:

    task_1 = PythonVirtualenvOperator(
        task_id="1_trigger_task",
        requirements=requirements,
        python_callable=task_callable_01,
        system_site_packages=True,
        op_kwargs={"project_dir": PROJECT_DIR},
        venv_cache_path=venv_path
    )

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=Utils().branch_func,
        provide_context=True,
        dag=dag,
    )

    task_2 = PythonVirtualenvOperator(
        task_id='2_sync_data_task',
        requirements=requirements,
        python_callable=task_callable_02,
        system_site_packages=True,
        op_kwargs={"project_dir": PROJECT_DIR},
        venv_cache_path=venv_path
    )

    # process_success = EmailOperator(
    #     task_id='process_success',
    #     to=Utils().get_email_list(),
    #     subject='BiomasBR API Sync Completed Successfully',
    #     html_content=Utils().read_html_template(
    #         'success_email.html',
    #         task_name='BiomasBR API Sync',
    #         date=Utils().get_current_datetime_brasilia(),
    #     ),
    #     trigger_rule=TriggerRule.ALL_SUCCESS
    # )

    # process_failed = EmailOperator(
    #     task_id='process_failed',
    #     to=Utils().get_email_list(),
    #     subject='BiomasBR API Sync Failed',
    #     html_content=Utils().read_html_template(
    #         'failed_email.html',
    #         task_name='BiomasBR API Sync',
    #         date=Utils().get_current_datetime_brasilia(),
    #     ),
    #     trigger_rule=TriggerRule.ONE_FAILED
    # )

    process_skipped = EmailOperator(
        task_id='process_skipped',
        to=Utils().get_email_list(),
        subject='BiomasBR API Sync Interrupted',
        html_content=Utils().read_html_template(
            'skipped_email.html',
            task_name='BiomasBR API Sync',
            date=Utils().get_current_datetime_brasilia(),
        )
    )

    trigger_self = TriggerDagRunOperator(
        task_id='trigger_self',
        trigger_dag_id='biomasbr_api_sync',
        conf={"message": "Rescheduling the BiomasBR API sync for the next cycle."},
    )

    task_1 >> branch >> [task_2, process_skipped]
    # task_2 >> [process_success, process_failed]
    # process_success >> trigger_self
    task_2 >> trigger_self
