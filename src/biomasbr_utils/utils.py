import os
import json
import requests
import pytz
from pathlib import Path
from datetime import datetime, timedelta
from string import Template

from airflow.hooks.base import BaseHook
from airflow.models import Variable


class Utils:

    def get_current_date(self):
        """Get the current date"""
        return datetime.now().date()

    def get_current_datetime_brasilia(self):
        """Get current datetime in Brasilia timezone"""
        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        now_brasilia = datetime.now(brasilia_tz)
        return now_brasilia.strftime("%d/%m/%Y %H:%M:%S")

    def get_date_range(self, days_back=7):
        """Get date range for API queries"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def get_api_connection(self, conn_id, extra_headers=None):
        """Get API connection from Airflow Connections"""
        conn = BaseHook.get_connection(conn_id)

        host = conn.host or ""
        if "://" in host:
            base_url = host.rstrip("/")
        else:
            schema = conn.schema or "https"
            port = f":{conn.port}" if conn.port else ""
            base_url = f"{schema}://{host}{port}"

        extra = conn.extra_dejson if conn.extra else {}
        base_path = extra.get("base_path", "")
        if base_path:
            base_url = f"{base_url}/{base_path.strip('/')}"

        headers = extra.get("headers", {})
        if not isinstance(headers, dict):
            headers = {}

        if extra_headers:
            headers.update(extra_headers)

        return {
            "base_url": base_url,
            "headers": headers,
        }

    def get_db_facade(self):
        """Get DatabaseFacade from Airflow Connection"""
        from biomasbr_utils.database_facade import DatabaseFacade

        dag_config_path = Path(__file__).resolve().parent.parent / "biomasbr_configuration" / "dag-config.json"
        with open(dag_config_path) as json_data:
            config = json.load(json_data)

        conn_id = config.get("output_connection")
        conn = BaseHook.get_connection(conn_id)
        db_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        return DatabaseFacade.from_url(db_url)

    def api_request(self, endpoint, conn_id, params=None, extra_headers=None):
        """Make API request"""
        api_conn = self.get_api_connection(conn_id, extra_headers=extra_headers)
        url = f"{api_conn['base_url']}/{endpoint}"
        try:
            response = requests.get(url, params=params, headers=api_conn["headers"], timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")

    @staticmethod
    def read_html_template(filename, **kwargs):
        """Read and format email template"""
        EMAIL_TEMPLATES_DIR = "/opt/airflow/projects/biomasbr_api_sync/src/biomasbr_email_templates"
        with open(os.path.join(EMAIL_TEMPLATES_DIR, filename), 'r') as file:
            template = Template(file.read())
            return template.substitute(**kwargs)

    @staticmethod
    def branch_func(**context):
        """Branch function to determine workflow path"""
        ti = context['ti']
        result = ti.xcom_pull(task_ids='1_trigger_task')
        if result is True:
            return '2_sync_data_task'
        else:
            return 'process_skipped'

    def get_email_list(self):
        """Get the list of email addresses from the Airflow variable."""
        email_list = Variable.get("GENERAL_EMAIL_TO")
        if not email_list:
            raise ValueError("Email list is empty. Please set the GENERAL_EMAIL_TO variable.")
        return [email.strip() for email in email_list.split(",") if email.strip()]
