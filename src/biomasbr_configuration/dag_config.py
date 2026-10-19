import json
from pathlib import Path


class DAG_Configuration:

    def __init__(self):
        self.conf_path = Path(__file__).resolve().parent.parent / "biomasbr_configuration" / "dag-config.json"
        self.dag_config_json = None

    def dag_config(self):
        """Load and validate the dag configuration."""
        self.load_dag_config()

    def load_dag_config(self):
        if not self.conf_path.exists():
            raise FileNotFoundError(f"dag-config.json file not found at {self.conf_path}")
        with open(self.conf_path) as json_data:
            self.dag_config_json = json.load(json_data)
        return self.dag_config_json

