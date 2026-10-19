# biomasbr_api_sync

### Required Connections

| ID | Type | Description |
|---|---|---|
| `BIOMASBR_AMAZONIA_CONNECTION_DB` | Postgres | Connection to the Amazonia output DB |
| `BIOMASBR_CERRADO_CONNECTION_DB` | Postgres | Connection to the Cerrado output DB |
| `BIOMASBR_PANTANAL_CONNECTION_DB` | Postgres | Connection to the Pantanal output DB |
| `BIOMASBR_AMAZONIA_NF_CONNECTION_DB` | Postgres | Connection to the Amazonia NF output DB |
| `smtp_default` | SMTP | Host: `Your email host`, Login:`Your email login`, Port: `Your email port`, Email From: `Your from email`, Disable SSL: True |

### Required Variables

| Variable Name | Value |
|---|---|
| `GENERAL_EMAIL_TO` | example: `your_email@receiver.com, your_email2@receiver.com` |

## Database Setup

The pipeline stores synchronized data in PostgreSQL databases. Each biome has its own database connection.

## Project Structure

```
biomasbr_api_sync/
├── src/
│   ├── dag.py                          # Main DAG definition
│   ├── biomasbr_configuration/         # Configuration files
│   │   ├── dag_config.py
│   │   └── dag-config.json
│   ├── biomasbr_email_templates/       # Email templates
│   │   ├── success_email.html
│   │   ├── failed_email.html
│   │   └── skipped_email.html
│   ├── biomasbr_utils/                 # Utility modules
│   │   ├── utils.py
│   │   ├── logger.py
│   │   └── database_facade.py
│   ├── tasks_dags/                     # Task implementations
│   │   ├── task_base.py
│   │   ├── task_01_trigger.py
│   │   ├── task_02_sync_data.py
│   │   ├── task_03_validate_data.py
│   │   ├── task_04_cleanup.py
│   │   └── output_database.py
│   └── files/                          # Temporary files directory
│       └── temp/
├── venv/                               # Python virtual environment
├── readme.md
├── .gitignore
└── .airflowignore
```
