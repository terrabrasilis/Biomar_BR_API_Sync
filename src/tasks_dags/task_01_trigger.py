"""Task 01: Trigger - Check if there is new data to sync."""


def task_callable_01(project_dir: str):
    """Check if there is new data since last sync."""
    import sys
    sys.path.append(project_dir)

    from datetime import datetime
    from biomasbr_utils.utils import Utils

    utils = Utils()
    db = utils.get_db_facade()

    # Check if table is empty (first run)
    count_query = "SELECT COUNT(*) FROM nup"
    count_result = db.fetchone(count_query)
    count = count_result[0] if isinstance(count_result, (tuple, list)) else count_result or 0

    if count == 0:
        print(f"[DEBUG] Tabela vazia - primeira execução, iniciando carga inicial")
        return True

    # Check if there are new records from today
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"[DEBUG] Buscando dados da API para data: {today}")

    try:
        from airflow.models import Variable

        params = {"page": 0, "size": 1, "dataInsercao": today}
        response = utils.api_request(
            endpoint="solicitacoes",
            conn_id="BIOMAS_BR_API",
            params=params,
            extra_headers={
                "X-App-Code": Variable.get("BIOMASBR_APP_CODE"),
                "X-App-Name": Variable.get("BIOMASBR_APP_NAME"),
            }
        )

        has_content = bool(response and response.get("content"))
        if has_content:
            qtd = len(response.get("content", []))
            print(f"[DEBUG] Dados encontrados ({qtd} registros) para {today}")
        else:
            print(f"[DEBUG] Nenhum dado encontrado para {today}")

        return has_content
    except Exception as e:
        print(f"[DEBUG] Erro na API: {str(e)}")
        return False
