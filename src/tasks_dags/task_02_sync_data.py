"""Task 02: Sync Data - Synchronize data from API to database."""


def task_callable_02(project_dir: str):
    """Sync data from BiomasBR API to local database."""
    import sys
    sys.path.append(project_dir)

    from datetime import datetime, timedelta
    from biomasbr_utils.utils import Utils

    utils = Utils()
    db = utils.get_db_facade()

    # Get last date from DB for incremental sync
    last_date_query = "SELECT MAX(data_insercao) FROM nup"
    last_date_result = db.fetchone(last_date_query)
    last_date = None
    if last_date_result:
        last_value = last_date_result[0] if isinstance(last_date_result, (tuple, list)) else last_date_result
        if last_value:
            last_date = last_value.date() if isinstance(last_value, datetime) else datetime.fromisoformat(last_value).date()

    today = datetime.now().date()

    # Determine which dates to fetch
    if last_date is None:
        dates_to_fetch = [None]  # First run: fetch all
    else:
        dates_to_fetch = []
        current = last_date
        while current <= today:
            dates_to_fetch.append(current)
            current += timedelta(days=1)

    # Fetch data from the API
    all_data = []

    from airflow.models import Variable
    extra_headers = {
        "X-App-Code": Variable.get("BIOMASBR_APP_CODE"),
        "X-App-Name": Variable.get("BIOMASBR_APP_NAME"),
    }

    for fetch_date in dates_to_fetch:
        page = 0
        total_pages = None

        while True:
            params = {"page": page, "size": 100}
            if fetch_date:
                params["dataInsercao"] = fetch_date.strftime("%Y-%m-%d")

            response = utils.api_request(
                endpoint="solicitacoes",
                conn_id="BIOMAS_BR_API",
                params=params,
                extra_headers=extra_headers,
            )

            if not response or not response.get("content"):
                break

            all_data.extend(response["content"])

            if total_pages is None:
                total_pages = response.get("totalPages", 1)

            if page >= total_pages - 1:
                break

            page += 1

    if not all_data:
        return 0

    print(f"[DEBUG] Records from API: {len(all_data)}")

    # Insert data into database
    nup_insert_query = """
        INSERT INTO nup 
            (numero_solicitacao, descricao, codigo_car, nup_status_id, data_insercao, prodes, sync_date)
        VALUES 
            (%s, %s, %s, %s, %s, %s::uuid[], CURRENT_TIMESTAMP)
        ON CONFLICT (numero_solicitacao) 
        DO UPDATE SET
            descricao = EXCLUDED.descricao,
            codigo_car = EXCLUDED.codigo_car,
            nup_status_id = EXCLUDED.nup_status_id,
            data_insercao = EXCLUDED.data_insercao,
            prodes = EXCLUDED.prodes,
            sync_date = CURRENT_TIMESTAMP
        RETURNING id
    """

    prodes_insert_query = """
        INSERT INTO prodes 
            (fid, geom, state, path_row, main_class, class_name, def_cloud, julian_day, year, area_km, scene_id, source, satellite, sensor, uuid, image_date, sub_class, pub_date, nup_id)
        VALUES 
            (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4674), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid, %s, %s, %s, %s)
        ON CONFLICT (uuid) 
        DO UPDATE SET
            fid = EXCLUDED.fid,
            geom = EXCLUDED.geom,
            state = EXCLUDED.state,
            path_row = EXCLUDED.path_row,
            main_class = EXCLUDED.main_class,
            class_name = EXCLUDED.class_name,
            def_cloud = EXCLUDED.def_cloud,
            julian_day = EXCLUDED.julian_day,
            year = EXCLUDED.year,
            area_km = EXCLUDED.area_km,
            scene_id = EXCLUDED.scene_id,
            source = EXCLUDED.source,
            satellite = EXCLUDED.satellite,
            sensor = EXCLUDED.sensor,
            image_date = EXCLUDED.image_date,
            sub_class = EXCLUDED.sub_class,
            pub_date = EXCLUDED.pub_date,
            nup_id = EXCLUDED.nup_id
    """

    car_insert_query = """
        INSERT INTO car 
            (nup_id, cod_imovel, status_imo, dat_criaca, data_atual, area, condicao, uf, municipio, cod_munici, m_fiscal, tipo_imove, geom)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4674))
        ON CONFLICT (cod_imovel) 
        DO UPDATE SET
            nup_id = EXCLUDED.nup_id,
            status_imo = EXCLUDED.status_imo,
            dat_criaca = EXCLUDED.dat_criaca,
            data_atual = EXCLUDED.data_atual,
            area = EXCLUDED.area,
            condicao = EXCLUDED.condicao,
            uf = EXCLUDED.uf,
            municipio = EXCLUDED.municipio,
            cod_munici = EXCLUDED.cod_munici,
            m_fiscal = EXCLUDED.m_fiscal,
            tipo_imove = EXCLUDED.tipo_imove,
            geom = EXCLUDED.geom
    """

    import re
    import json
    uuid_regex = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)

    status_map = {
        "ATENDIDA": 1,
        "REJEITADA": 2,
        "PENDENTE": 3,
        "AGUARDANDO": 4,
        "EM ATENDIMENTO": 5,
    }

    total_inserted = 0
    cursor = db.conn.cursor()
    for item in all_data:
        uuids = [u for u in item.get("uuids", []) if uuid_regex.match(u)]
        status_str = item.get("status")
        status_int = status_map.get(status_str)
        codigo_car = item.get("codigoCar")

        if status_int is None:
            print(f"[DEBUG] Unknown status '{status_str}' for {item.get('numeroSolicitacao')}, skipping")
            continue

        # Insert into nup and get the id
        try:
            cursor.execute(nup_insert_query, (
                item.get("numeroSolicitacao"),
                item.get("descricao"),
                codigo_car,
                status_int,
                item.get("dataInsercao"),
                uuids
            ))
            nup_id = cursor.fetchone()[0]
            db.commit()
            total_inserted += 1
        except Exception as insert_error:
            print(f"[DEBUG] Failed to insert {item.get('numeroSolicitacao')}: {insert_error}")
            db.rollback()
            continue

        # Fetch PRODES data by each UUID
        for uuid in uuids:
            try:
                prodes_data = utils.api_request(
                    endpoint=f"prodes/{uuid}",
                    conn_id="SICAR_PRODES_API",
                )
            except Exception as e:
                print(f"[DEBUG] PRODES error for {item.get('numeroSolicitacao')} with UUID {uuid}: {e}")
                continue

            if not prodes_data or "properties" not in prodes_data:
                print(f"[DEBUG] PRODES no data for {item.get('numeroSolicitacao')} with UUID {uuid}")
                continue

            props = prodes_data["properties"]
            geom_geojson = json.dumps(prodes_data.get("geometry"))

            try:
                cursor.execute(prodes_insert_query, (
                    props.get("fid"),
                    geom_geojson,
                    props.get("state"),
                    props.get("path_row"),
                    props.get("main_class"),
                    props.get("class_name"),
                    props.get("def_cloud"),
                    props.get("julian_day"),
                    props.get("year"),
                    props.get("area_km"),
                    props.get("scene_id"),
                    props.get("source"),
                    props.get("satellite"),
                    props.get("sensor"),
                    props.get("uuid"),
                    props.get("image_date"),
                    props.get("sub_class"),
                    props.get("pub_date"),
                    nup_id,
                ))
                db.commit()
                print(f"[DEBUG] PRODES inserted for {item.get('numeroSolicitacao')}: {uuid}")
            except Exception as prodes_insert_error:
                print(f"[DEBUG] Failed to insert PRODES for UUID {uuid}: {prodes_insert_error}")
                db.rollback()


        # Fetch SICAR data by codigo_car
        if codigo_car:
            try:
                sicar_data = utils.api_request(
                    endpoint=f"sicar/{codigo_car}",
                    conn_id="SICAR_PRODES_API",
                )
            except Exception as e:
                print(f"[DEBUG] SICAR error for {item.get('numeroSolicitacao')} with codigo_car {codigo_car}: {e}")
                continue

            if not sicar_data or "properties" not in sicar_data:
                print(f"[DEBUG] SICAR no data for {item.get('numeroSolicitacao')} with codigo_car {codigo_car}")
                continue

            sicar_props = sicar_data["properties"]
            sicar_geom = json.dumps(sicar_data.get("geometry"))

            try:
                cursor.execute(car_insert_query, (
                    nup_id,
                    sicar_props.get("cod_imovel"),
                    sicar_props.get("status_imo"),
                    sicar_props.get("dat_criaca"),
                    sicar_props.get("data_atual"),
                    sicar_props.get("area"),
                    sicar_props.get("condicao"),
                    sicar_props.get("uf"),
                    sicar_props.get("municipio"),
                    sicar_props.get("cod_munici"),
                    sicar_props.get("m_fiscal"),
                    sicar_props.get("tipo_imove"),
                    sicar_geom,
                ))
                db.commit()
                print(f"[DEBUG] SICAR inserted for {item.get('numeroSolicitacao')}: {sicar_props.get('cod_imovel')}")
            except Exception as car_insert_error:
                print(f"[DEBUG] Failed to insert SICAR for {codigo_car}: {car_insert_error}")
                db.rollback()

    cursor.close()

    # Check DB count after insertion
    count_after = db.fetchone("SELECT COUNT(*) FROM nup")
    db_count = count_after[0] if isinstance(count_after, (tuple, list)) else count_after
    print(f"[DEBUG] DB count after insert: {db_count}")

    return total_inserted