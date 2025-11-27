from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List, Tuple

import logging
from utils import db as db_utils
from utils import api_client
from utils.date_utils import to_tz, fmt_yymmdd, fmt_hh
from utils.stations import get_eva_numbers


def _batch_id(prefix: str, dt: datetime) -> str:
    return f"{prefix}_{dt.strftime('%Y%m%d_%H%M%S')}"


def _collect_payloads(evas: List[str], fetch_fn, endpoint: str, batch_id: str) -> Tuple[List[Dict], int]:
    payloads: List[Dict] = []
    success = 0
    for eva in evas:
        try:
            raw_text, status_code, elapsed_ms = fetch_fn(eva)
            db_utils.log_api_call(
                source="deutsche_bahn",
                endpoint=endpoint,
                params={"eva": eva, "batch_id": batch_id},
                status_code=status_code,
                response_time_ms=elapsed_ms,
                result_count=1 if raw_text else 0,
                called_at=datetime.utcnow(),
            )
            # Stocker exactement le retour API (texte brut) en JSON string
            payloads.append(raw_text)
            success += 1 if 200 <= status_code < 300 else 0
        except Exception as exc:
            db_utils.log_api_call(
                source="deutsche_bahn",
                endpoint=endpoint,
                params={"eva": eva, "batch_id": batch_id},
                status_code=500,
                response_time_ms=0,
                result_count=0,
                called_at=datetime.utcnow(),
            )
            # On continue pour ne pas bloquer l’ingestion entière
            logging.getLogger(__name__).warning(f"API {endpoint} échouée pour EVA {eva}: {exc}")
    return payloads, success


def ingest_plan(logical_date: datetime) -> Dict[str, int]:
    """Ingestion pour l’endpoint PLAN (XML par date/heure).

    - Filtre les gares via util stations
    - Construit batch_id
    - Appelle l’API et journalise
    - Insère STG, copie PSA, purge STG
    """
    berlin_dt = to_tz(logical_date)
    yymmdd = fmt_yymmdd(berlin_dt)
    hh = fmt_hh(berlin_dt)

    batch_id = _batch_id("PLAN", datetime.utcnow())
    process_id = db_utils.log_process_start(process_name="db_timetables_plan_import", batch_id=batch_id)
    try:
        evas = get_eva_numbers()

        def _fetch(eva: str):
            return api_client.fetch_db_timetables_plan(eva_no=eva, date_yymmdd=yymmdd, hour_hh=hh)

        payloads, success = _collect_payloads(evas, _fetch, endpoint="plan", batch_id=batch_id)

        inserted = db_utils.insert_text(table="stg.timetables_plan_raw", texts=payloads, batch_id=batch_id)
        db_utils.copy_timetables_plan_stg_to_psa()
        db_utils.purge_stg_timetables_plan()
        db_utils.log_process_end(process_id=process_id, status="success", message=f"rows_inserted={inserted}")
        return {"rows_inserted": inserted, "api_success": success, "stations": len(evas)}
    except Exception as exc:
        db_utils.log_process_end(process_id=process_id, status="failed", message=str(exc))
        raise


def ingest_fchg() -> Dict[str, int]:
    """Ingestion pour l’endpoint FCHG (full changes)."""
    batch_id = _batch_id("FCHG", datetime.utcnow())
    process_id = db_utils.log_process_start(process_name="db_timetables_fchg_import", batch_id=batch_id)
    try:
        evas = get_eva_numbers()
        payloads, success = _collect_payloads(evas, api_client.fetch_db_timetables_fchg, endpoint="fchg", batch_id=batch_id)
        inserted = db_utils.insert_text(table="stg.timetables_fchg_raw", texts=payloads, batch_id=batch_id)
        db_utils.copy_timetables_fchg_stg_to_psa()
        db_utils.purge_stg_timetables_fchg()
        db_utils.log_process_end(process_id=process_id, status="success", message=f"rows_inserted={inserted}")
        return {"rows_inserted": inserted, "api_success": success, "stations": len(evas)}
    except Exception as exc:
        db_utils.log_process_end(process_id=process_id, status="failed", message=str(exc))
        raise


def ingest_rchg() -> Dict[str, int]:
    """Ingestion pour l’endpoint RCHG (recent changes)."""
    batch_id = _batch_id("RCHG", datetime.utcnow())
    process_id = db_utils.log_process_start(process_name="db_timetables_rchg_import", batch_id=batch_id)
    try:
        evas = get_eva_numbers()
        payloads, success = _collect_payloads(evas, api_client.fetch_db_timetables_rchg, endpoint="rchg", batch_id=batch_id)
        inserted = db_utils.insert_text(table="stg.timetables_rchg_raw", texts=payloads, batch_id=batch_id)
        db_utils.copy_timetables_rchg_stg_to_psa()
        db_utils.purge_stg_timetables_rchg()
        db_utils.log_process_end(process_id=process_id, status="success", message=f"rows_inserted={inserted}")
        return {"rows_inserted": inserted, "api_success": success, "stations": len(evas)}
    except Exception as exc:
        db_utils.log_process_end(process_id=process_id, status="failed", message=str(exc))
        raise


def task_ingest_plan(**context):
    logical_date: datetime = context["logical_date"]
    return ingest_plan(logical_date)


def task_ingest_fchg(**_context):
    return ingest_fchg()


def task_ingest_rchg(**_context):
    return ingest_rchg()