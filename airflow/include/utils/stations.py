from __future__ import annotations

import os
from typing import List, Optional
from utils import db as db_utils


def get_eva_numbers(
    name_prefix: Optional[str] = None,
    ds100_prefix: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[str]:
    """Retourne la liste des EVA en filtrant `dwh.v_stations`.

    Les filtres sont configurables via variables d’environnement:
    - DB_TIMETABLES_FILTER_NAME_PREFIX (par défaut 'Brem%')
    - DB_TIMETABLES_FILTER_DS100_PREFIX (par défaut 'HB%')
    - DB_TIMETABLES_STATIONS_LIMIT (optionnel)
    """
    name_prefix = name_prefix or os.getenv("DB_TIMETABLES_FILTER_NAME_PREFIX", "Brem%")
    ds100_prefix = ds100_prefix or os.getenv("DB_TIMETABLES_FILTER_DS100_PREFIX", "HB%")
    if limit is None:
        env_limit = os.getenv("DB_TIMETABLES_STATIONS_LIMIT")
        limit = int(env_limit) if env_limit else None

    sql = (
        """
        SELECT DISTINCT eva_number
        FROM dwh.v_stations
        WHERE name LIKE %(name_prefix)s
          AND REPLACE(ds100, '"', '') LIKE %(ds_prefix)s
        """
        + (" LIMIT %(limit)s" if limit else "")
    )

    params = {"name_prefix": name_prefix, "ds_prefix": ds100_prefix}
    if limit:
        params["limit"] = limit

    with db_utils.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [row[0] for row in rows]