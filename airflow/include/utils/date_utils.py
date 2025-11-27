from __future__ import annotations

from datetime import datetime
import pendulum


TZ_EUROPE_BERLIN = "Europe/Berlin"


def to_tz(dt: datetime, tz_name: str = TZ_EUROPE_BERLIN) -> datetime:
    """Convertit un datetime vers un fuseau donné en conservant l’instant.

    Utilise pendulum pour une manipulation fiable des fuseaux.
    """
    tz = pendulum.timezone(tz_name)
    return dt.in_timezone(tz) if hasattr(dt, "in_timezone") else tz.convert(dt)


def fmt_yymmdd(dt: datetime) -> str:
    """Formate en `YYMMDD` (attendu par l’endpoint plan)."""
    return dt.strftime("%y%m%d")


def fmt_hh(dt: datetime) -> str:
    """Formate l’heure en `HH` (attendu par l’endpoint plan)."""
    return dt.strftime("%H")