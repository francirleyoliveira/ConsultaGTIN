from __future__ import annotations

from datetime import datetime
from typing import Any

_DATE_FORMATS = (
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().casefold()


def apply_column_filters(records: list[dict[str, Any]], filters: dict[str, str]) -> list[dict[str, Any]]:
    active_filters = {
        key: str(value).strip().casefold()
        for key, value in filters.items()
        if str(value).strip()
    }
    if not active_filters:
        return list(records)

    filtered: list[dict[str, Any]] = []
    for record in records:
        if all(value in _normalize_text(record.get(key, "")) for key, value in active_filters.items()):
            filtered.append(record)
    return filtered


def _parse_datetime(value: str) -> datetime | None:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _coerce_sort_value(value: Any) -> tuple[int, Any]:
    if value is None:
        return (3, "")
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, int):
        return (0, value)
    if isinstance(value, float):
        return (0, value)

    text = str(value).strip()
    if not text:
        return (3, "")

    parsed_dt = _parse_datetime(text)
    if parsed_dt is not None:
        return (1, parsed_dt.timestamp())

    if text.isdigit():
        return (0, int(text))

    decimal_candidate = text.replace('.', '').replace(',', '.', 1)
    if decimal_candidate.replace('-', '', 1).replace('.', '', 1).isdigit():
        try:
            return (0, float(text.replace('.', '').replace(',', '.')))
        except ValueError:
            pass

    return (2, text.casefold())


def sort_records(records: list[dict[str, Any]], column: str, reverse: bool = False) -> list[dict[str, Any]]:
    if not column:
        return list(records)

    populated = [record for record in records if str(record.get(column, "")).strip()]
    empty = [record for record in records if not str(record.get(column, "")).strip()]
    ordered = sorted(populated, key=lambda record: _coerce_sort_value(record.get(column)), reverse=reverse)
    return ordered + empty
