"""
Util functions for writing the data pipeline.
"""

import datetime
from typing import Optional, NewType, cast

#: ISO8601 date with separator, i.e., "YYYY-MM-DD".
ISO8601Date = NewType('ISO8601Date', str)


def parse_date(date: str) -> datetime.date:
    """
    Parses a date.  Multiple formats are tested.
    """
    dt = _maybe_parse_date(date, '%d/%m/%Y')
    if dt is not None:
        return dt

    dt = _maybe_parse_date(date, '%Y-%m-%d')
    if dt is not None:
        return dt

    dt = _maybe_parse_date(date, '%d %B %Y')
    if dt is not None:
        return dt

    raise ValueError(f'cannot parse date "{date}"')


def _maybe_parse_date(date: str, format: str) -> Optional[datetime.date]:
    """
    Parses a date and returns `None` (instead of raising an error) if
    a date could not be parsed.
    """
    try:
        return datetime.datetime.strptime(date, format)
    except ValueError as e:
        return None


def format_date(date: datetime.date) -> ISO8601Date:
    """
    Formats a date using the ISO8601 format.
    """
    return cast(ISO8601Date, date.strftime('%Y-%m-%d'))
