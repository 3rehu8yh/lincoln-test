import pytest

from .pipeline_util import parse_date, format_date


def test_date() -> None:
    assert format_date(parse_date('12 January 2021')) == '2021-01-12'
    assert format_date(parse_date('12/01/2021')) == '2021-01-12'
    with pytest.raises(ValueError):
        parse_date('12-01-2021')
    with pytest.raises(ValueError):
        parse_date('__invalid__')
