import datetime

from tap_atinternet.utils import get_next_month, get_start_end_days


def test_get_next_month():
    assert get_next_month(2020, 7) == (2020, 8)
    assert get_next_month(2020, 12) == (2021, 1)
    assert get_next_month(2021, 1) == (2021, 2)


def test_get_start_end_days():
    assert get_start_end_days(2020, 6) == (
        datetime.date(2020, 6, 1),
        datetime.date(2020, 6, 30),
    )
    assert get_start_end_days(2020, 12) == (
        datetime.date(2020, 12, 1),
        datetime.date(2020, 12, 31),
    )
    assert get_start_end_days(2021, 1) == (
        datetime.date(2021, 1, 1),
        datetime.date(2021, 1, 31),
    )
