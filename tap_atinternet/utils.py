import datetime

from singer_sdk import typing as th  # JSON schema typing helpers
from typing import List, Tuple, Optional


def property_list_to_str(properties: th.PropertiesList) -> List[str]:
    """
    Transform a PropertiesList into a list of names

    Example:
        th.PropertiesList(
            th.Property("date", th.DateType, required=True),
            th.Property("visit_hour", th.StringType, required=True),
        )
        -> ["date", "visit_hour"]
    """
    return [name for (name, prop) in properties.items()]


def merge_properties_lists(*properties_lists: th.PropertiesList) -> th.PropertiesList:
    """
    Merge multiple PropertiesList objects into a single one
    """
    result = th.PropertiesList()
    for properties_list in properties_lists:
        for name, prop in properties_list.items():
            result.append(prop)
    return result


def get_next_month(year: int, month: int) -> Tuple[int, int]:
    # first day of next month
    next_month = datetime.date(year + month // 12, month % 12 + 1, 1)
    return next_month.year, next_month.month


def get_start_end_days(
    year: int, month: int, min_start_date: Optional[datetime.date] = None
) -> Tuple[datetime.date, datetime.date]:
    # first day of current month
    start_date = datetime.date(year, month, 1)
    if min_start_date is not None:
        # if a minimum start date is specified, make sure we don't go before that
        start_date = max(start_date, min_start_date)

    # last day of current month
    next_year, next_month = get_next_month(year, month)
    end_date = datetime.date(next_year, next_month, 1) - datetime.timedelta(days=1)
    # AT Internet does not accept future dates in the filter range
    end_date = min(end_date, datetime.date.today())

    assert start_date <= end_date, (
        f"Could not compute correct start/end dates {start_date} / {end_date}"
        f" with year={year}, month={month}, min_start_date={min_start_date}"
    )
    return start_date, end_date


# also possible with datetime.strptime, but requires changing the locale..
month_str_to_int = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}
