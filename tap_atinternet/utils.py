import datetime

from singer_sdk import typing as th  # JSON schema typing helpers
from typing import List, Tuple


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
    next_month = datetime.date(year + month // 1, month % 12 + 1, 1)
    return next_month.year, next_month.month


def get_start_end_days(year: int, month: int) -> Tuple[datetime.date, datetime.date]:
    # first day of current month
    start_date = datetime.date(year, month, 1)
    next_year, next_month = get_next_month(year, month)

    # last day of current month
    end_date = datetime.date(next_year, next_month, 1) - datetime.timedelta(days=1)
    end_date = min(end_date, datetime.date.today())  # AT Internet does not accept future dates in the filter range
    return start_date, end_date
