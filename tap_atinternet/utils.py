from singer_sdk import typing as th  # JSON schema typing helpers
from typing import List


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
