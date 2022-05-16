"""Stream type classes for tap-atinternet."""

from singer_sdk import typing as th  # JSON schema typing helpers
from tap_atinternet.client import ATInternetStream
from tap_atinternet.utils import merge_properties_lists, property_list_to_str

SHARED_METRICS = th.PropertiesList(
    th.Property("m_visits", th.IntegerType, required=True),
    th.Property("m_unique_visitors", th.IntegerType, required=True),
    th.Property("m_bounces", th.IntegerType, required=True),
    th.Property("m_time_spent_per_visits", th.NumberType, required=True),
)


class VisitsStream(ATInternetStream):
    """Define custom stream."""

    # These will be used as the 'columns' parameter in the API request, alongside ATInternetStream.metrics.
    # It is also used to define the tap schema.
    metrics = SHARED_METRICS
    properties = th.PropertiesList(
        th.Property("date", th.DateType, required=True),
        th.Property("visit_hour", th.StringType, required=True),
        th.Property("geo_country", th.StringType, required=True),
        th.Property("geo_region", th.StringType, required=True),
        th.Property("page_full_name", th.StringType, required=True),
    )

    name = "visits"
    path = ""
    replication_key = "date"
    schema = merge_properties_lists(metrics, properties).to_dict()
    primary_keys = property_list_to_str(metrics) + property_list_to_str(
        properties
    )  # composite primary key
