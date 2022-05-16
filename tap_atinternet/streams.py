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


class HourlyVisitsStream(ATInternetStream):
    """
    Visits per hour and per day
    """

    # These will be used as the 'columns' parameter in the API request, alongside ATInternetStream.metrics.
    # It is also used to define the tap schema.
    metrics = SHARED_METRICS
    properties = th.PropertiesList(
        th.Property("date", th.DateType, required=True),
        th.Property("visit_hour", th.IntegerType, required=True),
    )

    name = "hourly_visits"
    path = ""
    # The replication_key is used in ATInternetStream.prepare_request_payload to only fetch and update recent data.
    # See https://www.stitchdata.com/docs/replication/replication-methods/key-based-incremental to learn more about
    # Key-based incremental replication
    replication_key = "date"
    schema = merge_properties_lists(metrics, properties).to_dict()
    # composite primary key
    primary_keys = property_list_to_str(metrics) + property_list_to_str(properties)


class GeoVisitsStream(ATInternetStream):
    """
    Visits per location (country, region, city) and per month
    """

    metrics = SHARED_METRICS
    properties = th.PropertiesList(
        th.Property("date_year", th.IntegerType, required=True),
        th.Property("date_month", th.StringType, required=True),
        th.Property("geo_country", th.StringType, required=True),
        th.Property("geo_region", th.StringType, required=True),
        th.Property("geo_city", th.StringType, required=True),
    )

    name = "geo_visits"
    path = ""
    replication_key = "year_month"
    schema = merge_properties_lists(metrics, properties).to_dict()
    # composite primary key
    primary_keys = property_list_to_str(metrics) + property_list_to_str(properties)
