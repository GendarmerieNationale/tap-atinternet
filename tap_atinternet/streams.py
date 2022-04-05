"""Stream type classes for tap-atinternet."""

from singer_sdk import typing as th  # JSON schema typing helpers
from tap_atinternet.client import ATInternetStream


class VisitsStream(ATInternetStream):
    """Define custom stream."""

    name = "visits"
    path = ""
    replication_key = "date"

    # ⚠️ don't forget to update the metrics and properties lists below if you change the schema
    schema = th.PropertiesList(
        # properties
        th.Property("date", th.DateType, required=True),
        th.Property("geo_city", th.StringType, required=True),
        th.Property("geo_country", th.StringType, required=True),
        th.Property("geo_region", th.StringType, required=True),
        th.Property("page_full_name", th.StringType, required=True),
        # metrics
        th.Property("m_visits", th.IntegerType, required=True),
    ).to_dict()

    # these will be used as the 'columns' parameter in the API request
    metrics = ["m_visits"]
    properties = ["date", "geo_city", "geo_country", "geo_region", "page_full_name"]

    # composite primary key
    primary_keys = metrics + properties
