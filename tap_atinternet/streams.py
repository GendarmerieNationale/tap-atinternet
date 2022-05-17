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
        # ⚠️ visit_hour can be "N/A" in some cases, this will be replaced by -1 in ATInternetStream.post_process
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
    primary_keys = property_list_to_str(properties)


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
    replication_key = "date"
    schema = merge_properties_lists(
        metrics,
        properties,
        # we don't want to use the date in AT Internet API requests, but we still want to (manually) save it in
        # the records, to allow for a "date" replication key
        th.PropertiesList(th.Property("date", th.DateType, required=True)),
    ).to_dict()
    primary_keys = property_list_to_str(properties)


class PagesVisitsStream(ATInternetStream):
    """
    Visits per page and per month
    """

    metrics = SHARED_METRICS
    properties = th.PropertiesList(
        th.Property("date_year", th.IntegerType, required=True),
        th.Property("date_month", th.StringType, required=True),
        th.Property("page", th.StringType, required=True),
        th.Property("page_full_name", th.StringType, required=True),
    )

    name = "pages_visits"
    path = ""
    replication_key = "date"
    schema = merge_properties_lists(
        metrics,
        properties,
        th.PropertiesList(th.Property("date", th.DateType, required=True)),
    ).to_dict()
    primary_keys = property_list_to_str(properties)


class SourcesVisitsStream(ATInternetStream):
    """
    Visits per traffic source (social network, search engine, etc) and per month
    """

    metrics = SHARED_METRICS
    properties = th.PropertiesList(
        th.Property("date_year", th.IntegerType, required=True),
        th.Property("date_month", th.StringType, required=True),
        th.Property("src", th.StringType, required=True),
        th.Property("src_detail", th.StringType, required=True),
        th.Property("src_referrer_url", th.StringType, required=True),
    )

    name = "sources_visits"
    path = ""
    replication_key = "date"
    schema = merge_properties_lists(
        metrics,
        properties,
        th.PropertiesList(th.Property("date", th.DateType, required=True)),
    ).to_dict()
    primary_keys = property_list_to_str(properties)


class DevicesVisitsStream(ATInternetStream):
    """
    Visits per device (OS, mobile or desktop, language, etc) and per month
    """

    metrics = SHARED_METRICS
    properties = th.PropertiesList(
        th.Property("date_year", th.IntegerType, required=True),
        th.Property("date_month", th.StringType, required=True),
        th.Property("device_type", th.StringType, required=True),
        th.Property("os_group", th.StringType, required=True),
        th.Property("browser_group", th.StringType, required=True),
        th.Property("browser_language", th.StringType, required=True),
    )

    name = "devices_visits"
    path = ""
    replication_key = "date"
    schema = merge_properties_lists(
        metrics,
        properties,
        th.PropertiesList(th.Property("date", th.DateType, required=True)),
    ).to_dict()
    primary_keys = property_list_to_str(properties)
