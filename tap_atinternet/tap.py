"""ATInternet tap class."""

from typing import List

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_atinternet.client import ATInternetStream
from tap_atinternet.streams import HourlyVisitsStream


class TapATInternet(Tap):
    """ATInternet tap class."""

    name = "tap-atinternet"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key", th.StringType, required=True, description="AT Internet API key"
        ),
        th.Property(
            "secret_key",
            th.StringType,
            required=True,
            description="AT Internet secret key",
        ),
        th.Property(
            "site_id",
            th.IntegerType,
            required=True,
            description="Site ID (can be queried at https://dataquery.atinternet-solutions.com/)",
        ),
        th.Property(
            "start_date",
            th.DateType,
            required=True,
            description="Start syncing data from that date",
        ),
        th.Property(
            "max_results",
            th.IntegerType,
            default=5000,
            description="Max number of results per page (up to 10000)",
        ),
        th.Property(
            "filter_str",
            th.StringType,
            default="",
            description="Optional. If not empty, filter and extract only the pages with "
                        "this string in the 'page_full_name'",
        ),
    ).to_dict()

    def discover_streams(self) -> List[ATInternetStream]:
        """Return a list of discovered streams."""
        return [HourlyVisitsStream(tap=self)]
