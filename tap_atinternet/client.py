"""REST client handling, including ATInternetStream base class."""
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.streams import RESTStream

from tap_atinternet.utils import property_list_to_str


class ATInternetStream(RESTStream):
    """
    ATInternet stream class.

    API documentation:
     - https://developers.atinternet-solutions.com/data-api-en/reporting-api-v3/getting-started/how-does-it-work/
     - https://management.atinternet-solutions.com/#/data-model/properties (metrics and properties doc)
    """

    # --- AT Internet specific attributes and methods
    # To be replaced by the child Stream class
    metrics: th.PropertiesList
    properties: th.PropertiesList

    # AT Internet date format utilities
    date_format = "%Y-%m-%d"

    def date_to_str(self, date):
        return datetime.strftime(date, self.date_format)

    def str_to_date(self, date):
        return datetime.strptime(date, self.date_format)

    # --- Singer SDK attributes and methods
    url_base = "https://api.atinternet.io/v3/data/getData"
    records_jsonpath = "$.DataFeed.Rows[*]"
    rest_method = "POST"

    @property
    def http_headers(self) -> dict:
        """
        Return the http headers with authentication and content type.
        """
        return {
            "x-api-key": f"{self.config.get('api_key')}_{self.config.get('secret_key')}",
            "Content-type": "application/json",
        }

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[dict]
    ) -> Optional[dict]:
        """
        Return a token for identifying next day and next page.

        If the previous day and page is still returning data, try using the same day and the next page.
        If there is no more data (-> no more pages) for the previous day, try using the next day.
        If the next day is greater or equal than today, it means there is no more data to sync
            -> return None to stop the loop
        """
        # Get the date of the previous request
        request_body_json = json.loads(response.request.body)
        previous_day = request_body_json["period"]["p1"][0]["start"]
        assert (
            previous_day == request_body_json["period"]["p1"][0]["end"]
        ), "AT Internet API requests should have the same start and end date"
        if previous_token:
            assert previous_day == previous_token["day"]

        # Find out if the previous day is still returning data (which means it may have pages left)
        data_feed = response.json()["DataFeed"]
        rows = data_feed["Rows"]
        if len(rows) > 0:
            # get the page-num of the previous request
            previous_page = request_body_json["page-num"]
            return {
                "day": previous_day,
                "page_num": previous_page + 1,
            }

        # Find out if we should use the next day
        previous_day = self.str_to_date(previous_day)
        next_day = previous_day + timedelta(days=1)
        if next_day < datetime.now():
            # Note: we choose not to sync today's data for now (only up to yesterday, to have full days)
            next_day = self.date_to_str(next_day)
            logging.info(f"Syncing date: {next_day}")
            return {
                "day": next_day,
                "page_num": 1,
            }
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[dict]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        # empty dictionary, since we pass all the parameters in the POST request payload
        return {}

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[dict]
    ) -> Optional[dict]:
        """
        Prepare the data payload for the REST API request.

        Note: Due to AT Internet API limitations (max 200000 total rows per API call,
        regardless of pagination [1]), we choose to split the full timespan (from start_date to today)
        into separate requests.
        Each request is querying data for a single day (same 'start' and 'end' period parameter),
        and we iterate over both:
            - the day (from the starting timestamp to today)
            - the page (even for a single day, API responses typically return multiple pages)

        The iteration is done in the `get_next_page_token` method.

        [1]: https://developers.atinternet-solutions.com/data-api-en/reporting-api-v3/getting-started/how-does-it-work
        """
        # on first request: use last state or 'start_date' as the day, and fetch page 1
        # Note: this leads to duplicates in incremental mode (https://gitlab.com/meltano/meltano/-/issues/2504)
        if next_page_token is None:
            # `get_starting_replication_key_value()` returns the stream replication key
            # (useful for incremental sync) or, if no state was passed, returns the 'start_date' in config
            day = self.get_starting_replication_key_value(context)
            page_num = 1
        else:
            day = next_page_token["day"]
            page_num = next_page_token["page_num"]

        filter_dict = {}
        if self.config.get("filter_str"):
            # $lk stands for 'contains'
            filter_dict = {
                "property": {"page_full_name": {"$lk": self.config.get("filter_str")}}
            }

        return {
            "space": {"s": [self.config.get("site_id")]},
            "columns": property_list_to_str(self.metrics)
            + property_list_to_str(self.properties),
            "period": {"p1": [{"type": "D", "start": day, "end": day}]},
            "filter": filter_dict,
            # ATInternet requires you to sort by something...
            "sort": ["-m_visits"],
            "max-results": self.config.get("max_results"),
            "page-num": page_num,
        }
