"""REST client handling, including ATInternetStream base class."""
import datetime
import json
import logging
from typing import Any, Dict, Optional

import requests
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams import RESTStream

from tap_atinternet.utils import (
    property_list_to_str,
    get_start_end_days,
    get_next_month,
    month_str_to_int,
)


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

    def date_to_str(self, date) -> str:
        return datetime.datetime.strftime(date, self.date_format)

    def str_to_date(self, date) -> datetime.datetime:
        return datetime.datetime.strptime(date, self.date_format)

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
        Return a token for identifying next (year,month) tuple and next page.

        If the previous (year,month) and page is still returning data, try using the same (year,month) and the next page.
        If there is no more data (-> no more pages) for the previous (year,month), try using the next (year,month).
        If the next (year,month) is greater or equal than today, it means there is no more data to sync
            -> return None to stop the loop
        """
        # Get the date of the previous request
        request_body_json = json.loads(response.request.body)
        previous_start_date = self.str_to_date(
            request_body_json["period"]["p1"][0]["start"]
        )

        # Find out if the previous month is still returning data (which means it may have pages left)
        data_feed = response.json()["DataFeed"]
        rows = data_feed["Rows"]
        if len(rows) > 0:
            # get the page-num of the previous request
            previous_page = request_body_json["page-num"]
            return {
                "year_month": (previous_start_date.year, previous_start_date.month),
                "page_num": previous_page + 1,
            }

        # Find out if we should use the next month
        next_year, next_month = get_next_month(
            previous_start_date.year, previous_start_date.month
        )
        if datetime.date(next_year, next_month, 1) <= datetime.date.today():
            logging.info(f"Syncing data for month: {next_month}/{next_year}")
            return {
                "year_month": (next_year, next_month),
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
            start_date = self.str_to_date(
                self.get_starting_replication_key_value(context)
            )
            _, end_date = get_start_end_days(start_date.year, start_date.month)
            page_num = 1
        else:
            year, month = next_page_token["year_month"]
            start_date, end_date = get_start_end_days(year, month)
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
            "period": {
                "p1": [
                    {
                        "type": "D",
                        "start": self.date_to_str(start_date),
                        "end": self.date_to_str(end_date),
                    }
                ]
            },
            "filter": filter_dict,
            # ATInternet requires you to sort by something...
            "sort": ["-m_visits"],
            "max-results": self.config.get("max_results"),
            "page-num": page_num,
        }

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if "visit_hour" in row and row["visit_hour"] == "N/A":
            # "N/A" does not work when casting to integers, and we don't want null values either since it will
            # be used in the composite primary key -> use -1 instead
            row["visit_hour"] = -1
        if "date" not in row:
            # for some streams, we request monthly data from the API, but we still want a 'date' column in our data
            # -> use the first day of the month
            year = row["date_year"]
            month = month_str_to_int[row["date_month"]]
            row["date"] = self.date_to_str(datetime.date(year, month, 1))
        return row

    def validate_response(self, response: requests.Response) -> None:
        """
        Override the parent method to give more details on 400 errors, by logging the request body.
        """
        if 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}. "
                f"Request payload: {response.request.body}"
            )
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)
