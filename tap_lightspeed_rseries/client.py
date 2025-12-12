"""REST client handling, including LightspeedRSeriesStream base class."""

from typing import Any, Dict, Optional
import requests
import copy
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from cached_property import cached_property
from tap_lightspeed_rseries.auth import LightspeedOAuthAuthenticator
import singer
from singer import StateMessage


class LightspeedRSeriesStream(RESTStream):
    """Lightspeed Retail (R-Series) stream class."""
    
    page_size = 100
    timeout = 300
    records_jsonpath = "$[*]"

    @cached_property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.lightspeedapp.com/API/V3"

    @property
    def authenticator(self) -> LightspeedOAuthAuthenticator:
        """Return a new authenticator object."""
        return LightspeedOAuthAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        try:
            response_data = response.json()
            attributes = response_data.get("@attributes", {})
            next_url = attributes.get("next", "")
            if not next_url:
                return None
            return next_url
        except Exception:
            return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if next_page_token and isinstance(next_page_token, str) and next_page_token.startswith("http"):
            return {}
        
        params: dict = {}
        if self.page_size:
            params["limit"] = min(self.page_size, 100)
        return params

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request for the API."""
        if next_page_token and isinstance(next_page_token, str) and next_page_token.startswith("http"):
            from requests import Request
            prepared = super().prepare_request(context, None)
            req = Request(
                method=prepared.method,
                url=next_page_token,
                headers=prepared.headers,
            )
            return self.requests_session.prepare_request(req)
        return super().prepare_request(context, next_page_token)

    def make_request(self, context, next_page_token):
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        resp = self._request(prepared_request, context)
        return resp
    
    def request_records(self, context: Optional[dict]):
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self.make_request)

        while not finished:
            resp = decorated_request(context, next_page_token)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            finished = not next_page_token

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in [401]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)
        elif response.status_code == 400 and "Please try again later." in response.text:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise FatalAPIError(msg)
        elif 500 <= response.status_code < 600 or response.status_code in [401]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name] = {"partitions": []}

        singer.write_message(StateMessage(value=tap_state))
