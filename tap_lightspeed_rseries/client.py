"""REST client handling, including LightspeedRSeriesStream base class."""

from typing import Any, Dict, Optional
from pytz import timezone
import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError as RequestsConnectionError
from urllib3.exceptions import ProtocolError, ReadTimeoutError
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
    _replication_key_logged = False  # Flag to track if replication key info was already logged

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

    def get_starting_time(self, context: Optional[dict]):
        """Get starting time for incremental sync.
        
        Returns the replication_key value from state if available,
        otherwise falls back to start_date from config.
        """
        # Get replication_key value from state (last synced value) first
        rep_key = self.get_starting_timestamp(context)
        
        # If no replication_key in state, fall back to start_date from config
        if not rep_key:
            start_date = self.config.get("start_date")
            if start_date:
                try:
                    from pendulum import parse
                    rep_key = parse(start_date)
                except Exception:
                    self.logger.warning(f"Failed to parse start_date: {start_date}")
                    rep_key = None
        
        return rep_key

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if next_page_token and isinstance(next_page_token, str) and next_page_token.startswith("http"):
            return {}
        
        params: dict = {}
        if self.page_size:
            params["limit"] = min(self.page_size, 100)
        
        # Handle incremental sync with replication_key
        # Lightspeed R-Series API supports timeStamp filtering with query operators
        # Format: timeStamp=%3E%3D,YYYY-MM-DDTHH:MM:SS-00:00
        # See: https://developers.lightspeedhq.com/retail/introduction/parameters/
        if self.replication_key:
            starting_time = self.get_starting_time(context)
            if starting_time:
                # Convert to UTC timezone if needed
                if hasattr(starting_time, 'astimezone'):
                    starting_time_utc = starting_time.astimezone(timezone('UTC'))
                else:
                    # If it's already a timezone-aware datetime in UTC
                    starting_time_utc = starting_time
                
                # Format according to Lightspeed API: YYYY-MM-DDTHH:MM:SS-00:00
                time_stamp_str = starting_time_utc.strftime("%Y-%m-%dT%H:%M:%S-00:00")
                
                # Use query operator >= with comma separator
                # Format: timeStamp=%3E%3D,YYYY-MM-DDTHH:MM:SS-00:00
                # The requests library will URL-encode this, encoding >= to %3E%3D
                # and the comma to %2C. If the API requires unencoded comma, we may
                # need to override prepare_request, but let's test this first.
                params["timeStamp"] = f">=,{time_stamp_str}"
                
                # Log replication key info only once per stream
                if not self._replication_key_logged:
                    self.logger.info(
                        f"Incremental sync: filtering records with {self.replication_key} >= {starting_time_utc} "
                        f"(using API filter: timeStamp={params['timeStamp']})"
                    )
                    self._replication_key_logged = True
            else:
                # Log full sync message only once per stream
                if not self._replication_key_logged:
                    self.logger.info(
                        f"Full sync: no previous {self.replication_key} value found in state and no start_date configured"
                    )
                    self._replication_key_logged = True
        
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
        """Make request with error handling for connection issues."""
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        try:
            resp = self._request(prepared_request, context)
            return resp
        except (ChunkedEncodingError, ProtocolError, RequestsConnectionError, ReadTimeoutError) as e:
            # These are connection/network errors that should be retried
            url = getattr(prepared_request, 'url', self.path or 'unknown endpoint')
            error_msg = (
                f"Connection error while requesting {url}: {str(e)}. "
                f"This is likely a temporary network issue or server-side problem. "
                f"The request will be retried."
            )
            self.logger.warning(error_msg)
            raise RetriableAPIError(error_msg) from e
    
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
            # Token expired, force refresh by invalidating current token
            self.logger.info("Received 401 Unauthorized, token may have expired. Refreshing token...")
            if hasattr(self.authenticator, 'access_token'):
                # Invalidate token to force refresh on next request
                self.authenticator.access_token = None
                self.authenticator.last_refreshed = None
                # Force refresh
                self.authenticator.update_access_token()
                self.logger.info("Token refreshed after 401 error, request will be retried")
            
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
        """Write out a STATE message with the latest state.
        
        Note: We don't clear partitions here as the Singer SDK automatically
        manages state for partitioned streams (streams with contexts).
        Clearing partitions would prevent proper incremental sync for child streams.
        """
        tap_state = self.tap_state
        singer.write_message(StateMessage(value=tap_state))
