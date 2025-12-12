"""Lightspeed OAuth2 Authentication."""

from singer import utils
import json
import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams import Stream as RESTStreamBase
from typing import Optional


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class LightspeedOAuthAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Lightspeed Retail (R-Series) API using OAuth 2.0."""

    def __init__(
        self,
        stream: RESTStreamBase,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None
    ) -> None:
        super().__init__(stream=stream, auth_endpoint=auth_endpoint, oauth_scopes=oauth_scopes)
        self._tap = stream._tap

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Lightspeed R-Series API."""
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
        }

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.expires_in is not None:
            self.expires_in = int(self.expires_in)
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        if self.expires_in > (utils.now() - self.last_refreshed).total_seconds():
            return True
        return False

    @classmethod
    def create_for_stream(cls, stream) -> "LightspeedOAuthAuthenticator":
        return cls(
            stream=stream,
            auth_endpoint="https://cloud.lightspeedapp.com/auth/oauth/token",
        )

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()
        auth_request_payload = self.oauth_request_body
        token_response = requests.post(
            self.auth_endpoint,
            data=auth_request_payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        
        # Handle rate limiting (429)
        if token_response.status_code == 429:
            retry_after = token_response.headers.get("Retry-After")
            if retry_after:
                from email.utils import parsedate_to_datetime
                try:
                    retry_datetime = parsedate_to_datetime(retry_after)
                    wait_seconds = (retry_datetime - utc_now()).total_seconds()
                    if wait_seconds > 0:
                        import time
                        time.sleep(wait_seconds)
                        token_response = requests.post(
                            self.auth_endpoint,
                            data=auth_request_payload,
                            headers={"Content-Type": "application/x-www-form-urlencoded"}
                        )
                except Exception:
                    import time
                    time.sleep(60)
                    token_response = requests.post(
                        self.auth_endpoint,
                        data=auth_request_payload,
                        headers={"Content-Type": "application/x-www-form-urlencoded"}
                    )
            else:
                import time
                time.sleep(60)
                token_response = requests.post(
                    self.auth_endpoint,
                    data=auth_request_payload,
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                )
        
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json.get("expires_in", 3600)
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in received in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time

        # store access_token in config file
        self._tap._config["access_token"] = token_json["access_token"]
        if "refresh_token" in token_json:
            self._tap._config["refresh_token"] = token_json["refresh_token"]
        
        # Store expires timestamp
        if self.expires_in:
            expires_timestamp = int(request_time.timestamp()) + int(self.expires_in)
            self._tap._config["expires"] = expires_timestamp
        self._tap._config["expires_in"] = self.expires_in

        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4)

