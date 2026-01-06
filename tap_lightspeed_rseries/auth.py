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
        
        # Try to load existing token from config if available
        if "access_token" in self.config:
            self.access_token = self.config["access_token"]

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
        
        Returns True if we have a token, allowing the API to decide if it's valid.
        If the API returns 401, the token will be refreshed automatically.

        Returns:
            True if we have an access_token (let API validate it).
        """
        # If we have an access_token, assume it's valid and let the API decide
        # If API returns 401, the refresh will happen automatically
        if not hasattr(self, 'access_token') or not self.access_token:
            # Try to load from config
            if "access_token" in self.config:
                self.access_token = self.config["access_token"]
                self.logger.debug("Loaded access_token from config for validation check")
                return True
            else:
                self.logger.debug("No access_token found in config")
                return False
        
        # We have a token, assume it's valid - API will tell us if it's not
        return True

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
        self.logger.info("Requesting new token from OAuth endpoint...")
        request_time = utc_now()
        
        # CRITICAL: Read refresh_token directly from _tap._config at the last moment
        # to avoid race conditions where another refresh might have updated it
        # This ensures we always use the most recent refresh_token
        current_refresh_token = self._tap._config.get("refresh_token")
        if not current_refresh_token:
            raise RuntimeError("No refresh_token found in config. Cannot refresh access token.")
        
        # Build the request payload using the most current refresh_token
        auth_request_payload = {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "refresh_token",
            "refresh_token": current_refresh_token,
        }
        
        # Log the refresh_token being used BEFORE making the request
        # This is critical for debugging when the refresh fails
        self.logger.info("=" * 80)
        self.logger.info("ATTEMPTING TOKEN REFRESH - Using refresh_token:")
        self.logger.info(f"refresh_token: {current_refresh_token}")
        self.logger.info("=" * 80)
        
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
            # Log error details including the refresh_token that was used
            error_response = {}
            try:
                error_response = token_response.json()
            except:
                error_response = {"error": "Could not parse error response", "text": token_response.text[:200]}
            
            refresh_token_used = auth_request_payload.get("refresh_token", "N/A")
            self.logger.error("=" * 80)
            self.logger.error("TOKEN REFRESH FAILED:")
            self.logger.error(f"refresh_token used: {refresh_token_used}")
            self.logger.error(f"Error response: {error_response}")
            self.logger.error(f"HTTP Status: {token_response.status_code}")
            self.logger.error("=" * 80)
            
            raise RuntimeError(
                f"Failed OAuth login, response was '{error_response}'. {ex}"
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

        # Log new token information - showing full tokens for debugging
        # This is important for troubleshooting when pods die
        # Get refresh_token (new from response or existing from config)
        refresh_token_to_log = token_json.get("refresh_token") or self.config.get("refresh_token", "N/A")
        
        self.logger.info("=" * 80)
        self.logger.info("NEW TOKEN RECEIVED - Full token details:")
        self.logger.info(f"access_token: {token_json['access_token']}")
        self.logger.info(f"refresh_token: {refresh_token_to_log}")
        self.logger.info(f"expires_in: {self.expires_in} seconds")
        self.logger.info("=" * 80)

        # store access_token in config file
        self._tap._config["access_token"] = token_json["access_token"]
        refresh_token_updated = False
        if "refresh_token" in token_json:
            self._tap._config["refresh_token"] = token_json["refresh_token"]
            refresh_token_updated = True
        else:
            self.logger.debug("No refresh_token in response, keeping existing one")
        
        # Store expires timestamp
        if self.expires_in:
            expires_timestamp = int(request_time.timestamp()) + int(self.expires_in)
            self._tap._config["expires"] = expires_timestamp
            from datetime import datetime
            expires_datetime = datetime.fromtimestamp(expires_timestamp)
            self.logger.info(f"Token expires at: {expires_datetime.isoformat()}")
        self._tap._config["expires_in"] = self.expires_in

        # Save config to file
        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4)
        
        self.logger.info(
            f"Tokens saved to config file: {self._tap.config_file}. "
            f"Access token: updated, "
            f"Refresh token: {'updated' if refresh_token_updated else 'unchanged'}"
        )

