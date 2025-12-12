"""Lightspeed tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_lightspeed_rseries.streams import (
    AccountStream,
    ItemStream,
    VendorStream,
    OrderStream,
    SaleStream,
    ShipmentStream,
)

STREAM_TYPES = [
    AccountStream,
    ItemStream,
    VendorStream,
    OrderStream,
    SaleStream,
    ShipmentStream,
]


class TapRLightspeed(Tap):
    """TapRLightspeed Retail (R-Series) tap class."""

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0] if config else None
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    name = "tap-lightspeed-rseries"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=False,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
        ),
        th.Property(
            "expires_in",
            th.IntegerType,
            required=False,
        ),
        th.Property(
            "expires",
            th.IntegerType,
            required=False,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapRLightspeed.cli()
