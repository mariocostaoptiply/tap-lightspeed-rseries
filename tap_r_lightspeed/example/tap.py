"""dynamics-bc tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_dynamics_bc.streams import (
    AccountsStream,
    CompaniesStream,
    CompanyInformationStream,
    ItemsStream,
    LocationsStream,
    PurchaseInvoicesStream,
    PurchaseOrdersStream,
    SalesInvoicesStream,
    VendorPurchases,
    VendorsStream,
    SalesOrdersStream,
    GeneralLedgerEntriesStream,
    GeneralLedgerEntriesIncrementalStream,
    GLEntriesDimensionsStream,
    DimensionsStream,
    DimensionValuesStream,
    CustomersStream,
    CurrenciesStream,
    VendorPaymentJournalsStream,
    PaymentTermsStream,
    VendorLedgerEntriesStream,
    ItemVariantsStream,
    InventoryByLocationStream,
    ItemWithVariantsStream
)

STREAM_TYPES = [
    CompaniesStream,
    CompanyInformationStream,
    ItemsStream,
    VendorsStream,
    VendorPurchases,
    SalesInvoicesStream,
    PurchaseInvoicesStream,
    PurchaseOrdersStream,
    AccountsStream,
    LocationsStream,
    SalesOrdersStream,
    GeneralLedgerEntriesStream,
    GeneralLedgerEntriesIncrementalStream,
    GLEntriesDimensionsStream,
    DimensionsStream,
    DimensionValuesStream,
    CustomersStream,
    CurrenciesStream,
    VendorPaymentJournalsStream,
    PaymentTermsStream,
    VendorLedgerEntriesStream,
    ItemVariantsStream,
    InventoryByLocationStream,
    ItemWithVariantsStream
]


class TapdynamicsBc(Tap):
    """dynamics-bc tap class."""

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    name = "tap-dynamics-bc"

    # TODO: Update this section with the actual config values you expect:
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
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "environment_name",
            th.StringType,
            required=True,
        ),
        th.Property(
            "company_ids",
            th.StringType,
            required=False,
            description="Optional company ID(s) to sync. Can be a single company ID string or comma-separated company IDs. If not provided, all companies will be synced.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapdynamicsBc.cli()
