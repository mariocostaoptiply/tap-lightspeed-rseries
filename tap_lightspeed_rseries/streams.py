"""Stream type classes for tap-lightspeed."""

from typing import Optional, Any, Dict
import requests
import json
from singer_sdk import typing as th
from tap_lightspeed_rseries.client import LightspeedRSeriesStream


class AccountStream(LightspeedRSeriesStream):
    """Define custom stream."""

    name = "account"
    path = "/Account.json"
    primary_keys = ["accountID"]
    replication_key = None

    records_jsonpath = "$.Account"

    schema = th.PropertiesList(
        th.Property("accountID", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("link", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response):
        response_data = response.json()
        account = response_data.get("Account")
        if account:
            yield account
        else:
            self.logger.warning("Account object not found in response")

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        
        accounts_ids = self.config.get("account_ids")
        if accounts_ids:
            # Handle comma-separated account IDs
            if isinstance(accounts_ids, str):
                accounts_ids = [id.strip() for id in accounts_ids.split(",") if id.strip()]
            elif isinstance(accounts_ids, (int, float)):
                accounts_ids = [str(accounts_ids)]
            elif isinstance(accounts_ids, list):
                accounts_ids = [str(id).strip() for id in accounts_ids if id]
            
            # Normalize AccountID from record to string for comparison
            record_account_id = str(record.get("accountID", ""))
            
            # Skip this account if it's not in the allowed list
            if record_account_id not in accounts_ids:
                self.logger.info(
                    f"Skipping account '{record.get('name', 'Unknown')}' "
                    f"({record_account_id}) - not in account_ids filter [{', '.join(accounts_ids)}]"
                )
                return None
            else:
                self.logger.info(
                    f"Processing account '{record.get('name', 'Unknown')}' "
                    f"({record_account_id}) - matches account_ids filter"
                )
        return {
            "accountID": record.get("accountID"),
            "account_name": record.get("name"),
        }

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {}

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        # Convert link to JSON string for consistency
        if "link" in row:
            if row["link"] and row["link"] != "":
                row["link"] = json.dumps(row["link"])
            else:
                row["link"] = None
        
        return row


class ItemStream(LightspeedRSeriesStream):
    """Define custom stream."""

    name = "items"
    parent_stream_type = AccountStream
    path = "/Account/{accountID}/Item.json"
    primary_keys = ["itemID"]
    replication_key = "timeStamp"

    records_jsonpath = "$.Item[*]"

    schema = th.PropertiesList(
        th.Property("accountID", th.StringType, required=True),
        th.Property("account_name", th.StringType),
        th.Property("itemID", th.StringType, required=True),
        th.Property("systemSku", th.StringType),
        th.Property("defaultCost", th.StringType),
        th.Property("avgCost", th.StringType),
        th.Property("discountable", th.StringType),
        th.Property("tax", th.StringType),
        th.Property("archived", th.StringType),
        th.Property("itemType", th.StringType),
        th.Property("serialized", th.StringType),
        th.Property("description", th.StringType),
        th.Property("modelYear", th.StringType),
        th.Property("upc", th.StringType),
        th.Property("ean", th.StringType),
        th.Property("customSku", th.StringType),
        th.Property("manufacturerSku", th.StringType),
        th.Property("createTime", th.DateTimeType),
        th.Property("timeStamp", th.DateTimeType, required=True),
        th.Property("publishToEcom", th.StringType),
        th.Property("categoryID", th.StringType),
        th.Property("taxClassID", th.StringType),
        th.Property("departmentID", th.StringType),
        th.Property("itemMatrixID", th.StringType),
        th.Property("manufacturerID", th.StringType),
        th.Property("seasonID", th.StringType),
        th.Property("defaultVendorID", th.StringType),
        th.Property("laborDurationMinutes", th.StringType),
        th.Property("Category", th.StringType),
        th.Property("TaxClass", th.StringType),
        th.Property("Manufacturer", th.StringType),
        th.Property("Note", th.StringType),
        th.Property("ItemShops", th.StringType),
        th.Property("ItemVendorNums", th.StringType),
        th.Property("ItemComponents", th.StringType),
        th.Property("ItemUUID", th.StringType),
        th.Property("Prices", th.StringType),
        th.Property("Tags", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        
        relations = self.config.get("items_relations", "all")
        
        if relations != "all":
            relations = [relation.strip() for relation in relations.split(",") if relation.strip()]
            params["load_relations"] = json.dumps(relations)
        else:
            params["load_relations"] = "all"
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Convert all relation fields to JSON strings for simplicity
        relation_fields = [
            "Category", "TaxClass", "Manufacturer", "Note", "ItemShops",
            "ItemVendorNums", "ItemComponents", "ItemUUID", "Prices", "Tags"
        ]
        for field in relation_fields:
            if field in row:
                if row[field] and row[field] != "":
                    row[field] = json.dumps(row[field])
                else:
                    row[field] = None
        
        return row


class VendorStream(LightspeedRSeriesStream):
    """Define custom stream."""

    name = "vendors"
    parent_stream_type = AccountStream
    path = "/Account/{accountID}/Vendor.json"
    primary_keys = ["vendorID"]
    replication_key = "timeStamp"

    records_jsonpath = "$.Vendor[*]"

    schema = th.PropertiesList(
        th.Property("accountID", th.StringType, required=True),
        th.Property("account_name", th.StringType),
        th.Property("vendorID", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("archived", th.StringType),
        th.Property("accountNumber", th.StringType),
        th.Property("priceLevel", th.StringType),
        th.Property("updatePrice", th.StringType),
        th.Property("updateCost", th.StringType),
        th.Property("updateDescription", th.StringType),
        th.Property("shareSellThrough", th.StringType),
        th.Property("timeStamp", th.DateTimeType, required=True),
        th.Property("b2bSellerUID", th.StringType),
        th.Property("purchasingCurrency", th.StringType),
        th.Property("Contact", th.StringType),
        th.Property("Reps", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        
        relations = self.config.get("vendors_relations", "all")
        
        if relations != "all":
            relations = [relation.strip() for relation in relations.split(",") if relation.strip()]
            params["load_relations"] = json.dumps(relations)
        else:
            params["load_relations"] = "all"
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Convert all relation fields to JSON strings for simplicity
        relation_fields = ["Contact", "purchasingCurrency", "Reps"]
        for field in relation_fields:
            if field in row:
                if row[field] and row[field] != "":
                    row[field] = json.dumps(row[field])
            else:
                    row[field] = None
        
        return row


class OrderStream(LightspeedRSeriesStream):
    """Define custom stream."""

    name = "orders"
    parent_stream_type = AccountStream
    path = "/Account/{accountID}/Order.json"
    primary_keys = ["orderID"]
    replication_key = "timeStamp"

    records_jsonpath = "$.Order[*]"

    schema = th.PropertiesList(
        th.Property("accountID", th.StringType, required=True),
        th.Property("account_name", th.StringType),
        th.Property("orderID", th.StringType, required=True),
        th.Property("shipInstructions", th.StringType),
        th.Property("stockInstructions", th.StringType),
        th.Property("shipCost", th.StringType),
        th.Property("shipVendorCost", th.StringType),
        th.Property("otherCost", th.StringType),
        th.Property("otherVendorCost", th.StringType),
        th.Property("complete", th.StringType),
        th.Property("archived", th.StringType),
        th.Property("discount", th.StringType),
        th.Property("totalDiscount", th.StringType),
        th.Property("totalQuantity", th.StringType),
        th.Property("subTotalCost", th.StringType),
        th.Property("totalCost", th.StringType),
        th.Property("orderedDate", th.DateTimeType),
        th.Property("receivedDate", th.DateTimeType),
        th.Property("arrivalDate", th.DateTimeType),
        th.Property("timeStamp", th.DateTimeType, required=True),
        th.Property("refNum", th.StringType),
        th.Property("createTime", th.DateTimeType),
        th.Property("vendorCurrencyRate", th.StringType),
        th.Property("vendorCurrencyCode", th.StringType),
        th.Property("shippingCostMethod", th.StringType),
        th.Property("hasShipments", th.StringType),
        th.Property("discountMethod", th.StringType),
        th.Property("discountMoneyValue", th.StringType),
        th.Property("discountMoneyVendorValue", th.StringType),
        th.Property("discountIsPercent", th.StringType),
        th.Property("discountPercentValue", th.StringType),
        th.Property("costsModifiedAfterShipment", th.StringType),
        th.Property("b2bOrderUID", th.StringType),
        th.Property("b2bOrderNumber", th.StringType),
        th.Property("vendorID", th.StringType),
        th.Property("noteID", th.StringType),
        th.Property("shopID", th.StringType),
        th.Property("createdByEmployeeID", th.StringType),
        th.Property("OrderLines", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error in Order response: {e}")
            self.logger.error(f"Response status: {response.status_code}")
            self.logger.error(f"Response text: {response.text[:500]}")
            raise
        
        if not response.text or not response.text.strip():
            self.logger.warning("Empty response from Order endpoint")
            return
        
        try:
            response_data = response.json()
        except ValueError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            self.logger.error(f"Response status: {response.status_code}")
            self.logger.error(f"Response text: {response.text[:500]}")
            raise
        
        order = response_data.get("Order")
        if order:
            if isinstance(order, list):
                for o in order:
                    yield o
            elif isinstance(order, dict):
                yield order
        else:
            self.logger.warning("Order object not found in response")

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        
        relations = self.config.get("orders_relations", "all")
        
        if relations != "all":
            relations = [relation.strip() for relation in relations.split(",") if relation.strip()]
            params["load_relations"] = json.dumps(relations)
        else:
            params["load_relations"] = "all"
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Convert OrderLines to JSON string for simplicity
        if "OrderLines" in row:
            if row["OrderLines"] and row["OrderLines"] != "":
                row["OrderLines"] = json.dumps(row["OrderLines"])
            else:
                row["OrderLines"] = None
        
        return row


class SaleStream(LightspeedRSeriesStream):
    """Define custom stream."""

    name = "sales"
    parent_stream_type = AccountStream
    path = "/Account/{accountID}/Sale.json"
    primary_keys = ["saleID"]
    replication_key = "timeStamp"

    records_jsonpath = "$.Sale[*]"

    schema = th.PropertiesList(
        th.Property("accountID", th.StringType, required=True),
        th.Property("account_name", th.StringType),
        th.Property("saleID", th.StringType, required=True),
        th.Property("timeStamp", th.DateTimeType, required=True),
        th.Property("discountPercent", th.StringType),
        th.Property("completed", th.StringType),
        th.Property("archived", th.StringType),
        th.Property("voided", th.StringType),
        th.Property("enablePromotions", th.StringType),
        th.Property("isTaxInclusive", th.StringType),
        th.Property("tipEnabled", th.StringType),
        th.Property("createTime", th.DateTimeType),
        th.Property("updateTime", th.DateTimeType),
        th.Property("completeTime", th.DateTimeType),
        th.Property("referenceNumber", th.StringType),
        th.Property("referenceNumberSource", th.StringType),
        th.Property("tax1Rate", th.StringType),
        th.Property("tax2Rate", th.StringType),
        th.Property("change", th.StringType),
        th.Property("receiptPreference", th.StringType),
        th.Property("displayableSubtotal", th.StringType),
        th.Property("ticketNumber", th.StringType),
        th.Property("calcDiscount", th.StringType),
        th.Property("calcTotal", th.StringType),
        th.Property("calcSubtotal", th.StringType),
        th.Property("calcTaxable", th.StringType),
        th.Property("calcNonTaxable", th.StringType),
        th.Property("calcAvgCost", th.StringType),
        th.Property("calcFIFOCost", th.StringType),
        th.Property("calcTax1", th.StringType),
        th.Property("calcTax2", th.StringType),
        th.Property("calcPayments", th.StringType),
        th.Property("calcTips", th.StringType),
        th.Property("calcItemFees", th.StringType),
        th.Property("total", th.StringType),
        th.Property("totalDue", th.StringType),
        th.Property("displayableTotal", th.StringType),
        th.Property("balance", th.StringType),
        th.Property("customerID", th.StringType),
        th.Property("discountID", th.StringType),
        th.Property("employeeID", th.StringType),
        th.Property("quoteID", th.StringType),
        th.Property("registerID", th.StringType),
        th.Property("shipToID", th.StringType),
        th.Property("shopID", th.StringType),
        th.Property("taxCategoryID", th.StringType),
        th.Property("tipEmployeeID", th.StringType),
        th.Property("tippableAmount", th.StringType),
        th.Property("taxTotal", th.StringType),
        th.Property("SaleLines", th.StringType),
        th.Property("MetaData", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        
        relations = self.config.get("sales_relations", "all")
        
        if relations != "all":
            relations = [relation.strip() for relation in relations.split(",") if relation.strip()]
            params["load_relations"] = json.dumps(relations)
        else:
            params["load_relations"] = "all"
        
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Convert relation fields to JSON strings for simplicity
        relation_fields = ["SaleLines", "MetaData"]
        for field in relation_fields:
            if field in row:
                if row[field] and row[field] != "":
                    row[field] = json.dumps(row[field])
                else:
                    row[field] = None
        
        return row


class ShipmentStream(LightspeedRSeriesStream):
    """Define Shipment stream for Order Shipments.
    
    Endpoint: GET /API/V3/Account/{accountID}/Shipment.json
    Documentation: https://developers.lightspeedhq.com/retail/endpoints/Order/
    """

    name = "shipments"
    parent_stream_type = AccountStream
    path = "/Account/{accountID}/Shipment.json"
    primary_keys = ["orderShipmentID"]
    replication_key = "timeStamp"

    records_jsonpath = "$.OrderShipment[*]"

    schema = th.PropertiesList(
        th.Property("accountID", th.StringType, required=True),
        th.Property("account_name", th.StringType),
        th.Property("orderShipmentID", th.StringType, required=True),
        th.Property("orderID", th.StringType),
        th.Property("sequenceNumber", th.StringType),
        th.Property("totalQtyReceived", th.StringType),
        th.Property("totalVendorCost", th.StringType),
        th.Property("totalCost", th.StringType),
        th.Property("currencyCode", th.StringType),
        th.Property("vendorCurrencyCode", th.StringType),
        th.Property("vendorCurrencyRate", th.StringType),
        th.Property("createTime", th.DateTimeType),
        th.Property("paymentDueDate", th.StringType),
        th.Property("shipmentPackingRefNum", th.StringType),
        th.Property("timeStamp", th.DateTimeType, required=True),
        th.Property("employeeID", th.StringType),
        th.Property("receptionDate", th.DateTimeType),
        th.Property("shippingCostMethod", th.StringType),
        th.Property("shippingVendorCost", th.StringType),
        th.Property("shippingCost", th.StringType),
        th.Property("shippingCostOrderFullValue", th.StringType),
        th.Property("shippingCostOrderFullVendorValue", th.StringType),
        th.Property("discountMethod", th.StringType),
        th.Property("discountMoneyVendorValue", th.StringType),
        th.Property("discountMoneyValue", th.StringType),
        th.Property("discountPercentValue", th.StringType),
        th.Property("discountOrderFullMoneyValue", th.StringType),
        th.Property("discountOrderFullMoneyVendorValue", th.StringType),
        th.Property("cost", th.StringType),
        th.Property("vendorCost", th.StringType),
        th.Property("status", th.StringType),
        th.Property("Employee", th.StringType),
        th.Property("Order", th.StringType),
        th.Property("OrderShipmentItems", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        
        relations = self.config.get("shipments_relations", "all")
        
        if relations != "all":
            relations = [relation.strip() for relation in relations.split(",") if relation.strip()]
            params["load_relations"] = json.dumps(relations)
        else:
            params["load_relations"] = "all"
            # Reduzir limit quando carregar todas as relações para evitar timeout
            params["limit"] = 50  # Reduzir de 100 para 50
        
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Convert all relation fields to JSON strings for simplicity
        relation_fields = ["Employee", "Order", "OrderShipmentItems"]
        for field in relation_fields:
            if field in row:
                if row[field] and row[field] != "":
                    row[field] = json.dumps(row[field])
                else:
                    row[field] = None
        
        return row


class ShopStream(LightspeedRSeriesStream):
    """Define Shop stream.
    
    Endpoint: GET /API/V3/Account/{accountID}/Shop.json
    Documentation: https://developers.lightspeedhq.com/retail/endpoints/Shop/
    """

    name = "shops"
    parent_stream_type = AccountStream
    path = "/Account/{accountID}/Shop.json"
    primary_keys = ["shopID"]
    replication_key = None

    records_jsonpath = "$.Shop[*]"

    schema = th.PropertiesList(
        th.Property("accountID", th.StringType, required=True),
        th.Property("account_name", th.StringType),
        th.Property("shopID", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("serviceRate", th.StringType),
        th.Property("timeZone", th.StringType),
        th.Property("taxLabor", th.StringType),
        th.Property("labelTitle", th.StringType),
        th.Property("labelMsrp", th.StringType),
        th.Property("archived", th.StringType),
        th.Property("timeStamp", th.DateTimeType, required=True),
        th.Property("companyRegistrationNumber", th.StringType),
        th.Property("vatNumber", th.StringType),
        th.Property("zebraBrowserPrint", th.StringType),
        th.Property("contactID", th.StringType),
        th.Property("taxCategoryID", th.StringType),
        th.Property("receiptSetupID", th.StringType),
        th.Property("ccGatewayID", th.StringType),
        th.Property("gatewayConfigID", th.StringType),
        th.Property("priceLevelID", th.StringType),
        th.Property("Contact", th.StringType),
        th.Property("ReceiptSetup", th.StringType),
        th.Property("TaxCategory", th.StringType),
        th.Property("ShelfLocations", th.StringType),
        th.Property("Registers", th.StringType),
        th.Property("CCGateway", th.StringType),
        th.Property("PriceLevel", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["load_relations"] = "all"
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Convert all relation fields to JSON strings for simplicity
        relation_fields = [
            "Contact", "ReceiptSetup", "TaxCategory", "ShelfLocations",
            "Registers", "CCGateway", "PriceLevel"
        ]
        for field in relation_fields:
            if field in row:
                if row[field] and row[field] != "":
                    row[field] = json.dumps(row[field])
                else:
                    row[field] = None
        
        return row