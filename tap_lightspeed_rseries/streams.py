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
        th.Property(
            "link",
            th.ObjectType(
                th.Property(
                    "@attributes",
                    th.ObjectType(
                        th.Property("href", th.StringType),
                    ),
                ),
            ),
        ),
    ).to_dict()

    def parse_response(self, response: requests.Response):
        response_data = response.json()
        account = response_data.get("Account")
        if account:
            yield account
        else:
            self.logger.warning("Account object not found in response")

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "accountID": record.get("accountID"),
            "account_name": record.get("name"),
        }

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {}


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
        th.Property(
            "Category",
            th.ObjectType(
                th.Property("categoryID", th.StringType),
                th.Property("name", th.StringType),
                th.Property("nodeDepth", th.StringType),
                th.Property("fullPathName", th.StringType),
                th.Property("leftNode", th.StringType),
                th.Property("rightNode", th.StringType),
                th.Property("createTime", th.DateTimeType),
                th.Property("timeStamp", th.DateTimeType),
                th.Property("parentID", th.StringType),
            ),
        ),
        th.Property(
            "TaxClass",
            th.ObjectType(
                th.Property("taxClassID", th.StringType),
                th.Property("name", th.StringType),
                th.Property("classType", th.StringType),
                th.Property("timeStamp", th.DateTimeType),
            ),
        ),
        th.Property(
            "Manufacturer",
            th.ObjectType(
                th.Property("manufacturerID", th.StringType),
                th.Property("name", th.StringType),
                th.Property("createTime", th.DateTimeType),
                th.Property("timeStamp", th.DateTimeType),
            ),
        ),
        th.Property(
            "Note",
            th.ObjectType(
                th.Property("note", th.StringType),
                th.Property("isPublic", th.StringType),
                th.Property("timeStamp", th.DateTimeType),
            ),
        ),
        th.Property(
            "ItemShops",
            th.ObjectType(
                th.Property(
                    "ItemShop",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("itemShopID", th.StringType),
                            th.Property("qoh", th.StringType),
                            th.Property("sellable", th.StringType),
                            th.Property("backorder", th.StringType),
                            th.Property("componentQoh", th.StringType),
                            th.Property("componentBackorder", th.StringType),
                            th.Property("onLayaway", th.StringType),
                            th.Property("onSpecialOrder", th.StringType),
                            th.Property("onWorkorder", th.StringType),
                            th.Property("onTransferOut", th.StringType),
                            th.Property("onTransferIn", th.StringType),
                            th.Property("averageCost", th.StringType),
                            th.Property("totalValueFifo", th.StringType),
                            th.Property("totalValueAvgCost", th.StringType),
                            th.Property("totalValueNegativeInventory", th.StringType),
                            th.Property("lastReceivedCost", th.StringType),
                            th.Property("nextFifoLotCost", th.StringType),
                            th.Property("reorderPoint", th.StringType),
                            th.Property("reorderLevel", th.StringType),
                            th.Property("timeStamp", th.DateTimeType),
                            th.Property("itemID", th.StringType),
                            th.Property("shopID", th.StringType),
                            th.Property("lastReceivedLotID", th.StringType),
                            th.Property("nextFifoLotID", th.StringType),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "ItemVendorNums",
            th.ObjectType(
                th.Property(
                    "ItemVendorNum",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("itemVendorNumID", th.StringType),
                            th.Property("value", th.StringType),
                            th.Property("timeStamp", th.DateTimeType),
                            th.Property("cost", th.StringType),
                            th.Property("b2bCatalogUUID", th.StringType),
                            th.Property("itemID", th.StringType),
                            th.Property("vendorID", th.StringType),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "ItemComponents",
            th.ObjectType(
                th.Property(
                    "ItemComponent",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("itemComponentID", th.StringType),
                            th.Property("quantity", th.StringType),
                            th.Property("componentGroup", th.StringType),
                            th.Property("assemblyItemID", th.StringType),
                            th.Property("componentItemID", th.StringType),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "ItemUUID",
            th.ObjectType(
                th.Property(
                    "ItemUUID",
                    th.ObjectType(
                        th.Property("productUUID", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property(
            "Prices",
            th.ObjectType(
                th.Property(
                    "ItemPrice",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("amount", th.StringType),
                            th.Property("useTypeID", th.StringType),
                            th.Property("useType", th.StringType),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "Tags",
            th.ObjectType(
                th.Property(
                    "@attributes",
                    th.ObjectType(
                        th.Property("count", th.StringType),
                    ),
                ),
                th.Property(
                    "tag",
                    th.ArrayType(th.StringType),
                ),
            ),
        ),
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
        
        # Normalize ItemVendorNums.ItemVendorNum: convert single object to array
        if "ItemVendorNums" in row and row["ItemVendorNums"]:
            item_vendor_nums = row["ItemVendorNums"]
            if "ItemVendorNum" in item_vendor_nums:
                item_vendor_num = item_vendor_nums["ItemVendorNum"]
                # If it's a dict (single object), convert to array
                if isinstance(item_vendor_num, dict):
                    item_vendor_nums["ItemVendorNum"] = [item_vendor_num]
                # If it's already a list, keep it as is
                elif not isinstance(item_vendor_num, list):
                    item_vendor_nums["ItemVendorNum"] = []
        
        # Normalize ItemComponents.ItemComponent: convert single object to array
        if "ItemComponents" in row and row["ItemComponents"]:
            item_components = row["ItemComponents"]
            if "ItemComponent" in item_components:
                item_component = item_components["ItemComponent"]
                # If it's a dict (single object), convert to array
                if isinstance(item_component, dict):
                    item_components["ItemComponent"] = [item_component]
                # If it's already a list, keep it as is
                elif not isinstance(item_component, list):
                    item_components["ItemComponent"] = []
        
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
        th.Property(
            "purchasingCurrency",
            th.ObjectType(
                th.Property("code", th.StringType),
                th.Property("symbol", th.StringType),
                th.Property("rate", th.StringType),
            ),
        ),
        th.Property(
            "Contact",
            th.ObjectType(
                th.Property("contactID", th.StringType),
                th.Property("custom", th.StringType),
                th.Property("noEmail", th.StringType),
                th.Property("noPhone", th.StringType),
                th.Property("noMail", th.StringType),
                th.Property(
                    "Addresses",
                    th.ObjectType(
                        th.Property(
                            "ContactAddress",
                            th.ObjectType(
                                th.Property("address1", th.StringType),
                                th.Property("address2", th.StringType),
                                th.Property("city", th.StringType),
                                th.Property("state", th.StringType),
                                th.Property("zip", th.StringType),
                                th.Property("country", th.StringType),
                                th.Property("countryCode", th.StringType),
                                th.Property("stateCode", th.StringType),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "Phones",
                    th.ObjectType(
                        th.Property(
                            "ContactPhone",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("number", th.StringType),
                                    th.Property("useType", th.StringType),
                                ),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "Emails",
                    th.ObjectType(
                        th.Property(
                            "ContactEmail",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("address", th.StringType),
                                    th.Property("useType", th.StringType),
                                ),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "Websites",
                    th.ObjectType(
                        th.Property(
                            "ContactWebsite",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("url", th.StringType),
                                ),
                            ),
                        ),
                    ),
                ),
                th.Property("timeStamp", th.DateTimeType),
            ),
        ),
        th.Property(
            "Reps",
            th.ObjectType(
                th.Property(
                    "VendorRep",
                    th.ObjectType(
                        th.Property("firstName", th.StringType),
                        th.Property("lastName", th.StringType),
                    ),
                ),
            ),
        ),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["load_relations"] = json.dumps(["Contact"])
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Normalize Contact.Phones.ContactPhone: convert single object to array
        if "Contact" in row and row["Contact"]:
            contact = row["Contact"]
            if "Phones" in contact and contact["Phones"]:
                phones = contact["Phones"]
                if "ContactPhone" in phones:
                    contact_phone = phones["ContactPhone"]
                    if isinstance(contact_phone, dict):
                        phones["ContactPhone"] = [contact_phone]
                    elif not isinstance(contact_phone, list):
                        phones["ContactPhone"] = []
            
            # Normalize Contact.Emails.ContactEmail: convert single object to array
            if "Emails" in contact and contact["Emails"]:
                emails = contact["Emails"]
                if "ContactEmail" in emails:
                    contact_email = emails["ContactEmail"]
                    if isinstance(contact_email, dict):
                        emails["ContactEmail"] = [contact_email]
                    elif not isinstance(contact_email, list):
                        emails["ContactEmail"] = []
            
            # Normalize Contact.Websites.ContactWebsite: convert single object to array
            if "Websites" in contact and contact["Websites"]:
                websites = contact["Websites"]
                if "ContactWebsite" in websites:
                    contact_website = websites["ContactWebsite"]
                    if isinstance(contact_website, dict):
                        websites["ContactWebsite"] = [contact_website]
                    elif not isinstance(contact_website, list):
                        websites["ContactWebsite"] = []
        
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
        th.Property(
            "OrderLines",
            th.ObjectType(
                th.Property(
                    "OrderLine",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("orderLineID", th.StringType),
                            th.Property("quantity", th.StringType),
                            th.Property("price", th.StringType),
                            th.Property("originalPrice", th.StringType),
                            th.Property("vendorCost", th.StringType),
                            th.Property("checkedIn", th.StringType),
                            th.Property("numReceived", th.StringType),
                            th.Property("timeStamp", th.DateTimeType),
                            th.Property("total", th.StringType),
                            th.Property("createTime", th.DateTimeType),
                            th.Property("shippingCost", th.StringType),
                            th.Property("shippingVendorCost", th.StringType),
                            th.Property("discountMoneyValue", th.StringType),
                            th.Property("discountMoneyVendorValue", th.StringType),
                            th.Property("discountPercentValue", th.StringType),
                            th.Property("orderID", th.StringType),
                            th.Property("itemID", th.StringType),
                        ),
                    ),
                ),
            ),
        ),
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
        params["load_relations"] = json.dumps(["OrderLines"])
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Normalize OrderLines.OrderLine: convert single object to array
        if "OrderLines" in row and row["OrderLines"]:
            order_lines = row["OrderLines"]
            if "OrderLine" in order_lines:
                order_line = order_lines["OrderLine"]
                if isinstance(order_line, dict):
                    order_lines["OrderLine"] = [order_line]
                elif not isinstance(order_line, list):
                    order_lines["OrderLine"] = []
        
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
        th.Property(
            "SaleLines",
            th.ObjectType(
                th.Property(
                    "SaleLine",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("saleLineID", th.StringType),
                            th.Property("createTime", th.DateTimeType),
                            th.Property("timeStamp", th.DateTimeType),
                            th.Property("unitQuantity", th.StringType),
                            th.Property("unitPrice", th.StringType),
                            th.Property("normalUnitPrice", th.StringType),
                            th.Property("discountAmount", th.StringType),
                            th.Property("discountPercent", th.StringType),
                            th.Property("avgCost", th.StringType),
                            th.Property("fifoCost", th.StringType),
                            th.Property("tax", th.StringType),
                            th.Property("tax1Rate", th.StringType),
                            th.Property("tax2Rate", th.StringType),
                            th.Property("isLayaway", th.StringType),
                            th.Property("isWorkorder", th.StringType),
                            th.Property("isSpecialOrder", th.StringType),
                            th.Property("displayableSubtotal", th.StringType),
                            th.Property("displayableUnitPrice", th.StringType),
                            th.Property("lineType", th.StringType),
                            th.Property("calcLineDiscount", th.StringType),
                            th.Property("calcTransactionDiscount", th.StringType),
                            th.Property("calcTotal", th.StringType),
                            th.Property("calcSubtotal", th.StringType),
                            th.Property("calcTax1", th.StringType),
                            th.Property("calcTax2", th.StringType),
                            th.Property("taxClassID", th.StringType),
                            th.Property("customerID", th.StringType),
                            th.Property("discountID", th.StringType),
                            th.Property("employeeID", th.StringType),
                            th.Property("itemID", th.StringType),
                            th.Property("noteID", th.StringType),
                            th.Property("parentSaleLineID", th.StringType),
                            th.Property("shopID", th.StringType),
                            th.Property("saleID", th.StringType),
                            th.Property("itemFeeID", th.StringType),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "MetaData",
            th.ObjectType(
                th.Property("tipOption1", th.StringType),
                th.Property("tipOption2", th.StringType),
                th.Property("tipOption3", th.StringType),
            ),
        ),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["load_relations"] = json.dumps(["SaleLines"])
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Normalize SaleLines.SaleLine: convert single object to array
        if "SaleLines" in row and row["SaleLines"]:
            sale_lines = row["SaleLines"]
            if "SaleLine" in sale_lines:
                sale_line = sale_lines["SaleLine"]
                if isinstance(sale_line, dict):
                    sale_lines["SaleLine"] = [sale_line]
                elif not isinstance(sale_line, list):
                    sale_lines["SaleLine"] = []
        
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
        th.Property(
            "OrderShipmentItems",
            th.ObjectType(
                th.Property(
                    "OrderShipmentItem",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("orderShipmentItemID", th.StringType),
                            th.Property("orderShipmentID", th.StringType),
                            th.Property("qtyReceived", th.StringType),
                            th.Property("vendorCost", th.StringType),
                            th.Property("cost", th.StringType),
                            th.Property("totalVendorCost", th.StringType),
                            th.Property("totalCost", th.StringType),
                            th.Property("shippingCost", th.StringType),
                            th.Property("shippingVendorCost", th.StringType),
                            th.Property("discountMoneyValue", th.StringType),
                            th.Property("discountMoneyVendorValue", th.StringType),
                            th.Property("discountPercentValue", th.StringType),
                            th.Property("currencyCode", th.StringType),
                            th.Property("vendorCurrencyCode", th.StringType),
                            th.Property("vendorCurrencyRate", th.StringType),
                            th.Property("createTime", th.DateTimeType),
                            th.Property("timeStamp", th.DateTimeType),
                            th.Property("employeeID", th.StringType),
                            th.Property("itemID", th.StringType),
                            th.Property("itemVendorID", th.StringType),
                            th.Property("itemDescription", th.StringType),
                            th.Property(
                                "Item",
                                th.ObjectType(
                                    th.Property("itemID", th.StringType),
                                    th.Property("systemSku", th.StringType),
                                    th.Property("defaultCost", th.StringType),
                                    th.Property("avgCost", th.StringType),
                                    th.Property("fifoCost", th.StringType),
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
                                    th.Property("publishToEcom", th.StringType),
                                    th.Property("timeStamp", th.DateTimeType),
                                    th.Property("createTime", th.DateTimeType),
                                    th.Property("categoryID", th.StringType),
                                    th.Property("taxClassID", th.StringType),
                                    th.Property("departmentID", th.StringType),
                                    th.Property("itemMatrixID", th.StringType),
                                    th.Property("itemAttributesID", th.StringType),
                                    th.Property("manufacturerID", th.StringType),
                                    th.Property("seasonID", th.StringType),
                                    th.Property("defaultVendorID", th.StringType),
                                    th.Property(
                                        "Prices",
                                        th.ObjectType(
                                            th.Property(
                                                "ItemPrice",
                                                th.ArrayType(
                                                    th.ObjectType(
                                                        th.Property("amount", th.StringType),
                                                        th.Property("useType", th.StringType),
                                                        th.Property("useTypeID", th.StringType),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["load_relations"] = json.dumps(["OrderShipmentItems"])
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["accountID"] = context.get("accountID")
            row["account_name"] = context.get("account_name")
        
        # Normalize OrderShipmentItems.OrderShipmentItem: convert single object to array
        if "OrderShipmentItems" in row and row["OrderShipmentItems"]:
            shipment_items = row["OrderShipmentItems"]
            if "OrderShipmentItem" in shipment_items:
                shipment_item = shipment_items["OrderShipmentItem"]
                if isinstance(shipment_item, dict):
                    shipment_items["OrderShipmentItem"] = [shipment_item]
                elif not isinstance(shipment_item, list):
                    shipment_items["OrderShipmentItem"] = []
        
        return row