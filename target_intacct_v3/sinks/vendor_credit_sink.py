from typing import Dict, List

from hotglue_models_accounting.accounting import VendorCredit
from target_intacct_v3.base_sinks import IntacctBatchSink
from target_intacct_v3.mappers.vendor_credit_schema_mapper import VendorCreditSchemaMapper


class VendorCreditSink(IntacctBatchSink):
    name = "VendorCredits"
    record_type = "apadjustment"
    unified_schema = VendorCredit
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing vendor credits by id or vendorCreditNumber
        existing_vendor_credits = []
        vendor_credit_ids = {record['id'] for record in records if record.get("id")}
        vendor_credit_numbers = {record['vendorCreditNumber'] for record in records if record.get("vendorCreditNumber")}

        vendor_credit_filters = []
        if vendor_credit_ids:
            vendor_credit_filters.append({
                "field": "RECORDNO",
                "value": list(vendor_credit_ids),
            })
        if vendor_credit_numbers:
            vendor_credit_filters.append({
                "field": "RECORDID",
                "value": list(vendor_credit_numbers),
            })

        if vendor_credit_filters:
            vendor_credit_filter = {"or": {"in": vendor_credit_filters}} if len(vendor_credit_filters) > 1 else {"in": vendor_credit_filters}
            existing_vendor_credits = self.intacct_client.get_records("APADJUSTMENT", filter=vendor_credit_filter)

        existing_vc_ids = {record['RECORDNO'] for record in existing_vendor_credits if record.get("RECORDNO")}
        existing_vc_lines = {}
        if existing_vc_ids:
            existing_vc_lines = self.intacct_client.get_existing_vendor_credit_lines(existing_vc_ids)

        # get existing vendors by id or vendorNumber or vendorName
        existing_vendors = []
        vendor_ids = {record['id'] for record in records if record.get("id")}
        vendor_names = {record['vendorName'] for record in records if record.get("vendorName")}
        vendor_numbers = {record['vendorNumber'] for record in records if record.get("vendorNumber")}

        vendor_filters = []
        if vendor_ids:
            vendor_filters.append({
                "field": "RECORDNO",
                "value": list(vendor_ids),
            })
        if vendor_names:
            vendor_filters.append({
                "field": "NAME",
                "value": list(vendor_names),
            })
        if vendor_numbers:
            vendor_filters.append({
                "field": "VENDORID",
                "value": list(vendor_numbers),
            })

        if vendor_filters:
            vendor_filter = {"or": {"in": vendor_filters}} if len(vendor_filters) > 1 else {"in": vendor_filters}
            existing_vendors = self.intacct_client.get_records("VENDOR", filter=vendor_filter)

        item_ids = set()
        item_numbers = set()
        item_names = set()
        for record in records:
            for item_line in record.get("lineItems", []):               
                if item_line.get("itemId"):
                    item_ids.add(item_line['itemId'])
                if item_line.get("itemNumber"):
                    item_numbers.add(item_line['itemNumber'])
                if item_line.get("itemName"):
                    item_names.add(item_line['itemName'])

        existing_items = []
        item_filters = []
        if item_ids:
            item_filters.append({
                "field": "RECORDNO",
                "value": list(item_ids),
            })
        if item_names:
            item_filters.append({
                "field": "NAME",
                "value": list(item_names),
            })
        if item_numbers:
            item_filters.append({
                "field": "ITEMID",
                "value": list(item_numbers),
            })

        if item_filters:
            item_filter = {"or": {"in": item_filters}} if len(item_filters) > 1 else {"in": item_filters}
            existing_items = self.intacct_client.get_records("ITEM", filter=item_filter)

        return {**self._target.reference_data, self.name: existing_vendor_credits, "Vendors": existing_vendors, "Items": existing_items, "VendorCreditLines": existing_vc_lines}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = VendorCreditSchemaMapper(record, self.name, reference_data=reference_data).to_intacct()
        control_id = f"{index}"
        operation_type = "update_apadjustment" if "@key" in mapped_record else "create_apadjustment"
        return {
            "controlId": control_id,
            "externalId": mapped_record.pop("externalId", None),
            "operation": operation_type,
            "recordId": mapped_record.get("@key", None),
            "locationId": mapped_record.pop("LOCATIONID",  "TOP_LEVEL"),
            "payload": {
                "function": {
                    "@controlid": control_id,
                    operation_type: mapped_record
                }
            }
        }
    