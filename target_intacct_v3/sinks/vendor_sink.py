from typing import Dict, List

from hotglue_models_accounting.accounting import Vendor
from target_intacct_v3.base_sinks import IntacctBatchSink
from target_intacct_v3.mappers.vendor_schema_mapper import VendorSchemaMapper


class VendorSink(IntacctBatchSink):
    name = "Vendors"
    record_type = "VENDOR"
    unified_schema = Vendor
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
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

        return {**self._target.reference_data, self.name: existing_vendors}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = VendorSchemaMapper(record, self.name, reference_data=reference_data).to_intacct()
        control_id = f"{index}"
        operation_type = "update" if "RECORDNO" in mapped_record else "create"
        return {
            "controlId": control_id,
            "externalId": mapped_record.pop("externalId", None),
            "operation": operation_type,
            "locationId": mapped_record.pop("LOCATIONID",  "TOP_LEVEL"),
            "payload": {
                "function": {
                    "@controlid": control_id,
                    operation_type: {
                        self.record_type: mapped_record
                    }
                }
            }
        }
    