from typing import Dict, List

from hotglue_models_accounting.accounting import BillPayment
from target_intacct_v3.base_sinks import IntacctBatchSink
from target_intacct_v3.mappers.bill_payment_schema_mapper import BillPaymentSchemaMapper


class BillPaymentSink(IntacctBatchSink):
    name = "BillPayments"
    record_type = "APPYMT"
    unified_schema = BillPayment
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing BillPaymentss by id or transactionNumber
        existing_bill_payments = []
        bill_payments_ids = {record['id'] for record in records if record.get("id")}
        bill_payments_numbers = {record['transactionNumber'] for record in records if record.get("transactionNumber")}

        bill_payments_filters = []
        if bill_payments_ids:
            bill_payments_filters.append({
                "field": "RECORDNO",
                "value": list(bill_payments_ids),
            })
        if bill_payments_numbers:
            bill_payments_filters.append({
                "field": "DOCNUMBER",
                "value": list(bill_payments_numbers),
            })

        if bill_payments_filters:
            bill_payments_filter = {"or": {"in": bill_payments_filters}} if len(bill_payments_filters) > 1 else {"in": bill_payments_filters}
            existing_bill_payments = self.intacct_client.get_records(self.record_type, filter=bill_payments_filter)

        # get existing Bills by billId or billNumber
        existing_bills = []
        bill_ids = {record['billId'] for record in records if record.get("billId")}
        bill_numbers = {record['billNumber'] for record in records if record.get("billNumber")}

        bill_filters = []
        if bill_ids:
            bill_filters.append({
                "field": "RECORDNO",
                "value": list(bill_ids),
            })
        if bill_numbers:
            bill_filters.append({
                "field": "RECORDID",
                "value": list(bill_numbers),
            })

        if bill_filters:
            bill_filter = {"or": {"in": bill_filters}} if len(bill_filters) > 1 else {"in": bill_filters}
            existing_bills = self.intacct_client.get_records("APBILL", filter=bill_filter, extra_fields=["CURRENCY", "VENDORID"])

        # get existing Vendors by id or vendorNumber or vendorName
        existing_vendors = []
        vendor_ids = {record['vendorId'] for record in records if record.get("vendorId")}
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

        return {
            **self._target.reference_data,
            self.name: existing_bill_payments,
            "Bills": existing_bills,
            "Vendors": existing_vendors
        }
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = BillPaymentSchemaMapper(record, self.name, reference_data=reference_data).to_intacct()
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
