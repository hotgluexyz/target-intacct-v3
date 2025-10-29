import datetime
from typing import Dict
from target_intacct_v3.mappers.base_mapper import BaseMapper
from target_intacct_v3.mappers.bill_line_item_or_expense_schema_mapper import BillLineItemOrExpenseSchemaMapper

class BillSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "intacct_field": "RECORDNO", "required_if_present": True},
        {"record_field": "billNumber", "intacct_field": "RECORDID", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "billNumber": "RECORDID",
        "description": "DESCRIPTION",
        "currency": ["CURRENCY", "BASECURR"],
        "createdAt": "WHENCREATED",
        "issueDate": "WHENPOSTED",
        "dueDate": "WHENDUE"
    }

    def to_intacct(self) -> Dict:
        if not self.record.get("isDraft") and not self.record.get("createdAt"):
            self.record["createdAt"] = datetime.datetime.now(datetime.timezone.utc)
        
        payload = {
            **self._map_internal_id(),
            **self._map_subsidiary(),
            **self._map_is_draft(),
            **self._map_sub_record("Vendors", "VENDORID", record_no_field="vendorId", record_id_field="vendorNumber", record_name_field="vendorName", subsidiary_id=self.subsidiary_id),
            **self._map_custom_fields()
        }
     
        self._map_line_items_and_expenses(payload)
        self._map_fields(payload)

        return payload

    def _map_line_items_and_expenses(self, payload):
        mapped_lines = []
        vendor_id = payload["VENDORID"]

        line_items = self.record.get("lineItems", [])
        if line_items:
            for line in line_items:
                mapped_line = BillLineItemOrExpenseSchemaMapper(line, "BillLineItem", self.subsidiary_id, vendor_id, self.reference_data).to_intacct()
                mapped_lines.append(mapped_line)
        
        expenses = self.record.get("expenses", [])
        if expenses:
            for expense in expenses:
                mapped_line = BillLineItemOrExpenseSchemaMapper(expense, "BillLineExpense", self.subsidiary_id, vendor_id, self.reference_data).to_intacct()
                mapped_lines.append(mapped_line)

        if mapped_lines:
            payload["APBILLITEMS"] = { "APBILLITEM": mapped_lines }
