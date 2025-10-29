from typing import Dict
from collections import defaultdict
from target_intacct_v3.mappers.base_mapper import BaseMapper
from target_intacct_v3.mappers.vendor_credit_line_item_or_expense_schema_mapper import VendorCreditLineItemOrExpenseSchemaMapper

class VendorCreditSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "intacct_field": "RECORDNO", "required_if_present": True},
        {"record_field": "vendorCreditNumber", "intacct_field": "RECORDID", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "vendorCreditNumber": "adjustmentno",
        "description": "description",
        "currency": ["currency", "basecurr"]
    }

    def to_intacct(self) -> Dict:
        payload = {
            **self._map_internal_id(as_key=True),
            **self._map_subsidiary(),
            **self._map_sub_record("Vendors", "vendorid", record_no_field="vendorId", record_id_field="vendorNumber", record_name_field="vendorName", subsidiary_id=self.subsidiary_id),
            **self._map_date_legacy("issueDate", "datecreated"),
            **self._map_exchange_rate(),
            **self._map_custom_fields_legacy(),
            **self._map_is_draft(key_name="action")
        }

        is_update = self.existing_record is not None
        self._map_line_items_and_expenses(payload, is_update)
        self._map_fields(payload)

        if is_update:
            payload.pop("basecurr", None)
            # it's not possible to update custom fields at the header level
            payload.pop("customfields", None)

        order_keys = ["vendorid", "datecreated", "adjustmentno", "action", "description", "basecurr", "currency", "exchrate", "exchratetype", "customfields", "apadjustmentitems", "updateapadjustmentitems"]
        payload = self._order_payload(payload, order_keys)

        return payload

    def _map_exchange_rate(self):
        if self.record.get("exchangeRate"):
            return {"exchrate": self.record.get("exchangeRate")}
        return {"exchratetype": "Intacct Daily Rate"}

    def _map_line_items_and_expenses(self, payload: dict, is_update: bool):
        adjustment_items_key = "updateapadjustmentitems" if is_update else "apadjustmentitems"
        payload[adjustment_items_key] = defaultdict(list)
        existing_lines = self.reference_data.get("VendorCreditLines", {}).get(self.existing_record["RECORDNO"], []) if is_update else []

        line_items = self.record.get("lineItems", [])
        if line_items:
            for line in line_items:
                mapped_line = VendorCreditLineItemOrExpenseSchemaMapper(line, "VendorCreditLineItem", self.subsidiary_id, self.reference_data, existing_lines).to_intacct()
                line_operation_key = "updatelineitem" if mapped_line.get("@line_num") else "lineitem"
                payload[adjustment_items_key][line_operation_key].append(mapped_line)
        
        expenses = self.record.get("expenses", [])
        if expenses:
            for expense in expenses:
                mapped_line = VendorCreditLineItemOrExpenseSchemaMapper(expense, "VendorCreditLineExpense", self.subsidiary_id, self.reference_data, existing_lines).to_intacct()
                line_operation_key = "updatelineitem" if mapped_line.get("@line_num") else "lineitem"
                payload[adjustment_items_key][line_operation_key].append(mapped_line)
