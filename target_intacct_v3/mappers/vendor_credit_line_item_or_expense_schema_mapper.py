from typing import Dict
from target_intacct_v3.mappers.base_mapper import BaseMapper, RecordNotFound

class VendorCreditLineItemOrExpenseSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    def __init__(
            self,
            record,
            sink_name,
            subsidiary_number,
            reference_data,
            existing_lines
    ) -> None:
        self.record = record
        self.sink_name = sink_name
        self.reference_data = reference_data
        self.subsidiary_number = subsidiary_number
        self.existing_lines = existing_lines
        self.is_item_line = sink_name == "VendorCreditLineItem"

    field_mappings = {
        "description": "memo",
        "amount": "amount"
    }

    def to_intacct(self) -> Dict:
        payload = {
            **self._map_sub_record("Accounts", "glaccountno", record_no_field="accountId",
                record_id_field="accountNumber", record_name_field="accountName",
                subsidiary_number=self.subsidiary_number),
            **self._map_sub_record("Classes", "classid", record_no_field="classId",
                record_id_field="classNumber", record_name_field="className",
                subsidiary_number=self.subsidiary_number, required=False),
            **self._map_sub_record("Departments", "departmentid", record_no_field="departmentId",
                record_id_field="departmentNumber", record_name_field="departmentName",
                required=False),
            **self._map_sub_record("Locations", "locationid", record_no_field="locationId",
                record_id_field="locationNumber", record_name_field="locationName",
                required=False),
            **self._map_custom_fields_legacy()
        }
        
        if self.is_item_line:
            self._map_item(payload)

        self._map_fields(payload)
        # in case it's an update, we need to map the line number
        self._map_line_number(payload)

        order_keys = ["glaccountno", "amount", "memo", "locationid", "departmentid", "customfields", "itemid","classid"]
        payload = self._order_payload(payload, order_keys)

        return payload
    
    def _map_line_number(self, payload):
        record_description = payload.get("memo")
        record_item_id = payload.get("itemid")
        record_account_id = payload.get("glaccountno")

        existing_line = None
        if record_description and record_account_id and record_item_id:
            existing_line = next(
                (line for line in self.existing_lines
                if line["ENTRYDESCRIPTION"] == record_description and line["ITEMID"] == record_item_id and line["ACCOUNTNO"] == record_account_id),
                None
            )
        elif record_description and record_account_id:
            existing_line = next(
                (line for line in self.existing_lines
                if line["ENTRYDESCRIPTION"] == record_description and line["ACCOUNTNO"] == record_account_id),
                None
            )

        if existing_line:
            payload["@line_num"] = existing_line["LINE_NO"]

    def _map_item(self, payload):
        found_item = self._find_entity("Items", record_no_field="itemId",
                record_id_field="itemNumber", record_name_field="itemName",
                subsidiary_number=self.subsidiary_number, required=True)

        payload["itemid"] = found_item["ITEMID"] 
