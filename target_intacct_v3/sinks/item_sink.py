from typing import Dict, List

from hotglue_models_accounting.accounting import Item
from target_intacct_v3.base_sinks import IntacctBatchSink
from target_intacct_v3.mappers.item_schema_mapper import ItemSchemaMapper


class ItemSink(IntacctBatchSink):
    name = "Items"
    record_type = "ITEM"
    unified_schema = Item
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing items by id or displayName or itemNumber
        existing_items = []
        item_ids = {record['id'] for record in records if record.get("id")}
        item_names = {record['displayName'] for record in records if record.get("displayName")}
        item_numbers = {record['itemNumber'] for record in records if record.get("itemNumber")}

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
            existing_items = self.intacct_client.get_records(self.record_type, fields=["RECORDNO", "NAME", "ITEMID", "MEGAENTITYID"], filter=item_filter)
        
        return {**self._target.reference_data, self.name: existing_items}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = ItemSchemaMapper(record, self.name, reference_data=reference_data).to_intacct()
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
    