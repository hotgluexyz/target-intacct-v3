from typing import Dict
from target_intacct_v3.mappers.base_mapper import BaseMapper, InvalidInputError

class ItemSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "intacct_field": "RECORDNO", "required_if_present": True},
        {"record_field": "itemNumber", "intacct_field": "ITEMID", "required_if_present": False},
        {"record_field": "displayName", "intacct_field": "NAME", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "displayName": "NAME"
    }

    def to_intacct(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_subsidiary(),
            **self._map_item_id(),
            **self._map_item_type(),
            # TODO: Map Accounts: our test account is not enabled to set this at the moment
            **self._map_is_active()
        }
        
        self._map_fields(payload)

        return payload

    def _map_item_id(self):
        if self.existing_record:
            return {"ITEMID": self.existing_record["ITEMID"]}
        
        item_id = self.record.get("itemNumber")

        if not item_id:
            raise InvalidInputError(f"itemNumber is required.")

        return {"ITEMID": item_id}

    def _map_item_type(self):
        item_type = self.record.get("type")
        allowed_item_types = ["Inventory", "Non-Inventory", "Non-Inventory (Purchase only)", "Non-Inventory (Sales only)"]

        if item_type not in allowed_item_types:
            raise InvalidInputError(f"Invalid item type: '{item_type}'. Allowed item types are: {allowed_item_types}")

        return {"ITEMTYPE": item_type}
