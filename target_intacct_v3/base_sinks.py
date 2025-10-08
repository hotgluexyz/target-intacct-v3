import json
from typing import Dict, List, Optional
from collections import defaultdict

from singer_sdk.plugin_base import PluginBase

from target_hotglue.client import HotglueBatchSink
from target_hotglue.client import HotglueBatchSink
from target_intacct_v3.client import IntacctClient


class IntacctBatchSink(HotglueBatchSink):
    max_size = 50

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: Optional[List[str]]) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.intacct_client: IntacctClient = target.intacct_client
        self.reference_data = self._target.reference_data

    def validate_input(self, record: dict):
        return True    

    def get_batch_reference_data(self, records: List) -> dict:
        """Get the reference data for a batch

        Args:
            records: List of records to be processed by the batch

        Returns:
            A dict containing batch specific reference data.
        """
        return self._target.reference_data

    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        return {"bId": f"bid{index}", "operation": record[2], record[0]: record[1]}

    def process_batch(self, context: dict) -> None:
        # If the latest state is not set, initialize it
        if not self.latest_state:
            self.init_state()
        
        # Extract the raw records from the context
        raw_records = context.get("records", [])

        # make sure we're signed into the top level location before fetching reference data
        self.intacct_client.current_location_id = "TOP_LEVEL"
        reference_data = self.get_batch_reference_data(raw_records)

        records = []
        for raw_record in enumerate(raw_records):
            try:
                # performs record mapping from unified to Intacct
                record = self.process_batch_record(raw_record[1], raw_record[0], reference_data)
                records.append(record)
            except Exception as e:
                state = {"success": False, "error": str(e)}
                
                id = raw_record[1].get("id")
                if id:
                    state["id"] = str(id)
                
                external_id = raw_record[1].get("externalId")
                if external_id:
                    state["externalId"] = external_id
                
                self.update_state(state)

        if not records:
            return
        
        # group records by locationId because each different location needs a new session
        records_by_location = defaultdict(list)
        for record in records:
            records_by_location[record['locationId']].append(record)

        for location_id, location_records in records_by_location.items():
            self.intacct_client.current_location_id = location_id
            response = self.make_batch_request(location_records)
            # Handle the batch response 
            result = self.handle_batch_response(response, location_records)
            state_updates = result.get("state_updates", [])

            # Update the latest state for each state update in the response
            for state_update in state_updates:
                self.update_state(state_update)

    def make_batch_request(self, records: List[Dict]):
        records_payload = []
        
        for record in records:
            records_payload.append(record["payload"])
        
        return self.intacct_client.make_batch_request(records_payload)

    def handle_batch_response(self, response, records):
        response_items = response or []
        state_updates = []

        for ri in response_items:
            record_payload = next((record for record in records if record.get("controlId") == ri.get("controlid")), {})

            if ri.get("status") == "success":
                state = {
                    "id": ri["data"][self.record_type.lower()]["RECORDNO"],
                    "externalId": record_payload.get("externalId"),
                    "success": True,
                }

                if record_payload.get("operation") == "update":
                    state["is_updated"] = True
                
                state_updates.append(state)
            else:
                self.logger.error(f"Failure processing entity. Error=[{json.dumps(ri)}]")
                state_updates.append({
                    "success": False,
                    "externalId": record_payload.get("externalId"),
                    "error": ri.get("errormessage", {}).get("error", {})
                })


        return {"state_updates": state_updates}
