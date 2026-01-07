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
            state_updates = []
            self.intacct_client.current_location_id = location_id

            try:
                response = self.make_batch_request(location_records)
                # Handle the batch response 
                result = self.handle_batch_response(response, location_records)
                state_updates = result.get("state_updates", [])
            except Exception as e:
                self.logger.error(f"Failed to make batch request: {e.__repr__()}")
                for record in location_records:
                    state = {"success": False, "error": str(e)}
                    
                    external_id = record.get("externalId")
                    if external_id:
                        state["externalId"] = external_id
                    
                    state_updates.append(state)

            # Update the latest state for each state update in the response
            for state_update in state_updates:
                mapped_record = next((record for record in location_records if record.get("externalId") == state_update.get("externalId")), {})
                self.update_state(state_update, record=mapped_record)

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
                operation = record_payload.get("operation", "")
                # if operations starts with create_ or update_ it's legacy behavior
                if operation.startswith("create_"):
                    record_id = ri.get("key")
                elif operation.startswith("update_"):
                    record_id = record_payload.get("recordId")
                else:
                    record_id = ri["data"][self.record_type.lower()]["RECORDNO"]

                state = {
                    "id": record_id,
                    "externalId": record_payload.get("externalId"),
                    "success": True,
                }

                
                if "update" in operation:
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


class IntacctBatchPreprocessSingleUpsertSink(HotglueBatchSink):
    max_size = 50
    main_control_id = None

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

    def upsert_record(self, record: dict) -> dict:
        try:
            results = self.intacct_client.make_batch_request(record["payload"], is_atomic_request=True)
        except Exception as e:
            self.logger.error(f"Failed to upsert record: {e.__repr__()}")
            return None, False, {"error": str(e)}
        
        # parse results
        success = True
        id = None
        state = {}
        error_messages = []
        for result in results:
            if result.get("status") != "success":
                success = False
                error_message = result.get("errormessage")
                if error_message:
                    error_messages.append(str(error_message))
                continue

            if result["controlid"] == self.main_control_id:
                id = result["data"][self.record_type.lower()]["RECORDNO"]
                if result["function"] == "update":
                    state["is_updated"] = True

        if not success:
            state["error"] = "\n".join(error_messages) if error_messages else str(results)

        return id, success, state

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

            for record in location_records:
                id, success, state = self.upsert_record(record)

                if success:
                    self.logger.info(f"{self.name} processed id: {id}")

                state["success"] = success

                if id:
                    state["id"] = id

                external_id = record.get("externalId")
                if external_id:
                    state["externalId"] = external_id

                self.update_state(state, record=record)

    def make_batch_request(self, records: List[dict]):
        pass
