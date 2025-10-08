import datetime

class InvalidInputError(Exception):
    pass

class RecordNotFound(InvalidInputError):
    pass

class BaseMapper:
    """A base class responsible for mapping a record ingested in the unified schema format to a payload for Intacct"""
    existing_record_pk_mappings = []
    field_mappings = {}

    def __init__(
            self,
            record,
            sink_name,
            reference_data
    ) -> None:
        self.record = record
        self.sink_name = sink_name
        self.reference_data = reference_data
        self.existing_record = self._find_existing_record(self.reference_data.get(self.sink_name, []))

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """
        location_id = self._map_subsidiary()["LOCATIONID"]
        if location_id == "TOP_LEVEL":
            location_id = None

        for existing_record_pk_mapping in self.existing_record_pk_mappings:
            record_id = self.record.get(existing_record_pk_mapping["record_field"])
            if record_id:
                found_record = next(
                    (intacct_record for intacct_record in reference_list
                    if location_id == intacct_record["MEGAENTITYID"] and str(intacct_record[existing_record_pk_mapping["intacct_field"]]) == str(record_id)),
                    None
                )
                if existing_record_pk_mapping["required_if_present"] and found_record is None:
                    raise RecordNotFound(f"Record {existing_record_pk_mapping['record_field']}={record_id} not found in Intacct. Skipping it")
                
                if found_record:
                    return found_record
        
        return None

    def _map_internal_id(self):
        if self.existing_record:
            return {
                "RECORDNO": self.existing_record["RECORDNO"]
            }

        return {}

    def _map_subsidiary(self):
        found_subsidiary = None

        subsidiary_id = self.record.get("subsidiaryId")
        if subsidiary_id:
            found_subsidiary = next(
                    (intacct_record for intacct_record in self.reference_data.get("Subsidiaries", [])
                    if str(intacct_record["LOCATIONID"]) == str(subsidiary_id)),
                    None
                )

        subsidiary_name = self.record.get("subsidiaryName")
        if found_subsidiary is None and subsidiary_name:
            found_subsidiary = next(
                    (intacct_record for intacct_record in self.reference_data.get("Subsidiaries", [])
                    if str(intacct_record["NAME"]) == str(subsidiary_name)),
                    None
                )

        if found_subsidiary is None and (subsidiary_id or subsidiary_name):
            raise RecordNotFound(f"Subsidiary not found with subsidiaryId='{subsidiary_id}' / subsidiaryName='{subsidiary_name}'.")

        return {
            "LOCATIONID": found_subsidiary["LOCATIONID"] if found_subsidiary else "TOP_LEVEL"
        }

    def _map_fields(self, payload, custom_field_mappings={}):
        field_mappings = self.field_mappings

        if custom_field_mappings:
            field_mappings = custom_field_mappings

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record and self.record.get(record_key) != None:
                if isinstance(payload_key, list):
                    for key in payload_key:
                        payload[key] = self.record.get(record_key)
                else:
                    record_value = self.record.get(record_key)
                    if isinstance(record_value, datetime.datetime):
                        payload[payload_key] = record_value.isoformat()
                    else:
                        payload[payload_key] = record_value

    def _map_is_active(self):
        is_active = self.record.get("isActive")
        if is_active is not None:
            return {"STATUS": "active" if is_active else "inactive"}
        return {}
