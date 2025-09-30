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

        for existing_record_pk_mapping in self.existing_record_pk_mappings:
            record_id = self.record.get(existing_record_pk_mapping["record_field"])
            if record_id:
                found_record = next(
                    (intacct_record for intacct_record in reference_list
                    if str(intacct_record[existing_record_pk_mapping["intacct_field"]]) == str(record_id)),
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
