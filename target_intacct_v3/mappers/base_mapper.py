import datetime

class InvalidInputError(Exception):
    pass

class RecordNotFound(InvalidInputError):
    pass

def format_supdoc_id(record_type: str, record_id: str) -> str:
    record_id = record_id.replace("-","")
    return f"{record_type}-{record_id}"[-20:]  # supdocid only allows 20 chars

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
        self.subsidiary_number = self._get_subsidiary_number()
        self.existing_record = self._find_existing_record(self.reference_data.get(self.sink_name, []))

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """
        location_id = self.subsidiary_number

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

    def _map_internal_id(self, as_key=False):
        if self.existing_record:
            if as_key:
                return {
                    "@key": self.existing_record["RECORDNO"]
                }
            return {
                "RECORDNO": self.existing_record["RECORDNO"]
            }

        return {}

    def _get_subsidiary_number(self):
        found_subsidiary = None

        subsidiary_id = self.record.get("subsidiaryId")
        if subsidiary_id:
            found_subsidiary = next(
                    (intacct_record for intacct_record in self.reference_data.get("Subsidiaries", [])
                    if str(intacct_record["RECORDNO"]) == str(subsidiary_id)),
                    None
                )

        subsidiary_number = self.record.get("subsidiaryNumber")
        if subsidiary_number:
            found_subsidiary = next(
                    (intacct_record for intacct_record in self.reference_data.get("Subsidiaries", [])
                    if str(intacct_record["LOCATIONID"]) == str(subsidiary_number)),
                    None
                )

        subsidiary_name = self.record.get("subsidiaryName")
        if found_subsidiary is None and subsidiary_name:
            found_subsidiary = next(
                    (intacct_record for intacct_record in self.reference_data.get("Subsidiaries", [])
                    if str(intacct_record["NAME"]) == str(subsidiary_name)),
                    None
                )

        if found_subsidiary is None and (subsidiary_id or subsidiary_number or subsidiary_name):
            raise RecordNotFound(f"Subsidiary not found with subsidiaryId='{subsidiary_id}' / subsidiaryNumber='{subsidiary_number}' / subsidiaryName='{subsidiary_name}'.")

        return found_subsidiary["LOCATIONID"] if found_subsidiary else "TOP_LEVEL"

    def _map_subsidiary(self):
        return {
            "LOCATIONID": self.subsidiary_number
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
                        payload[payload_key] = record_value.strftime("%m/%d/%Y")
                    else:
                        payload[payload_key] = record_value

    def _map_is_active(self):
        is_active = self.record.get("isActive")
        if is_active is not None:
            return {"STATUS": "active" if is_active else "inactive"}
        return {}

    def _map_is_draft(self, key_name="ACTION"):
        if self.record.get("isDraft"):
            return {key_name: "Draft"}
        return {}

    def _map_date_legacy(self, source_field_name, target_field_name, required=True):
        date_value = self.record.get(source_field_name)

        if not date_value and required:
            raise RecordNotFound(f"{source_field_name} is required but it was not present in the record")

        if date_value:
            return {
                target_field_name: {
                    "year": date_value.year,
                    "month": date_value.month,
                    "day": date_value.day
                }
            }
        
        return {}

    def _order_payload(self, payload, order_keys):
        new_dict = {key: payload.get(key, None) for key in order_keys if key in payload}
        new_dict.update({key: payload[key] for key in payload if key not in order_keys})
        return new_dict

    def _find_entity(self, entity_name, record_no_field=None, record_id_field=None, record_name_field=None, subsidiary_number=None, required=True, required_if_present=True):
        found_entity = None
        no_value = self.record.get(record_no_field) if record_no_field else None
        id_value = self.record.get(record_id_field) if record_id_field else None
        name_value = self.record.get(record_name_field) if record_name_field else None

        if no_value is None and id_value is None and name_value is None:
            if required:
                raise RecordNotFound(f"{entity_name} is required and none of the matching fields are present: {record_no_field}/{record_id_field}/{record_name_field}")
            return {}
        
        reference_list = self.reference_data.get(entity_name, [])

        # if subsidiary is TOP_LEVEL or None we match for all subsidiaries
        should_match_subsidiary = subsidiary_number not in ["TOP_LEVEL", None]
        valid_subsidiaries = [subsidiary_number, "TOP_LEVEL"] if subsidiary_number not in ["TOP_LEVEL", None] else ["TOP_LEVEL"]

        # iterate over valid subsidiaries because we wanna first look for the entity
        # at the subsidiary level and then at the top level
        for subsidiary in valid_subsidiaries:
            if no_value:
                found_entity = next(
                    (entity for entity in reference_list
                    if entity["RECORDNO"] == no_value and 
                        ((should_match_subsidiary and entity["MEGAENTITYID"] == subsidiary) or not should_match_subsidiary)),
                    None
                )
            
            if found_entity is None and id_value:
                found_entity = next(
                    (entity for entity in reference_list
                    if entity["ENTITYID"] == id_value and 
                        ((should_match_subsidiary and entity["MEGAENTITYID"] == subsidiary) or not should_match_subsidiary)),
                    None
                )
            
            if found_entity is None and name_value:
                found_entity = next(
                    (entity for entity in reference_list
                    if entity["ENTITYNAME"] == name_value and 
                        ((should_match_subsidiary and entity["MEGAENTITYID"] == subsidiary) or not should_match_subsidiary)),
                    None
                )
            
            if found_entity:
                return found_entity

        if found_entity is None and required_if_present:
            fields = [(record_no_field, no_value), (record_id_field, id_value), (record_name_field, name_value)]
            raise RecordNotFound(f"{entity_name} could not be found in Intacct with {' / '.join([f'{field}={value}' for field, value in fields if field and value])}")
        
        return {}

    def _map_sub_record(self, entity_name, target_field_name, record_no_field=None, record_id_field=None, record_name_field=None, subsidiary_number=None, required=True, required_if_present=True):
        found_entity = self._find_entity(entity_name, record_no_field, record_id_field, record_name_field, subsidiary_number, required, required_if_present)
        return {target_field_name: found_entity["ENTITYID"]} if found_entity else {}

    def _map_custom_fields_legacy(self):
        custom_fields = self.record.get("customFields", [])
        custom_fields_payload = []

        if custom_fields:
            for custom_field in custom_fields:
                custom_fields_payload.append({
                    "customfieldname": custom_field.get("name"),
                    "customfieldvalue": custom_field.get("value")
                })

            return {
                "customfields": { "customfield": custom_fields_payload }
            }
        return {}

    def _map_custom_fields(self):
        custom_fields = self.record.get("customFields", [])
        custom_fields_payload = {}

        if custom_fields:
            for custom_field in custom_fields:
                if custom_field.get("name") and custom_field.get("value") is not None:
                    custom_fields_payload[custom_field.get("name")] = custom_field.get("value")

        return custom_fields_payload
