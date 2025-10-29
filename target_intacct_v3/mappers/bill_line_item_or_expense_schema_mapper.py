from typing import Dict
from target_intacct_v3.mappers.base_mapper import BaseMapper, RecordNotFound

class BillLineItemOrExpenseSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    def __init__(
            self,
            record,
            sink_name,
            subsidiary_id,
            header_vendor_id,
            reference_data
    ) -> None:
        self.record = record
        self.sink_name = sink_name
        self.reference_data = reference_data
        self.subsidiary_id = subsidiary_id
        self.header_vendor_id = header_vendor_id

    field_mappings = {
        "description": "ENTRYDESCRIPTION",
        "amount": "TRX_AMOUNT"
    }

    def to_intacct(self) -> Dict:
        payload = {
            **self._map_sub_record("Accounts", "ACCOUNTNO", record_no_field="accountId",
                record_id_field="accountNumber", record_name_field="accountName",
                subsidiary_id=self.subsidiary_id),
            **self._map_sub_record("Vendors", "VENDORID", record_no_field="vendorId",
                record_id_field="vendorNumber", record_name_field="vendorName",
                subsidiary_id=self.subsidiary_id, required=False),
            **self._map_sub_record("Classes", "CLASSID", record_no_field="classId",
                record_id_field="classNumber", record_name_field="className",
                subsidiary_id=self.subsidiary_id, required=False),
            **self._map_sub_record("Departments", "DEPARTMENTID", record_no_field="departmentId",
                record_id_field="departmentNumber", record_name_field="departmentName",
                required=False),
            **self._map_sub_record("Projects", "PROJECTID", record_no_field="projectId",
                record_id_field="projectNumber", record_name_field="projectName",
                subsidiary_id=self.subsidiary_id, required=False),
            **self._map_sub_record("Locations", "LOCATIONID", record_no_field="locationId",
                record_id_field="locationNumber", record_name_field="locationName",
                required=False),
            **self._map_sub_record("Tasks", "TASKID", record_id_field="taskNumber",
                required=False),
            **self._map_sub_record("Employees", "EMPLOYEEID", record_id_field="employeeId",
                subsidiary_id=self.subsidiary_id, required=False),
            **self._map_custom_fields()
        }

        # fallback to header vendor if vendor not supplied at line level
        if "VENDORID" not in payload:
            payload["VENDORID"] = self.header_vendor_id
        
        if self.sink_name == "BillLineItem":
            self._map_item(payload)

        self._map_fields(payload)

        return payload
    
    def _map_item(self, payload):
        found_item = self._find_entity("Items", record_no_field="itemId",
                record_id_field="itemNumber", record_name_field="itemName",
                subsidiary_id=self.subsidiary_id, required=True)

        payload["ITEMID"] = found_item["ITEMID"]
