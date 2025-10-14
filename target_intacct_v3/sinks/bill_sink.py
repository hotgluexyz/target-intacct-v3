from typing import Dict, List

from hotglue_models_accounting.accounting import Bill
from target_intacct_v3.base_sinks import IntacctBatchPreprocessSingleUpsertSink
from target_intacct_v3.mappers.base_mapper import format_supdoc_id, InvalidInputError
from target_intacct_v3.mappers.bill_schema_mapper import BillSchemaMapper
from target_intacct_v3.mappers.attachment_schema_mapper import AttachmentSchemaMapper


class BillSink(IntacctBatchPreprocessSingleUpsertSink):
    name = "Bills"
    record_type = "APBILL"
    unified_schema = Bill
    auto_validate_unified_schema = True
    main_control_id = "bill-upsert"

    def get_batch_reference_data(self, records: List) -> Dict:
        existing_bills = []

        # get existing Bills by id or billNumber
        bill_ids = {record['id'] for record in records if record.get("id")}
        bill_numbers = {record['billNumber'] for record in records if record.get("billNumber")}

        bill_filters = []
        if bill_ids:
            bill_filters.append({
                "field": "RECORDNO",
                "value": list(bill_ids),
            })
        if bill_numbers:
            bill_filters.append({
                "field": "RECORDID",
                "value": list(bill_numbers),
            })

        if bill_filters:
            bill_filter = {"or": {"in": bill_filters}} if len(bill_filters) > 1 else {"in": bill_filters}
            existing_bills = self.intacct_client.get_records("APBILL", filter=bill_filter)

        # get existing Vendors by id or vendorNumber or vendorName
        existing_vendors = []
        vendor_ids = {record['vendorId'] for record in records if record.get("vendorId")}
        vendor_names = {record['vendorName'] for record in records if record.get("vendorName")}
        vendor_numbers = {record['vendorNumber'] for record in records if record.get("vendorNumber")}

        employee_ids = set()
        task_numbers = set()
        item_ids = set()
        item_numbers = set()
        item_names = set()

        for record in records:
            for item_line in record.get("lineItems", []):
                if item_line.get("vendorId"):
                    vendor_ids.add(item_line['vendorId'])
                if item_line.get("vendorName"):
                    vendor_names.add(item_line['vendorName'])
                if item_line.get("vendorNumber"):
                    vendor_numbers.add(item_line['vendorNumber'])
                if item_line.get("employeeId"):
                    employee_ids.add(item_line['employeeId'])
                if item_line.get("taskNumber"):
                    task_numbers.add(item_line['taskNumber'])
                
                if item_line.get("itemId"):
                    item_ids.add(item_line['itemId'])
                if item_line.get("itemNumber"):
                    item_numbers.add(item_line['itemNumber'])
                if item_line.get("itemName"):
                    item_names.add(item_line['itemName'])

            for expense in record.get("expenses", []):
                if expense.get("vendorId"):
                    vendor_ids.add(expense['vendorId'])
                if expense.get("vendorName"):
                    vendor_names.add(expense['vendorName'])
                if expense.get("vendorNumber"):
                    vendor_numbers.add(expense['vendorNumber'])
                if expense.get("employeeId"):
                    employee_ids.add(expense['employeeId'])
                if expense.get("taskNumber"):
                    task_numbers.add(expense['taskNumber'])

        vendor_filters = []
        if vendor_ids:
            vendor_filters.append({
                "field": "RECORDNO",
                "value": list(vendor_ids),
            })
        if vendor_names:
            vendor_filters.append({
                "field": "NAME",
                "value": list(vendor_names),
            })
        if vendor_numbers:
            vendor_filters.append({
                "field": "VENDORID",
                "value": list(vendor_numbers),
            })

        if vendor_filters:
            vendor_filter = {"or": {"in": vendor_filters}} if len(vendor_filters) > 1 else {"in": vendor_filters}
            existing_vendors = self.intacct_client.get_records("VENDOR", filter=vendor_filter)

        existing_employees = []
        employee_filters = []
        if employee_ids:
            employee_filters.append({
                "field": "EMPLOYEEID",
                "value": list(employee_ids),
            })
            employee_filter = {"or": {"in": employee_filters}} if len(employee_filters) > 1 else {"in": employee_filters}
            existing_employees = self.intacct_client.get_records("EMPLOYEE", filter=employee_filter)

        existing_tasks = []
        task_filters = []
        if task_numbers:
            task_filters.append({
                "field": "TASKID",
                "value": list(task_numbers),
            })
            task_filter = {"or": {"in": task_filters}} if len(task_filters) > 1 else {"in": task_filters}
            existing_tasks = self.intacct_client.get_records("TASK", filter=task_filter)

        existing_items = []
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
            existing_items = self.intacct_client.get_records("ITEM", filter=item_filter)
        
        existing_attachment_folders = []
        folder_names = [f"bill-{record['billNumber']}" for record in records if record.get("billNumber")]
        if folder_names:
            existing_attachment_folders = self.intacct_client.get_attachment_folders(folder_names)

        existing_attachments = []
        supdoc_ids = {format_supdoc_id(self.record_type, record['billNumber']) for record in records if record.get("billNumber")}
        if supdoc_ids:
            existing_attachments = self.intacct_client.get_attachments(list(supdoc_ids))

        return {
            **self._target.reference_data,
            self.name: existing_bills,
            "Vendors": existing_vendors,
            "Employees": existing_employees,
            "Tasks": existing_tasks,
            "Items": existing_items,
            "AttachmentFolders": existing_attachment_folders,
            "Attachments": existing_attachments
        }
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = BillSchemaMapper(record, self.name, reference_data=reference_data).to_intacct()
        operation_type = "update" if "RECORDNO" in mapped_record else "create"

        payloads = []

        attachments = record.get("attachments", [])
        if attachments:
            supdoc_id, attachment_payloads = self.map_attachments(mapped_record, attachments, reference_data)
            if supdoc_id:
                mapped_record["SUPDOCID"] = supdoc_id
                payloads += attachment_payloads

        payloads.append({
            "function": {
                "@controlid": self.main_control_id,
                operation_type: {
                    self.record_type: mapped_record
                }
            }
        })
        
        return {
            "externalId": mapped_record.pop("externalId", None),
            "operation": operation_type,
            "locationId": mapped_record.pop("LOCATIONID",  "TOP_LEVEL"),
            "payload": payloads
        }
    
    def map_attachments(self, mapped_record, attachments, reference_data):
        if not attachments:
            return None, []

        self.logger.info(f"Mapping attachments for bill {mapped_record.get('externalId')}")
        record_id = mapped_record.get("RECORDID")
        
        if not record_id:
            raise InvalidInputError(f"Attachemnts cannot be processed becausebillNumber not present.")

        payloads = []

        folder_name = f"bill-{record_id}"
        folder_exists = folder_name in reference_data.get("AttachmentFolders", [])

        if folder_exists:
            self.logger.info(f"Folder {folder_name} already exists. Skipping creation.")
        else:
            self.logger.info(f"Folder {folder_name} does not exist. Will create it.")
            payloads.append(self.intacct_client.get_attachment_folder_create_payload(folder_name))

        supdoc_id = format_supdoc_id(self.record_type,record_id)

        existing_supdoc = next((supdoc for supdoc in reference_data.get("Attachments", []) if supdoc.get("supdocid") == supdoc_id), {})
        existing_attachments = existing_supdoc.get("attachments", {}).get("attachment", [])
        if isinstance(existing_attachments, dict):
            existing_attachments = [existing_attachments]
        
        new_attachments = []

        for attachment in attachments:
            input_path = self._target.config.get('input_path')
            new_attachment = AttachmentSchemaMapper(self.logger).to_intacct(input_path, attachment, existing_attachments)
            if new_attachment:
                new_attachments.append(new_attachment)

        if len(new_attachments) == 0:
            return None, []
        
        # if the supdoc_id already exists we only add new attachments
        operation_type = "update_supdoc" if existing_supdoc else "create_supdoc"
        payloads.append({
            "function": {
                "@controlid": f"{operation_type}_{supdoc_id}",
                operation_type: {
                    "supdocid": supdoc_id,
                    "supdocfoldername": folder_name,
                    "attachments": {"attachment": new_attachments }
                }
            }
            })

        return supdoc_id, payloads    
