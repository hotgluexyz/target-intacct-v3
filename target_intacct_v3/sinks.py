"""IntacctV3 target sink class, which handles writing streams."""


import re

from target_intacct_v3.client import IntacctSink
from target_intacct_v3.util import *


class Suppliers(IntacctSink):
    """IntacctV3 target sink class."""

    name = "Suppliers"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            # get list of vendors
            self.get_vendors()

            # map record
            addresses = parse_objs(record.get("addresses"))
            address = addresses[0] if addresses else {}

            phone_numbers = parse_objs(record.get("phoneNumbers"))
            phone = phone_numbers[0] if phone_numbers else {}

            payload = {
                "VENDORID": record.get("vendorNumber"),
                "NAME": record.get("vendorName"),
                "CURRENCY": record.get("currency"),
                "COMMENTS": record.get("note"),
                "PHONE1": phone.get("number"),
                "DISPLAYCONTACT": {
                    "MAILADDRESS": {
                        "ADDRESS1": address.get("line1"),
                        "ADDRESS2": address.get("line2"),
                        "CITY": address.get("city"),
                        "STATE": address.get("state"),
                        "ZIP": address.get("postalCode"),
                        "COUNTRY": address.get("country"),
                    }
                },
            }

            # check for duplicates
            vendor_id = payload.get(
                "VENDORID"
            )  # VENDORID is required if company does not use document sequencing
            if vendor_id and re.match("^[A-Za-z0-9- ]*$", vendor_id):
                payload["VENDORID"] = vendor_id[
                    :20
                ]  # Intact size limit on VENDORID (20 characters)

                if (payload["VENDORID"] in IntacctSink.vendors.items()) or (
                    payload["NAME"] in IntacctSink.vendors.keys()
                ):
                    return {
                        "error": f"Skipping vendor with VENDORID: {vendor_id} and name {payload['NAME']} due a vendor with same NAME or VENDORID exists."
                    }
            else:
                return {
                    "error": f"Skipping vendor due VENDORID is either missing or has unsupported chars. chars. Only letters, numbers and dashes accepted."
                }

            return {"VENDOR": payload}
        except Exception as e:
            return {"error": e.__repr__()}

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record["error"])
        if record:
            response = self.request_api("POST", request_data={"create": record})
            id = response["data"]["vendor"]["VENDORID"]
            return id, True, state_updates


class APAdjustments(IntacctSink):
    """IntacctV3 target sink class."""

    name = "APAdjustment"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            payload = {
                "vendorid": record.get("vendorId"),
                "datecreated": {
                    "year": record.get("transactionDate", "").split("-")[0],
                    "month": record.get("transactionDate", "").split("-")[1],
                    "day": record.get("transactionDate", "").split("-")[2],
                },
                "adjustmentno": record.get("adjustmentNumber"),
                "action": "Draft"
                if record.get("status", "").lower() == "draft"
                else "Submit",
                "billno": record.get("billNumber"),
                "description": record.get("description"),
                "currency": record.get("currency"),
                "exchratetype": "Intacct Daily Rate",
                "apadjustmentitems": {"lineitem": []},
            }

            self.get_vendors()
            if (
                payload.get("vendorname")
                and payload.get("vendorid") not in IntacctSink.vendors.values()
            ):
                payload["vendorid"] = IntacctSink.vendors.get(payload["vendorname"])

            lines = parse_objs(record.get("lineItems", []))
            for line in lines:
                item = {
                    "accountlabel": line.get("accountName"),
                    "glaccountno": line.get(
                        "accountNumber",
                    ),
                    "amount": line.get("amount"),
                    "memo": line.get("memo"),
                    "locationname": line.get("locationName"),
                    "locationid": line.get("locationId"),
                    "departmentname": line.get("departmentName"),
                    "departmentid": line.get("departmentId"),
                    "projectname": line.get("projectName"),
                    "projectid": line.get("projectId"),
                    "vendorname": line.get("vendorName"),
                    "vendorid": line.get("vendorId"),
                    "classname": line.get("className"),
                    "classid": line.get("classId"),
                }

                accountlabel = item.pop("accountlabel", None)
                if accountlabel and not item.get("glaccountno"):
                    self.get_accounts()
                    item["glaccountno"] = IntacctSink.accounts.get(item["accountlabel"])

                vendorname = item.pop("vendorname", None)
                if vendorname and not item.get("vendorid"):
                    self.get_vendors()
                    try:
                        item["vendorid"] = IntacctSink.vendors[item["vendorname"]]
                    except:
                        raise Exception(
                            f"ERROR: vendorname {item['vendorname']} not found for this account."
                        )

                projectname = item.pop("projectname", None)
                if projectname and not item.get("projectid"):
                    self.get_projects()
                    try:
                        item["projectid"] = IntacctSink.projects[item["projectname"]]
                    except:
                        raise Exception(
                            f"ERROR: projectname {item['projectname']} not found for this account."
                        )

                locationname = item.pop("locationname", None)
                if locationname and not item.get("locationid"):
                    self.get_locations()
                    try:
                        item["locationid"] = IntacctSink.locations[item["locationname"]]
                    except:
                        raise Exception(
                            f"ERROR: locationname {item['locationname']} not found for this account."
                        )

                classname = item.pop("classname", None)
                if classname and not item.get("classid"):
                    self.get_classes()
                    try:
                        item["classid"] = IntacctSink.classes[item["classname"]]
                    except:
                        raise Exception(
                            f"ERROR: classname {item['classname']} not found for this account."
                        )

                departmentname = item.pop("departmentname", None)
                if departmentname and not item.get("departmentid"):
                    self.get_departments()
                    try:
                        item["departmentid"] = IntacctSink.departments[
                            item["departmentname"]
                        ]
                    except:
                        raise Exception(
                            f"ERROR: departmentname {item['departmentname']} not found for this account."
                        )

                payload["apadjustmentitems"]["lineitem"].append(item)

            payload = clean_convert(payload)
            return payload
        except Exception as e:
            return {"error": e.__repr__()}

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record["error"])
        if record:
            response = self.request_api(
                "POST", request_data={"create_apadjustment": record}
            )
            id = response["key"]
            return id, True, state_updates


class JournalEntries(IntacctSink):
    """IntacctV3 target sink class."""

    name = "JournalEntries"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            payload = {
                "JOURNAL": record.get("type"),
                "BATCH_TITLE": record.get("type"),
                "BATCH_DATE": record.get("transactionDate", "").split("T")[0],
                "BASELOCATION_NO": record.get("sourceEntityId"),
                "ENTRIES": {"GLENTRY": []},
            }

            lines = parse_objs(record.get("lines"))

            for je in lines:
                item = {
                    "ACCOUNTNO": je.get("accountNumber"),
                    "DESCRIPTION": je.get("description"),
                    "TRX_AMOUNT": je.get("amount"),
                    "TR_TYPE": 1
                    if je.get("postingType", "").lower() == "debit"
                    else -1,
                    "DEPARTMENT": je.get("departmentId"),
                    "ACCOUNTID": je.get("accountId"),
                    "CLASSID": je.get("classId"),
                    "CUSTOMERID": je.get("customerId"),
                    "VENDORID": je.get("vendorId"),
                    "LOCATION": je.get("locationId"),
                }

                self.get_accounts()
                accountname = je.get("ACCOUNTNAME", None)
                if (
                    accountname
                    and item.get("ACCOUNTNO") not in IntacctSink.accounts.values()
                ):
                    try:
                        item["ACCOUNTNO"] = IntacctSink.accounts.get(
                            item["ACCOUNTNAME"]
                        )
                    except:
                        return {
                            "error": f"ACCOUNTNO '{item.get('ACCOUNTNO')}' and ACCOUNTNAME '{accountname}' were not found or invalid for this account. \n Intacct Requires an ACCOUNTNO associated with each line item"
                        }

                departmentname = je.get("departmentName", je.get("department", None))
                if departmentname and not item.get("DEPARTMENT"):
                    self.get_departments()
                    item["DEPARTMENT"] = IntacctSink.departments.get(departmentname)

                locationname = je.get("locationName")
                if locationname and not item.get("LOCATION"):
                    self.get_locations()
                    item["LOCATION"] = IntacctSink.locations.get(locationname)

                classname = je.get("className")
                if classname and not item.get("CLASSID"):
                    self.get_classes()
                    item["CLASSID"] = IntacctSink.classes.get(classname)

                customername = je.get("customerName")
                if customername and not item.get("CUSTOMERID"):
                    self.get_customers()
                    item["CUSTOMERID"] = IntacctSink.customers.get(customername)

                vendorname = je.get("vendorName")
                if vendorname and not item.get("VENDORID"):
                    self.get_vendors()
                    item["VENDORID"] = IntacctSink.vendors.get(vendorname)

                payload["ENTRIES"]["GLENTRY"].append(item)

            payload = clean_convert(payload)
            return {"GLBATCH": payload}
        except Exception as e:
            return {"error": e.__repr__()}

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record["error"])
        if record:
            response = self.request_api("POST", request_data={"create": record})
            id = response["data"]["glbatch"]["RECORDNO"]
            return id, True, state_updates


class Bills(IntacctSink):
    """IntacctV3 target sink class."""

    name = "Bills"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            # Map bill
            payload = {
                "ACTION": "Draft"
                if record.get("status", "").lower() == "draft"
                else None,
                "WHENDUE": record.get("dueDate"),
                "BASECURR": record.get("currency"),
                "RECPAYMENTDATE": record.get("paidDate"),
                "WHENCREATED": record.get("createdAt", "").split("T")[0],
                "WHENPOSTED": record.get("issueDate"),
                "APBILLITEMS": {"APBILLITEM": []},
                "VENDORID": record.get("vendorId"),
                "RECORDID": record.get("invoiceNumber"),
                "LOCATIONID": record.get("locationId"),
            }

            # check if bill exists
            record_no = self.get_records(
                "APBILL",
                fields=["RECORDNO"],
                filter={
                    "filter": {
                        "equalto": {
                            "field": "RECORDID",
                            "value": payload.get("RECORDID"),
                        }
                    }
                },
            )
            if record_no:
                payload["RECORDNO"] = record_no[0].get("RECORDNO")

            # include locationid at header level
            locationname = record.get("location")
            if locationname and not payload.get("LOCATIONID"):
                self.get_locations()
                try:
                    payload["LOCATIONID"] = IntacctSink.locations[locationname]
                except:
                    return {
                        "error": f"ERROR: Location '{locationname}' does not exist. Did you mean any of these: {list(IntacctSink.locations.keys())}?"
                    }

            # look for vendorName, vendorNumber and vendorId
            vendorname = record.get("vendorName")
            if vendorname and not payload.get("VENDORID"):
                self.get_vendors()
                try:
                    payload["VENDORID"] = IntacctSink.vendors[vendorname]
                except:
                    return {
                        "error": f"ERROR: Vendor {vendorname} does not exist. Did you mean any of these: {list(IntacctSink.vendors.keys())}?"
                    }

            vendor_number = record.get("vendorNum")
            if not payload.get("VENDORID") and vendor_number:
                self.get_vendors()
                if vendor_number in IntacctSink.vendors.values():
                    payload["VENDORID"] = vendor_number
                else:
                    return {
                        "error": f"ERROR: VENDORID {vendor_number} not found for this account."
                    }

            lines = parse_objs(record.get("lineItems", "[]"))
            expenses = parse_objs(record.get("expenses", "[]"))

            for line in lines + expenses:
                item = {
                    "PROJECTID": line.get("projectId"),
                    "TRX_AMOUNT": line.get("totalPrice", line.get("amount")),
                    "ACCOUNTNAME": line.get("accountName"),
                    "ENTRYDESCRIPTION": line.get("description"),
                    "LOCATIONID": payload.get("LOCATIONID"),  # same as header level
                    "CLASSID": line.get("classId"),
                    "ACCOUNTNO": line.get("accountNumber"),
                    "VENDORID": line.get("vendorId"),
                }

                if line.get("vendorName") and not item.get("VENDORID"):
                    self.get_vendors()
                    item["VENDORID"] = IntacctSink.vendors[payload["VENDORNAME"]]

                class_name = line.get("className")
                if class_name and not item.get("CLASSID"):
                    self.get_classes()
                    try:
                        item["CLASSID"] = IntacctSink.classes[class_name]
                    except:
                        self.logger.info(
                            f"Skipping class due Class {class_name} does not exist. Did you mean any of these: {list(IntacctSink.classes.keys())}?"
                        )

                self.get_accounts()
                account_name = line.get("accountName")
                if (
                    account_name
                    and item.get("ACCOUNTNO") not in IntacctSink.accounts.keys()
                ):
                    item["ACCOUNTNO"] = IntacctSink.accounts.get(account_name)
                if not item.get("ACCOUNTNO"):
                    return {
                        "error": f"ERROR: ACCOUNTNAME or ACCOUNTNO not found for this tenant in item {item}. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                    }

                # departmentid is optional
                department = line.get("department")
                department_name = line.get("departmentName")
                if department or department_name:
                    self.get_departments()
                    item["DEPARTMENTID"] = IntacctSink.departments.get(
                        department
                    ) or IntacctSink.departments.get(department_name)
                payload["APBILLITEMS"]["APBILLITEM"].append(item)

            # send payload and attachments
            payload = clean_convert(payload)
            return {
                "payload": {"APBILL": payload},
                "attachments": record.get("attachments"),
            }
        except Exception as e:
            return {"error": e.__repr__()}

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record["error"])
        if record:
            payload, attachments = record.values()
            record_id = payload["APBILL"].get("RECORDID")
            # post/update attachments if exist
            supdoc_id = None
            if attachments and record_id:
                supdoc_id = self.post_attachments(attachments, record_id)
                payload["APBILL"]["SUPDOCID"] = supdoc_id
            # post/update bill
            try:
                # post/update bill
                action = "update" if payload["APBILL"].get("RECORDNO") else "create"
                response = self.request_api("POST", request_data={action: payload})
                id = response["data"]["apbill"]["RECORDNO"]
                return id, True, state_updates
            except Exception as e:
                # if bill is new and attachments were sent delete the sent attachments
                if supdoc_id and action == "create":
                    self.logger.info(
                        f"Posting bill with RECORDID {record_id} has failed due to {e.__repr__}, deleting sent attachments..."
                    )
                    self.request_api(
                        "POST", request_data={"delete_supdoc": {"@key": supdoc_id}}
                    )
                raise Exception(e.__repr__())


class PurchaseInvoices(IntacctSink):
    """IntacctV3 target sink class."""

    name = "PurchaseInvoices"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            # Map bill
            payload = {
                "ACTION": "Draft"
                if record.get("status", "").lower() == "draft"
                else None,
                "WHENDUE": record.get("dueDate"),
                "BASECURR": record.get("currency"),
                "RECPAYMENTDATE": record.get("paidDate"),
                "WHENCREATED": record.get("createdAt", "").split("T")[0],
                "WHENPOSTED": record.get("issueDate"),
                "APBILLITEMS": {"APBILLITEM": []},
                "VENDORID": record.get("supplierCode", record.get("supplierNumber")),
                "RECORDID": record.get("invoiceNumber"),
                "LOCATIONID": record.get("locationId"),
                "DOCNUMBER": record.get("number"),
                "DESCRIPTION": record.get("description"),
            }

            # check if bill exists
            record_no = self.get_records(
                "APBILL",
                fields=["RECORDNO"],
                filter={
                    "filter": {
                        "equalto": {
                            "field": "RECORDID",
                            "value": payload.get("RECORDID"),
                        }
                    }
                },
            )
            if record_no:
                payload["RECORDNO"] = record_no[0].get("RECORDNO")

            # include locationid at header level
            address = parse_objs(record.get("addresses", "[]"))
            if address:
                address_location = address[0].get("name")
            locationname = record.get("location") or address_location
            if locationname and not payload.get("LOCATIONID"):
                self.get_locations()
                try:
                    payload["LOCATIONID"] = IntacctSink.locations[locationname]
                except:
                    return {
                        "error": f"ERROR: Location '{locationname}' does not exist. Did you mean any of these: {list(IntacctSink.locations.keys())}?"
                    }

            # look for vendorName, vendorNumber and vendorId
            vendorname = record.get("supplierName")
            if vendorname and not payload.get("VENDORID"):
                self.get_vendors()
                try:
                    payload["VENDORID"] = IntacctSink.vendors[vendorname]
                except:
                    return {
                        "error": f"ERROR: Vendor {vendorname} does not exist. Did you mean any of these: {list(IntacctSink.vendors.keys())}?"
                    }

            vendor_number = record.get("vendorNum")
            if not payload.get("VENDORID") and vendor_number:
                self.get_vendors()
                if vendor_number in IntacctSink.vendors.values():
                    payload["VENDORID"] = vendor_number
                else:
                    return {
                        "error": f"ERROR: VENDORID {vendor_number} not found for this account."
                    }

            lines = parse_objs(record.get("lineItems", "[]"))
            for line in lines:
                item = {
                    "PROJECTID": line.get("projectId"),
                    "TRX_AMOUNT": line.get("totalPrice", line.get("amount")),
                    "ACCOUNTNAME": line.get("accountName"),
                    "ENTRYDESCRIPTION": line.get("description"),
                    "LOCATIONID": payload.get("LOCATIONID"),  # same as header level
                    "CLASSID": line.get("classId"),
                    "ACCOUNTNO": line.get("accountNumber"),
                    "VENDORID": line.get("supplierNumber"),
                }

                if line.get("supplierName") and not item.get("VENDORID"):
                    self.get_vendors()
                    item["VENDORID"] = IntacctSink.vendors[payload["VENDORNAME"]]

                class_name = line.get("className")
                if class_name and not item.get("CLASSID"):
                    self.get_classes()
                    try:
                        item["CLASSID"] = IntacctSink.classes[class_name]
                    except:
                        self.logger.info(
                            f"Skipping class due Class {class_name} does not exist. Did you mean any of these: {list(IntacctSink.classes.keys())}?"
                        )

                self.get_accounts()
                account_name = line.get("accountName")
                if (
                    account_name
                    and item.get("ACCOUNTNO") not in IntacctSink.accounts.keys()
                ):
                    item["ACCOUNTNO"] = IntacctSink.accounts.get(account_name)
                if not item.get("ACCOUNTNO"):
                    return {
                        "error": f"ERROR: ACCOUNTNAME or ACCOUNTNO not found for this tenant in item {item}. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                    }

                # departmentid is optional
                department = line.get("department")
                department_name = line.get("departmentName")
                if department or department_name:
                    self.get_departments()
                    item["DEPARTMENTID"] = IntacctSink.departments.get(
                        department
                    ) or IntacctSink.departments.get(department_name)
                payload["APBILLITEMS"]["APBILLITEM"].append(item)

                location_name = line.get("location")
                if location_name and not item["LOCATIONID"]:
                    self.get_locations()
                    try:
                        item["LOCATIONID"] = IntacctSink.locations.get(location_name)
                    except:
                        return {
                            "error": f"Location '{location_name}' does not exist. Did you mean any of these: {list(self.locations.keys())}?"
                        }
                if not item["LOCATIONID"] and payload["LOCATIONID"]:
                    item["LOCATIONID"] = payload["LOCATIONID"]

                project_name = line.get("projectName")
                if project_name and not item["PROJECTID"]:
                    self.get_projects()
                    item["PROJECTID"] = IntacctSink.projects.get(project_name)

                item_name = line.get("productName")
                if item_name:
                    self.get_items()
                    item["ITEMID"] = IntacctSink.items.get(item_name)

                # add custom fields to the item payload
                custom_fields = parse_objs(line.get("customFields", "[]"))
                if custom_fields:
                    [
                        item.update({cf.get("name"): cf.get("value")})
                        for cf in custom_fields
                    ]

            # send payload and attachments
            payload = clean_convert(payload)
            return {
                "payload": {"APBILL": payload},
                "attachments": record.get("attachments"),
            }
        except Exception as e:
            return {"error": e.__repr__()}

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record["error"])
        if record:
            payload, attachments = record.values()
            record_id = payload["APBILL"].get("RECORDID")
            # post/update attachments if exist
            supdoc_id = None
            if attachments and record_id:
                supdoc_id = self.post_attachments(attachments, record_id)
                payload["APBILL"]["SUPDOCID"] = supdoc_id
            # post/update bill
            try:
                # post/update bill
                action = "update" if payload["APBILL"].get("RECORDNO") else "create"
                response = self.request_api("POST", request_data={action: payload})
                id = response["data"]["apbill"]["RECORDNO"]
                return id, True, state_updates
            except Exception as e:
                # if bill is new and attachments were sent delete the sent attachments
                if supdoc_id and action == "create":
                    self.logger.info(
                        f"Posting PurchaseInvoice with RECORDID {record_id} has failed due to {e.__repr__}, deleting sent attachments..."
                    )
                    self.request_api(
                        "POST", request_data={"delete_supdoc": {"@key": supdoc_id}}
                    )
                raise Exception(e.__repr__())