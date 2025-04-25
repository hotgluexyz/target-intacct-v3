"""IntacctV3 target sink class, which handles writing streams."""


import re

from target_intacct_v3.client import IntacctSink
from target_intacct_v3.util import *

from datetime import datetime

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
                if len(vendor_id) > 20:
                    self.logger.info(f"Truncating VENDORID due to size limit (>20 characters): {vendor_id}")
                payload["VENDORID"] = vendor_id[
                    :20
                ]  # Intact size limit on VENDORID (20 characters)

                if (payload["VENDORID"] in IntacctSink.vendors.items()):
                    return {
                            "error": f"Skipping vendor with VENDORID: {vendor_id} and NAME: {payload['NAME']} due a vendor with same VENDORID exists."
                        }
                if (payload["NAME"] in IntacctSink.vendors.keys()):
                    return {
                        "error": f"Skipping vendor with VENDORID: {vendor_id} and NAME: {payload['NAME']} due a vendor with same NAME exists."
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

    name = "APAdjustments"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            # Only want the date, not the time
            record["transactionDate"] = record.get("transactionDate", "").split("T")[0]

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
                    item["glaccountno"] = IntacctSink.accounts.get(accountlabel)

                vendorname = item.pop("vendorname", None)
                if vendorname and not item.get("vendorid"):
                    self.get_vendors()
                    try:
                        item["vendorid"] = IntacctSink.vendors[vendorname]
                    except:
                        raise Exception(
                            f"ERROR: vendorname {item['vendorname']} not found for this account."
                        )

                projectname = item.pop("projectname", None)
                if projectname and not item.get("projectid"):
                    self.get_projects()
                    try:
                        item["projectid"] = IntacctSink.projects[projectname]
                    except:
                        raise Exception(
                            f"ERROR: projectname {item['projectname']} not found for this account."
                        )

                locationname = item.pop("locationname", None)
                if locationname and not item.get("locationid"):
                    self.get_locations()
                    try:
                        item["locationid"] = IntacctSink.locations[locationname]
                    except:
                        raise Exception(
                            f"ERROR: locationname {item['locationname']} not found for this account."
                        )

                classname = item.pop("classname", None)
                if classname and not item.get("classid"):
                    self.get_classes()
                    try:
                        item["classid"] = IntacctSink.classes[classname]
                    except:
                        raise Exception(
                            f"ERROR: classname {item['classname']} not found for this account."
                        )

                departmentname = item.pop("departmentname", None)
                if departmentname and not item.get("departmentid"):
                    self.get_departments()
                    try:
                        item["departmentid"] = IntacctSink.departments[departmentname]
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
        bill_state = None
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
                "VENDORID": record.get("vendorNumber"),
                "RECORDID": record.get("externalId"),
                "RECORDNO": record.get("id"),
                "LOCATIONID": record.get("locationNumber"),
                "DOCNUMBER": record.get("transactionNumber"),
                "DESCRIPTION": record.get("description"),
            }

            # validate RECORDID
            if payload.get("RECORDID"):
                invalid_chars = r"[\"\'&<>#?]"  # characters not allowed for RECORDID [&, <, >, #, ?]
                is_id_valid = not bool(re.search(invalid_chars, (payload.get("RECORDID"))))

                if not is_id_valid:
                    raise Exception(
                        f"RECORDID '{payload.get('RECORDID')}' contains one or more invalid characters '&,<,>,#,?'. Please provide a RECORDID that does not include these characters."
                    )

            # lookup if the bill already exists if RECORDNO is not provided
            if payload.get("RECORDID") and not payload.get("RECORDNO"):
                existing_bill = self.get_records(
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
                if existing_bill:
                    payload["RECORDNO"] = existing_bill[0].get("RECORDNO")
                    bill_state = existing_bill[0].get("STATE")

            # include locationid at header level
            location_id = record.get("locationId")
            if location_id and not payload.get("LOCATIONID"):
                self.get_locations()
                try:
                    payload["LOCATIONID"] = IntacctSink.locations_by_id[location_id]
                except:
                    return {
                        "error": f"ERROR: Location id '{location_id}' does not exist."
                    }

            locationname = record.get("locationName")
            if locationname and not payload.get("LOCATIONID"):
                self.get_locations()
                try:
                    payload["LOCATIONID"] = IntacctSink.locations[locationname]
                except:
                    return {
                        "error": f"ERROR: Location '{locationname}' does not exist. Did you mean any of these: {list(IntacctSink.locations.keys())}?"
                    }

            vendorid = record.get("vendorId")
            if vendorid and not payload.get("VENDORID"):
                self.get_vendors()
                try:
                    payload["VENDORID"] = IntacctSink.vendors_by_id[vendorid]
                except:
                    return {
                        "error": f"ERROR: Vendor id '{vendorid}' does not exist."
                    }

            # lookup for vendorName
            vendorname = record.get("vendorName")
            if vendorname and not payload.get("VENDORID"):
                self.get_vendors()
                try:
                    payload["VENDORID"] = IntacctSink.vendors[vendorname]
                except:
                    return {
                        "error": f"ERROR: Vendor {vendorname} does not exist. Did you mean any of these: {list(IntacctSink.vendors.keys())}?"
                    }

            if bill_state == "Paid":
                self.logger.info("Bill is already paid. Skipping the line items.")
            else:
                lines = parse_objs(record.get("lineItems", "[]"))
                expenses = parse_objs(record.get("expenses", "[]"))

                for line in lines + expenses:
                    item = {
                        "PROJECTID": line.get("projectId"),
                        "TRX_AMOUNT": line.get("amount"),
                        "ACCOUNTNAME": line.get("accountName"),
                        "ENTRYDESCRIPTION": line.get("description"),
                        "LOCATIONID": line.get("locationNumber"),
                        "CLASSID": line.get("classNumber"),
                        "ACCOUNTNO": line.get("accountNumber"),
                        "VENDORID": line.get("vendorNumber"),
                        "DEPARTMENTID": line.get("departmentNumber"),
                        "ITEMID": line.get("itemId"),
                        "TASKID": line.get("taskNumber"),
                    }

                    if line.get("vendorId") and not item.get("VENDORID"):
                        self.get_vendors()
                        try:
                            item["VENDORID"] = IntacctSink.vendors_by_id[line["vendorId"]]
                        except:
                            return {
                                "error": f"ERROR: Vendor id '{line['vendorId']}' does not exist."
                            }

                    if line.get("vendorName") and not item.get("VENDORID"):
                        self.get_vendors()
                        item["VENDORID"] = IntacctSink.vendors[line["vendorName"]]

                    if line.get("classId") and not item.get("CLASSID"):
                        self.get_classes()
                        try:
                            item["CLASSID"] = IntacctSink.classes_by_id[line["classId"]]
                        except:
                            return {
                                "error": f"ERROR: Class id '{line['classId']}' does not exist."
                            }

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

                    account_id = line.get("accountId")
                    if account_id and item.get("ACCOUNTNO") not in IntacctSink.accounts.values():
                        item["ACCOUNTNO"] = IntacctSink.accounts_by_id.get(account_id)

                    account_name = line.get("accountName")
                    if (
                        account_name
                        and item.get("ACCOUNTNO") not in IntacctSink.accounts.values()
                    ):
                        item["ACCOUNTNO"] = IntacctSink.accounts.get(account_name)
                    if not item.get("ACCOUNTNO"):
                        return {
                            "error": f"ERROR: ACCOUNTNAME or ACCOUNTNO not found for this tenant in item {item}. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                        }

                    department_id = line.get("departmentId")
                    if department_id and not item.get("DEPARTMENTID"):
                        self.get_departments()
                        item["DEPARTMENTID"] = IntacctSink.departments_by_id.get(department_id)

                    department_name = line.get("departmentName")
                    # if no departmentId set, lookup based on departmentName
                    if department_name and not item.get("DEPARTMENTID"):
                        self.get_departments()
                        item["DEPARTMENTID"] = IntacctSink.departments.get(
                            department_name
                        )

                    location_id = line.get("locationId")
                    if location_id and not item.get("LOCATIONID"):
                        self.get_locations()
                        try:
                            item["LOCATIONID"] = IntacctSink.locations_by_id.get(location_id)
                        except:
                            return {
                                "error": f"ERROR: Location id '{location_id}' does not exist."
                            }

                    location_name = line.get("locationName")
                    if location_name and not item["LOCATIONID"]:
                        self.get_locations()
                        try:
                            item["LOCATIONID"] = IntacctSink.locations.get(location_name)
                        except:
                            return {
                                "error": f"Location '{location_name}' does not exist. Did you mean any of these: {list(self.locations.keys())}?"
                            }

                    # if no item level locationId set, fall back to header level
                    if not item["LOCATIONID"] and payload["LOCATIONID"]:
                        item["LOCATIONID"] = payload["LOCATIONID"]

                    project_id = line.get("projectId")
                    if project_id and not item["PROJECTID"]:
                        self.get_projects()
                        item["PROJECTID"] = IntacctSink.projects_by_id.get(project_id)

                    project_name = line.get("projectName")
                    if project_name and not item["PROJECTID"]:
                        self.get_projects()
                        item["PROJECTID"] = IntacctSink.projects.get(project_name)

                    item_id = line.get("itemId")
                    if item_id and not item["ITEMID"]:
                        self.get_items()
                        item["ITEMID"] = IntacctSink.items_by_id.get(item_id)

                    item_name = line.get("itemName")
                    if item_name and not item["ITEMID"]:
                        self.get_items()
                        item["ITEMID"] = IntacctSink.items.get(item_name)

                    # lookup if the bill already exists if RECORDNO is not provided
                    employee_id = line.get("employeeId")
                    if employee_id:
                        employee = self.get_records(
                            "employees",
                            fields=["RECORDNO", "EMPLOYEEID"],
                            filter={
                                "filter": {
                                    "equalto": {
                                        "field": "RECORDNO",
                                        "value": employee_id,
                                    }
                                }
                            },
                        )
                        if employee:
                            item["EMPLOYEEID"] = employee[0].get("EMPLOYEEID")

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
        state_updates = {}
        if not record:
            raise Exception("Received an empty record, skipping.")

        if "error" in record:
            raise Exception(f"Record error: {record['error']}")

        payload, attachments = record.values()
        record_id = payload.get("APBILL",{}).get("RECORDID","")
        # post/update attachments if exist
        supdoc_id = None
        if attachments:
            if not record_id:
                self.logger.error("No RECORDID found in the payload. Skipping sendind attachments as no pk was found to create the folder and/or supdoc.")
            try:
                supdoc_id = self.post_attachments(attachments, record_id)
                payload["APBILL"]["SUPDOCID"] = supdoc_id
            except Exception as e:
                self.logger.error(f"Failed to post attachments for RECORDID {record_id}: {e}")
                raise

        # post/update bill
        try:
            # post/update bill
            action = "update" if payload["APBILL"].get("RECORDNO") else "create"
            response = self.request_api("POST", request_data={action: payload})
            bill_id = response["data"]["apbill"]["RECORDNO"]

            self.logger.info(f"Successfully {action}d bill with RECORDNO {bill_id}")
            return bill_id, True, state_updates
        except Exception as e:
            self.logger.error(f"Failed to {action} bill with RECORDID {record_id}: {e}")

            # if bill is new and attachments were sent delete the sent attachments
            if supdoc_id and action == "create":
                try:
                    self.logger.info(
                        f"Deleting attachments for failed bill creation with RECORDID {record_id}..."
                    )
                    self.request_api("POST", request_data={"delete_supdoc": {"@key": supdoc_id}})
                except Exception as delete_error:
                    self.logger.error(f"Failed to delete attachments with SUPDOCID {supdoc_id}: {delete_error}")
            raise Exception(f"Failed to {action} bill: {e}")


class BillPayment(IntacctSink):
    """IntacctV3 target sink class."""

    name = "BillPayment"


    def preprocess_record(self, record: dict, context: dict) -> dict:
        if not record.get("billId"):
            return {"error": "billId is a required field"}

        # Get the bill with the id
        bills = self.get_records("APBILL", [
            "RECORDNO",
            "VENDORNAME",
            "VENDORID",
            "RECORDID",
            "DOCNUMBER",
            "CURRENCY",
            "TRX_TOTALDUE",
        ], filter={"filter": {"equalto": {"field": "RECORDNO", "value": f"{record['billId']}"}}})

        if not bills:
            raise Exception(f"No bill with id={record['billId']} found.")

        # get the bill
        bill = bills[0]

        # If no payment date is set, we fall back to today
        payment_date = record.get("paymentDate")

        if payment_date is None:
            payment_date = datetime.today().strftime("%m/%d/%Y")

        if not record.get("bankAccountName"):
            return {"error": "bankAccountName is a required field"}

        bank_name = record["bankAccountName"]
        # TODO: not sure why we need this
        if "--" in bank_name:
            bank_name = bank_name.split("--")[0]

        if not record.get("paymentMethod"):
            return {"error": "paymentMethod is a required field"}

        payload = {
            "FINANCIALENTITY": bank_name,
            "PAYMENTMETHOD": record["paymentMethod"],
            "VENDORID": record.get("vendorId") or bill["VENDORID"],
            "CURRENCY": record.get("currency") or bill["CURRENCY"],
            "PAYMENTDATE": payment_date,
            "APPYMTDETAILS": {
                "APPYMTDETAIL": {
                    "RECORDKEY": bill["RECORDNO"],
                    "TRX_PAYMENTAMOUNT": record.get("amount") or bill["TRX_TOTALDUE"],
                }
            },
        }

        return {"APPYMT": payload}


    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record["error"])

        if record:
            response = self.request_api("POST", request_data={"create": record})
            id = response["data"]["appymt"]["RECORDNO"]
            return id, True, state_updates

class PurchaseOrders(IntacctSink):
    """IntacctV3 target sink class."""

    name = "PurchaseOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            # Map purchase order
            payload = {
                "transactiontype": "Purchase Order",
                "RECORDNO": record.get("id"),
                "datecreated": record.get("transactionDate"),
                "vendorid": record.get("vendorId"),
                "documentno": record.get("number"),
                "referenceno": record.get("referenceNumber"),
                "termname": record.get("paymentTerm"),
                "datedue": record.get("dueDate"),
                "message": record.get("description"),
                "returnto": {"contactname": None},
                "payto": {"contactname": None},
                "basecurr": record.get("currency"),
                "currency": record.get("currency"),
                "exchratetype": "Intacct Daily Rate"
            }

            existing_order = None
            if payload.get("RECORDNO"):
                recordno = payload.get("RECORDNO")

                # validate RECORDNO
                invalid_chars = r"[\"\'&<>#?]"  # characters not allowed for RECORDNO [&, <, >, #, ?]
                is_id_valid = not bool(re.search(invalid_chars, recordno))

                if not is_id_valid:
                    raise Exception(
                        f"RECORDNO '{payload.get('RECORDNO')}' contains one or more invalid characters '&,<,>,#,?'. Please provide a RECORDNO that does not include these characters."
                    )

                # check if bill exists
                existing_order = self.get_records(
                    "PODOCUMENT",
                    fields=["RECORDNO", "DOCNO"],
                    filter={
                        "filter": {
                            "equalto": {
                                "field": "RECORDNO",
                                "value": recordno,
                            }
                        }
                    },
                    docparid="Purchase Order"
                )
            
            existing_order_lines = None
            if existing_order:
                payload["@key"] = f"Purchase Order-{existing_order[0]['DOCNO']}"

                existing_order_lines = self.get_records(
                    "PODOCUMENTENTRY",
                    fields=["RECORDNO"],
                    filter={
                        "filter": {
                            "equalto": {
                                "field": "DOCHDRNO",
                                "value": recordno,
                            }
                        }
                    },
                    docparid="Purchase Order"
                )

            # look for vendorName and vendorId
            vendor_name = record.get("vendorName")
            if vendor_name and not payload.get("vendorid"):
                self.get_vendors()
                try:
                    payload["vendorid"] = IntacctSink.vendors[vendor_name]
                except:
                    return {
                        "error": f"ERROR: Vendor {vendor_name} does not exist. Did you mean any of these: {list(IntacctSink.vendors.keys())}?"
                    }

            if payload.get("datecreated"):
                payload["datecreated"] = convert_date(payload.get("datecreated"))

            if payload.get("datedue"):
                payload["datedue"] = convert_date(payload.get("datedue"))

            # process items
            po_items = []
            for item in record.get("lineItems", []):
                item_payload = {
                    "itemid": item.get("productId"),
                    "quantity": item.get("quantity"),
                    "unit": "Each",
                    "price": item.get("unitPrice"),
                    "tax": item.get("taxAmount"),
                    "locationid": item.get("locationId"),
                    "departmentid": item.get("departmentId"),
                    "memo": item.get("description"),
                    "projectid": item.get("projectId"),
                    "employeeid": item.get("employeeId"),
                    "classid": item.get("classId")
                }

                project_name = item.pop("projectName", None)
                if project_name and not item_payload.get("projectid"):
                    self.get_projects()
                    try:
                        item_payload["projectid"] = IntacctSink.projects[project_name]
                    except:
                        raise Exception(
                            f"ERROR: projectname {project_name} not found for this account."
                        )

                location_name = item.pop("locationName", None)
                if location_name and not item_payload.get("locationid"):
                    self.get_locations()
                    try:
                        item_payload["locationid"] = IntacctSink.locations[location_name]
                    except:
                        raise Exception(
                            f"ERROR: locationname {location_name} not found for this account."
                        )

                class_name = item.pop("className", None)
                if class_name and not item_payload.get("classid"):
                    self.get_classes()
                    try:
                        item_payload["classid"] = IntacctSink.classes[class_name]
                    except:
                        raise Exception(
                            f"ERROR: classname {class_name} not found for this account."
                        )

                department_name = item.pop("departmentName", None)
                if department_name and not item_payload.get("departmentid"):
                    self.get_departments()
                    try:
                        item_payload["departmentid"] = IntacctSink.departments[department_name]
                    except:
                        raise Exception(
                            f"ERROR: departmentname {department_name} not found for this account."
                        )

                po_items.append(item_payload)

            # if it's an update
            if payload.get("@key"):
                fields_remove = ["vendorid", "transactiontype", "documentno"]
                for field in fields_remove:
                    payload.pop(field, None)

                payload["updatepotransitems"] = {"potransitem": po_items}
                if existing_order_lines:
                    # delete existing lines
                    payload["updatepotransitems"]["updatepotransitem"] = [{"@line_num": n, "itemid": None} for n in range(1, len(existing_order_lines)+1)] 
            else:
                payload["potransitems"] = {"potransitem": po_items}

            return payload
        except Exception as e:
            return {"error": e.__repr__()}

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = {}

        if not record:
            raise Exception("Received an empty record, skipping.")

        if "error" in record:
            raise Exception(f"Record error: {record['error']}")

        record_id = record.pop("RECORDNO", record.get("documentno"))

        # post/update record
        try:
            action = "update_potransaction" if record.get("@key") else "create_potransaction"
            response = self.request_api("POST", request_data={action: record})
            po_key = response["key"]

            order = self.get_records(
                "PODOCUMENT",
                fields=["RECORDNO"],
                filter={
                    "filter": {
                        "equalto": {
                            "field": "DOCNO",
                            "value": po_key.split("-")[1],
                        }
                    }
                },
                docparid="Purchase Order"
            )
            po_id = order[0]["RECORDNO"]
            if action == "update_potransaction":
                state_updates["is_updated"] = True

            # Step 3: Log success and return the PO ID, success status, and state updates
            self.logger.info(f"Successfully {action}d Purchase Order with id {po_id}")
            return po_id, True, state_updates
        except Exception as e:
            self.logger.error(f"Failed to {action} Purchase Order with ID {record_id}: {e}")
            raise Exception(f"Failed to {action} Purchase Order: {e}")
