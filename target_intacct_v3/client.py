import base64
import datetime as dt
import json
import uuid
from pathlib import Path

import backoff
import requests
import xmltodict
from pendulum import parse
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink

from target_intacct_v3.util import dictify, parse_objs


class IntacctSink(HotglueSink):
    base_url = "https://api.intacct.com/ia/xml/xmlgw.phtml"
    endpoint = ""
    vendors = None
    accounts = None
    locations = None
    projects = None
    classes = None
    departments = None
    items = None
    previous_stream = None
    controlid_list = []

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"content-type": "application/xml"}
        return headers
    
    @classmethod
    def register_controlid(cls, controlid):
        cls.controlid_list.append(controlid)

    @classmethod
    def check_request_body_duplicity(cls, controlid):
        return controlid in cls.controlid_list
    
    def get_request_body(self, sender_id, sender_password, login_payload = {}, content = {}, operation = None):
        request_body = {
                "request": {
                    "control": {
                        "senderid": sender_id,
                        "password": sender_password,
                        "controlid": None,
                        "uniqueid": False,
                        "dtdversion": 3.0,
                        "includewhitespace": False,
                    }
                }
            }
        if operation == 'login':
            request_body["request"]["operation"] = {
                        "authentication": {"login": login_payload},
                        "content": {
                            "function": {
                                "@controlid": str(uuid.uuid4()),
                                "getAPISession": None,
                            }
                        },
                    }
        elif operation == "send_content":
            request_body["request"]["operation"] = {
                    "authentication": {"sessionid": self._target.session_id},
                    "content": content,
                }
        else:
            raise Exception(f"Invalid operation given when requesting the request body: {operation}")
        
        controlid = hash(str(request_body))
        if self.check_request_body_duplicity(controlid):
            raise Exception(f"Request body duplicity identified: {request_body}")
        
        request_body["request"]["control"]["controlid"] = controlid
        self.register_controlid(controlid)

        return request_body

    def login(self):
        user_id = self.config.get("user_id")
        company_id = self.config.get("company_id")
        user_password = self.config.get("user_password")
        location_id = self.config.get("location_id")
        sender_id = self.config.get("sender_id")
        sender_password = self.config.get("sender_password")
        login_payload = {
            "userid": user_id,
            "companyid": company_id,
            "password": user_password,
        }

        # register current stream
        IntacctSink.previous_stream = self.name

        if (
            self.config.get("use_locations")
            and location_id
            and self.name not in ["Suppliers"]
        ):
            login_payload["locationid"] = location_id

        request_body = self.get_request_body(sender_id, sender_password, login_payload= login_payload, operation='login')

        xml_request_body = xmltodict.unparse(request_body).encode("utf-8")
        try:
            response = requests.post(self.base_url, headers=self.http_headers, data=xml_request_body)
            self.validate_response(response)
            res_json = self.parse_response(response)["response"]["operation"]
            if res_json["authentication"]["status"] == "success":
                session_details = res_json["result"]["data"]["api"]
                self._target.session_id = session_details["sessionid"]
                self._target.session_timeout = self._get_session_timeout(res_json)

        except requests.RequestException as e:
            raise FatalAPIError(f"Login request failed: {e.__repr__()}")
        except KeyError as e:
            raise FatalAPIError(f"Unexpected response structure: {e.__repr__()}")

    def _get_session_timeout(self, response_json) -> dt.datetime:
        """Extract session timeout from the response."""
        try:
            return parse(response_json["authentication"]["sessiontimeout"])
        except (KeyError, ValueError):
            return dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)

    def parse_response(self, response):
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        return parsed_response

    def is_session_valid(self):
        now = round(dt.datetime.now(dt.timezone.utc).timestamp())
        session_timeout = self._target.session_timeout
        if not self._target.session_id:
            return False
        if self.name != IntacctSink.previous_stream:
            return False
        if session_timeout is not None:
            session_timeout = session_timeout.timestamp()
        return not ((session_timeout - now) < 120)

    def format_payload(self, payload):
        content = {"function": {"@controlid": str(uuid.uuid4())}}
        content["function"].update(payload)

        dict_body = self.get_request_body(self.config.get("sender_id"),self.config.get("sender_password"), content= content, operation='send_content')
        # transform payload to xml
        body = xmltodict.unparse(dict_body).encode("utf-8")
        return body

    def request_api(
        self, http_method, endpoint=None, params=None, request_data=None, headers=None
    ):
        """Request records from REST endpoint(s), returning response records."""
        # check if session is still valid before sending any request
        if params is None:
            params = {}
        if headers is None:
            headers = {}

        if not self.is_session_valid():
            self.login()
        # wrap and format payload
        request_data = self.format_payload(request_data)
        # send request
        resp = self._request(http_method, endpoint, params, request_data, headers)
        return resp

    def validate_response(self, response) -> None:
        """Validate HTTP response."""
        try:
            # Parse response
            parsed_response = self.parse_response(response)

            result = parsed_response.get("response", {})

            # Check if status exists
            operation_result = result.get("operation", {}).get("result", {})
            status = operation_result.get("status", "")
            if status != "success":
                # Extract error message
                error = (
                    operation_result.get("errormessage")
                    or parsed_response.get("errormessage", parsed_response)
                )
                
                # Raise appropriate error
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    raise RetriableAPIError(error)
                else:
                    raise FatalAPIError(error)

        except (KeyError, ValueError, TypeError) as e:
            raise FatalAPIError(f"Failed to parse response: {e.__repr__()}")

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        if params is None:
            params = {}
        if headers is None:
            headers = {}

        url = self.url(endpoint)
        headers.update(self.default_headers)
        params.update(self.params)

        if "attachmentdata" not in str(request_data):
            self.logger.info(f"Making request to {url} with payload: {request_data}")

        try:
            response = requests.request(
                method=http_method,
                url=url,
                params=params,
                headers=headers,
                data=request_data,
            )
            self.validate_response(response)
            # parse response
            parsed_response = self.parse_response(response)

            # validate response
            result = parsed_response["response"]["operation"]["result"]
            self.logger.info(f"Succesful request to {url} with response: {result}")
            return result
        
        except requests.RequestException as e:
            self.logger.error(f"Request to {url} failed: {e.__repr__()}")
            raise FatalAPIError(f"HTTP request failed: {e.__repr__()}")
        except KeyError as e:
            self.logger.error(f"Failed to parse response from {url}: {e.__repr__()}")
            raise FatalAPIError(f"Malformed response: {e.__repr__()}")

    def get_records(self, intacct_object, fields, filter=None, docparid=None):
        if filter is None:
            filter = {}
 
        pagesize = 1000
        offset = 0
        total_intacct_objects = []

        while True:
            data = {
                "query": {
                    "object": intacct_object,
                    "select": {"field": fields},
                    "options": {"showprivate": "true"},
                    "pagesize": pagesize,
                    "offset": offset,
                }
            }
            if filter:
                data["query"].update(filter)

            if docparid:
                data["query"]["docparid"] = docparid

            try:
                response = self.request_api("POST", request_data=data)
                count = int(response.get("data", {}).get("@totalcount", 0))
                intacct_objects = response.get("data", {}).get(intacct_object, [])
                # When only 1 object is found, Intacct returns a dict, otherwise it returns a list of dicts.
                if isinstance(intacct_objects, dict):
                    intacct_objects = [intacct_objects]

                total_intacct_objects.extend(intacct_objects)

                if offset + pagesize >= count:
                    break

                offset += pagesize
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Failed to retrieve records: {e.__repr__()}")
                raise FatalAPIError(f"Error while fetching records: {e.__repr__()}")
        
        return total_intacct_objects

    def get_vendors(self):
        if IntacctSink.vendors is None:
            vendors = self.get_records("VENDOR", ["VENDORID", "NAME"])
            IntacctSink.vendors = dictify(vendors, "NAME", "VENDORID")
        return IntacctSink.vendors

    def get_accounts(self):
        if IntacctSink.accounts is None:
            accounts = self.get_records("GLACCOUNT", ["RECORDNO", "ACCOUNTNO", "TITLE"])
            IntacctSink.accounts = dictify(accounts, "TITLE", "ACCOUNTNO")
        return IntacctSink.accounts

    def get_projects(self):
        if IntacctSink.projects is None:
            projects = self.get_records("PROJECT", ["PROJECTID", "NAME"])
            IntacctSink.projects = dictify(projects, "NAME", "PROJECTID")
        return IntacctSink.projects

    def get_locations(self):
        if IntacctSink.locations is None:
            locations = self.get_records("LOCATION", ["LOCATIONID", "NAME", "STATUS"])
            # filter out locations with status "Inactive", not doing on the request because status filtering is not working for some reason
            locations = [location for location in locations if location.get("STATUS").lower() == "active"]
            IntacctSink.locations = dictify(locations, "NAME", "LOCATIONID")
        return IntacctSink.locations

    def get_classes(self):
        if IntacctSink.classes is None:
            classes = self.get_records("CLASS", ["CLASSID", "NAME"])
            IntacctSink.classes = dictify(classes, "NAME", "CLASSID")
        return IntacctSink.classes

    def get_departments(self):
        if IntacctSink.departments is None:
            departments = self.get_records("DEPARTMENT", ["DEPARTMENTID", "TITLE"])
            IntacctSink.departments = dictify(departments, "TITLE", "DEPARTMENTID")
        return IntacctSink.departments

    def get_customers(self):
        if IntacctSink.customers is None:
            customers = self.get_records("CUSTOMER", ["CUSTOMERID", "NAME"])
            IntacctSink.customers = dictify(customers, "NAME", "CUSTOMERID")
        return IntacctSink.customers

    def get_items(self):
        if IntacctSink.items is None:
            items = self.get_records("ITEM", ["ITEMID", "NAME"])
            IntacctSink.items = dictify(items, "NAME", "ITEMID")
        return IntacctSink.items

    def prepare_attachment_payload(
        self, attachments, supdoc_id, existing_attachments=None, folder_id=None
    ):
        if existing_attachments is None:
            existing_attachments = {"names": [], "content": []}

        if isinstance(attachments, str):
            attachments = parse_objs(attachments)

        for attachment in attachments:
            url = attachment.get("url")
            if url:
                try:
                    response = requests.get(url)
                    data = base64.b64encode(response.content)
                    data = data.decode()
                    attachment["data"] = data
                except requests.RequestException as e:
                    self.logger.error(f"Failed to fetch attachment from {url}: {e.__repr__()}")
                    continue
            else:
                try:
                    att_path = f"{self.config.get('input_path')}/{attachment.get('id')}_{attachment.get('name')}"
                    with open(att_path, "rb") as attach_file:
                        data = base64.b64encode(attach_file.read()).decode()
                        attachment["data"] = data
                except FileNotFoundError as e:
                    self.logger.error(f"File not found for attachment: {att_path}. Error: {e.__repr__()}")
                    continue
                except OSError as e:
                    self.logger.error(f"Failed to read file {att_path}. Error: {e.__repr__()}")
                    continue

        filtered_attachments = []
        for att in attachments:
            att_name = f'{att.get("id")}_{att.get("name")}' if att.get("id") else att.get("name")
            should_post = False

            if att.get("id"):
                # check if attachment content was previously posted (precoro)
                should_post = att.get("data") not in existing_attachments.get("content", [])
            else:
                # check if attachment name was previously posted
                should_post = att_name not in existing_attachments.get("names", [])

            if should_post:
                filtered_attachments.append({
                        "attachmentname": att_name,
                        "attachmenttype": Path(att_name).suffix,
                        "attachmentdata": att.get("data"),
                })
            else:
                self.logger.info(f"Skipping attachment '{att_name}' (duplicate name or content found)")

        if filtered_attachments:
            action = "create" if not existing_attachments.get("names") else "update"
            return {
                f"{action}_supdoc": {
                    "supdocid": supdoc_id,  # only 20 chars allowed
                    "supdocname": supdoc_id,
                    "supdocfoldername": folder_id,  # we use the actual record id as foldername
                    "attachments": {"attachment": filtered_attachments},
                }
            }

    def post_attachments(self, attachments, record_id):

        supdoc_id = str(record_id).replace("-","")[-20:]  # supdocid only allows 20 chars
        self.logger.info(f"Transforming record_id: {record_id} into supdoc_id: {supdoc_id}")
        # 1. check if supdoc exists and get existing attachments
        try:
            check_supdoc = {"get": {"@object": "supdoc", "@key": supdoc_id}}
            supdoc_response = self.request_api("POST", request_data=check_supdoc)
            supdoc = (supdoc_response.get("data") or {}).get("supdoc")
        except Exception as e:
            self.logger.error(f"Failed to check existing supdoc for record {supdoc_id}: {e.__repr__()}")
            return
        

        # getting existing attachments
        existing_attachments = {"names": [], "content": []}
        if supdoc:
            self.logger.info(f"Supdoc with ID {supdoc_id} already exists, updating it.")
            existing_attachments_data = supdoc.get("attachments", {}).get("attachment")
            if isinstance(existing_attachments_data, dict):
                existing_attachments["names"] = [existing_attachments_data.get("attachmentname")]
                existing_attachments["content"] = existing_attachments_data.get("attachmentdata")
            elif isinstance(existing_attachments_data, list):
                existing_attachments["names"] = [att.get("attachmentname") for att in existing_attachments_data]
                existing_attachments["content"] = [att.get("attachmentdata") for att in existing_attachments_data]

        # prepare attachments payload
        try:
            att_payload = self.prepare_attachment_payload(attachments, supdoc_id, existing_attachments, folder_id=record_id)
        except Exception as e:
            self.logger.error(f"Failed to prepare attachment payload for record {record_id}: {e.__repr__()}")
            return

        if att_payload:
            try:
                # Check if the folder exists
                check_folder = {"get": {"@object": "supdocfolder", "@key": record_id}}
                folder_response = self.request_api("POST", request_data=check_folder)
                folder_exists = folder_response.get("data", {}).get("supdocfolder")

                if folder_exists:
                    self.logger.info(f"Folder with name {record_id} already exists.")
                else:
                    # Create folder if it doesn't exist
                    folder_payload = {"create_supdocfolder": {"supdocfoldername": record_id}}
                    self.request_api("POST", request_data=folder_payload)
                    self.logger.info(f"Created folder with name {record_id}.")

                # Post the attachments
                self.request_api("POST", request_data=att_payload)
                self.logger.info(f"Attachments for record {record_id} have been posted successfully.")
                return supdoc_id
            except Exception as e:
                raise Exception(f"Failed to post attachments or folder for record {record_id}: {e.__repr__()}")

        self.logger.info(f"No new attachments to post for record {record_id}.")
        return

    def get_employee_id_by_recordno(self, recordno):
        employee = self.request_api("POST", request_data={"query": {"object": "EMPLOYEE", "select": {"field": ["EMPLOYEEID", "RECORDNO"]}, "filter": {"equalto": {"field": "RECORDNO", "value": f"{recordno}"}}}})
        if employee:
            return employee.get("data", {}).get("EMPLOYEE", {}).get("EMPLOYEEID")
        raise Exception(f"Employee with recordno {recordno} not found.")

    def get_account_no_by_account_id(self, account_id):
        account = self.request_api("POST", request_data={"query": {"object": "GLACCOUNT", "select": {"field": ["ACCOUNTNO", "RECORDNO"]}, "filter": {"equalto": {"field": "RECORDNO", "value": f"{account_id}"}}}})
        if account:
            return account.get("data", {}).get("GLACCOUNT", {}).get("ACCOUNTNO")
        raise Exception(f"Account with account_id {account_id} not found.")
    

    def get_record_url(self, object, record_id, state_updates):
        try:
            if self.config.get("output_record_url"):
                record_url = self.request_api("POST", request_data={"query": {"object": object, "select": {"field": ["RECORD_URL"]}, "filter": {"equalto": {"field": "RECORDNO", "value": f"{record_id}"}}}})
                if record_url:
                    record_url = record_url.get("data", {}).get(object, {}).get("RECORD_URL")
                    state_updates["record_url"] = record_url
        except Exception as e:
            self.logger.error(f"Failed to get record url for {object} with record_id {record_id}: {str(e)}")
        return state_updates
