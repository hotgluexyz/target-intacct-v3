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

from target_intacct_v3.util import *


class IntacctSink(HotglueSink):
    base_url = "https://api.intacct.com/ia/xml/xmlgw.phtml"
    endpoint = ""
    vendors = None
    accounts = None
    locations = None
    projects = None
    classes = None
    departments = None
    previous_stream = None

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"content-type": "application/xml"}
        return headers

    def login(self):
        user_id = self.config.get("user_id")
        company_id = self.config.get("company_id")
        user_password = self.config.get("user_password")
        location_id = self.config.get("location_id")
        sender_id = self.config.get("sender_id")
        sender_password = self.config.get("sender_password")
        login = {
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
            login["locationid"] = location_id

        timestamp = dt.datetime.now()
        dict_body = {
            "request": {
                "control": {
                    "senderid": sender_id,
                    "password": sender_password,
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {"login": login},
                    "content": {
                        "function": {
                            "@controlid": str(uuid.uuid4()),
                            "getAPISession": None,
                        }
                    },
                },
            }
        }

        body = xmltodict.unparse(dict_body).encode("utf-8")
        response = requests.post(self.base_url, headers=self.http_headers, data=body)
        self.validate_response(response)
        res_json = self.parse_response(response)["response"]["operation"]
        if res_json["authentication"]["status"] == "success":
            session_details = res_json["result"]["data"]["api"]
            self._target.session_id = session_details["sessionid"]
            try:
                session_timeout = parse(res_json["authentication"]["sessiontimeout"])
            except:
                session_timeout = dt.datetime.utcnow() + dt.timedelta(hours=1)
            self._target.session_timeout = session_timeout

    def parse_response(self, response):
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        return parsed_response

    def is_session_valid(self):
        now = round(dt.datetime.utcnow().timestamp())
        session_timeout = self._target.session_timeout
        if not self._target.session_id:
            return False
        if self.name != IntacctSink.previous_stream:
            return False
        if session_timeout is not None:
            session_timeout = session_timeout.timestamp()
        return not ((session_timeout - now) < 120)

    def format_payload(self, payload):
        timestamp = dt.datetime.now()

        content = {"function": {"@controlid": str(uuid.uuid4())}}
        content["function"].update(payload)

        dict_body = {
            "request": {
                "control": {
                    "senderid": self.config.get("sender_id"),
                    "password": self.config.get("sender_password"),
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {"sessionid": self._target.session_id},
                    "content": content,
                },
            }
        }
        # transform payload to xml
        body = xmltodict.unparse(dict_body).encode("utf-8")
        return body

    def request_api(
        self, http_method, endpoint=None, params={}, request_data=None, headers={}
    ):
        """Request records from REST endpoint(s), returning response records."""
        # check if session is still valid before sending any request
        if not self.is_session_valid():
            self.login()
        # wrap and format payload
        request_data = self.format_payload(request_data)
        # send request
        resp = self._request(http_method, endpoint, params, request_data, headers)
        return resp

    def validate_response(self, response) -> None:
        """Validate HTTP response."""
        # parse response
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        result = parsed_response["response"]
        success = result["operation"]["result"]["status"]
        if success == "failure":
            error = result.get("operation", {}).get("result", {}).get(
                "errormessage"
            ) or parsed_response.get("errormessage")
            if response.status_code in [429] or 500 <= response.status_code < 600:
                raise RetriableAPIError(error)
            else:
                raise FatalAPIError(error)

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        params.update(self.params)

        if "attachmentdata" not in str(request_data):
            self.logger.info(f"Making request with payload {request_data}")

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=request_data,
        )
        self.validate_response(response)
        # parse response
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        # validate response
        parsed_response = parsed_response["response"]["operation"]["result"]
        self.logger.info(f"Succesful request with response {parsed_response}")
        return parsed_response

    def get_records(self, intacct_object, fields, filter={}):
        pagesize = 1000
        offset = 0
        paginate = True
        count = None
        total_intacct_objects = []
        while paginate:
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

            response = self.request_api("POST", request_data=data)
            count = int(response.get("data", {}).get("@count", 0))
            intacct_objects = response.get("data", {}).get(intacct_object, [])
            # When only 1 object is found, Intacct returns a dict, otherwise it returns a list of dicts.
            if isinstance(intacct_objects, dict):
                intacct_objects = [intacct_objects]

            total_intacct_objects = total_intacct_objects + intacct_objects
            offset = offset + pagesize
            if offset > count:
                paginate = False
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
            locations = self.get_records("LOCATION", ["LOCATIONID", "NAME"])
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
        self, attachments, record_id, existing_attachments={}
    ):
        supdoc_id = str(record_id)[-20:].strip("-")  # supdocid only allows 20 chars
        if isinstance(attachments, str):
            attachments = parse_objs(attachments)

        for attachment in attachments:
            url = attachment.get("url")
            if url:
                response = requests.get(url)
                data = base64.b64encode(response.content)
                data = data.decode()
                attachment["data"] = data
            else:
                att_path = f"{self.config.get('input_path')}/{attachment.get('id')}_{attachment.get('name')}"
                with open(att_path, "rb") as attach_file:
                    data = base64.b64encode(attach_file.read()).decode()
                    attachment["data"] = data

        filtered_attachments = []
        for att in attachments:
            should_post = False
            if att.get("id"):
                att_name = f'{att.get("id")}_{att.get("name")}'
                # check if attachment content was previously posted (precoro)
                should_post = att.get("data") not in existing_attachments.get(
                    "content", []
                )
            else:
                att_name = att.get("name")
                # check if attachment name was previously posted
                should_post = att_name not in existing_attachments.get("names", [])

            if should_post:
                filtered_attachments.append(
                    {
                        "attachmentname": att_name,
                        "attachmenttype": Path(att_name).suffix,
                        "attachmentdata": att.get("data"),
                    }
                )
            else:
                self.logger.info(
                    f"Attachment '{att_name}' skipped because attachment with the same name or content was found "
                )

        if filtered_attachments:
            action = "create" if not existing_attachments else "update"
            return {
                f"{action}_supdoc": {
                    "supdocid": supdoc_id,  # only 20 chars allowed
                    "supdocname": record_id,
                    "supdocfoldername": supdoc_id,  # we name the folder the same as the supdoc for easy correlation
                    "attachments": {"attachment": filtered_attachments},
                }
            }

    def post_attachments(self, attachments, record_id):
        # 1. check if supdoc exists and get existing attachments
        check_supdoc = {"get": {"@object": "supdoc", "@key": record_id}}
        supdoc = self.request_api("POST", request_data=check_supdoc)
        supdoc = (supdoc.get("data") or {}).get("supdoc")

        # getting existing attachments
        existing_attachments = {}
        if supdoc:
            self.logger.info(
                f"supdoc with id {record_id} already exists, updating existing supdoc"
            )
            ex_attachments = supdoc.get("attachments", {}).get("attachment")
            # getting a list of existing attachments to avoid duplicates
            if isinstance(ex_attachments, dict):
                existing_attachments["names"] = [ex_attachments.get("attachmentname")]
                existing_attachments["content"] = ex_attachments.get("attachmentdata")
            elif isinstance(attachments, list):
                existing_attachments["content"] = [
                    att.get("attachmentdata") for att in ex_attachments
                ]
                existing_attachments["names"] = [
                    att.get("attachmentname") for att in ex_attachments
                ]

        # prepare attachments payload
        att_payload = self.prepare_attachment_payload(
            attachments, record_id, existing_attachments
        )

        if att_payload:
            # 1. check if folder exists, create if not
            check_folder = {"get": {"@object": "supdocfolder", "@key": record_id}}
            folder = self.request_api("POST", request_data=check_folder)

            if folder.get("data", {}).get("supdocfolder"):
                self.logger.info(f"Folder with name {record_id} already exists")
            else:
                # if folder doesn't exist create folder
                folder_payload = {
                    "create_supdocfolder": {"supdocfoldername": record_id}
                }
                self.request_api("POST", request_data=folder_payload)

            # 2. post attachments
            self.request_api("POST", request_data=att_payload)
            return record_id
