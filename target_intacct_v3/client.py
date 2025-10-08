import datetime as dt
import json
from logging import Logger

import backoff
import requests
import xmltodict
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class IntacctClient:
    base_url = "https://api.intacct.com/ia/xml/xmlgw.phtml"
    endpoint = ""

    def __init__(self, config: dict, logger: Logger):
        self._current_location_id = None
        self.logger = logger
        self.config = config
        self.session_id = None
        self.session_timeout_timestamp = None
        self.login()


    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"content-type": "application/xml"}
        return headers

    def invalidate_session(self):
        self.session_id = None
        self.session_timeout_timestamp = None

    @property
    def current_location_id(self):
        return self._current_location_id

    @current_location_id.setter
    def current_location_id(self, new_value):
        """The setter for the '_current_location_id' property and invalidate the session if necessary."""
        if new_value == "TOP_LEVEL":
            new_value = None

        if new_value != self._current_location_id:
            self.invalidate_session()
            self._current_location_id = new_value
    
    def get_login_request_body(self):
        user_id = self.config.get("user_id")
        company_id = self.config.get("company_id")
        user_password = self.config.get("user_password")
        sender_id = self.config.get("sender_id")
        sender_password = self.config.get("sender_password")

        login_payload = {
            "userid": user_id,
            "companyid": company_id,
            "password": user_password,
        }

        self.logger.info(f"Current location id: {self.current_location_id if self.current_location_id else 'TOP_LEVEL'}")
        if (
            self.current_location_id
        ):
            login_payload["locationid"] = self.current_location_id

        request_body = {
            "request": {
                "control": {
                    "senderid": sender_id,
                    "password": sender_password,
                    "controlid": str(dt.datetime.now(dt.timezone.utc).timestamp()),
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {"login": login_payload},
                    "content": {
                        "function": {
                            "@controlid": str(dt.datetime.now(dt.timezone.utc).timestamp()),
                            "getAPISession": None,
                        }
                    },
                }
            }
        }
        
        return request_body

    def get_request_body(self, content = {}):
        sender_id = self.config.get("sender_id")
        sender_password = self.config.get("sender_password")
        request_body = {
            "request": {
                "control": {
                    "senderid": sender_id,
                    "password": sender_password,
                    "controlid": str(dt.datetime.now(dt.timezone.utc).timestamp()),
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                }
            }
        }

        request_body["request"]["operation"] = {
            "authentication": {"sessionid": self.session_id},
            "content": content,
        }

        return request_body

    def login(self):
        request_body = self.get_login_request_body()

        xml_request_body = xmltodict.unparse(request_body).encode("utf-8")
        try:
            response = requests.post(self.base_url, headers=self.http_headers, data=xml_request_body)
            self.validate_response(response)
            res_json = self.parse_response(response)["response"]["operation"]
            if res_json["authentication"]["status"] == "success":
                session_details = res_json["result"]["data"]["api"]
                self.session_id = session_details["sessionid"]
                self.session_timeout_timestamp = self._get_session_timeout(res_json)

        except requests.RequestException as e:
            raise FatalAPIError(f"Login request failed: {e.__repr__()}")
        except KeyError as e:
            raise FatalAPIError(f"Unexpected response structure: {e.__repr__()}")

    def _get_session_timeout(self, response_json) -> dt.datetime:
        """Extract session timeout from the response."""
        try:
            return round(dt.datetime.fromisoformat(response_json["authentication"]["sessiontimeout"]).timestamp())
        except (KeyError, ValueError):
            return round((dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).timestamp())

    def parse_response(self, response):
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        return parsed_response

    def is_session_valid(self):
        now = round(dt.datetime.now(dt.timezone.utc).timestamp())
        session_timeout_timestamp = self.session_timeout_timestamp
        if not self.session_id:
            return False

        return not ((session_timeout_timestamp - now) < 120)

    def format_payload(self, payload):
        content = {"function": {"@controlid": str(dt.datetime.now(dt.timezone.utc).timestamp())}}
        content["function"].update(payload)

        dict_body = self.get_request_body(content=content)
        # transform payload to xml
        body = xmltodict.unparse(dict_body).encode("utf-8")
        return body

    def request_api(
        self, request_data
    ):
        """Request records from XML endpoint(s), returning response records."""
        # check if session is still valid before sending any request
        if not self.is_session_valid():
            self.login()

        # wrap and format payload
        xml_request_data = self.format_payload(request_data)
        # send request
        response = self._request(xml_request_data)

        try:
            self.validate_response(response)
            # parse response
            parsed_response = self.parse_response(response)
        except KeyError as e:
            self.logger.error(f"Failed to parse response: {response.text} {e.__repr__()}")
            raise FatalAPIError(f"Malformed response: {response.text} {e.__repr__()}")

        # validate response
        result = parsed_response["response"]["operation"]["result"]
        self.logger.info(f"Succesful request with response: {result}")

        return result

    def make_batch_request(self, request_data):
        # check if session is still valid before sending any request
        if not self.is_session_valid():
            self.login()
        
        dict_body = self.get_request_body(content=request_data)
        
        # transform payload to xml
        xml_request_data = xmltodict.unparse(dict_body).encode("utf-8")
        response = self._request(xml_request_data)
        
        # Parse response
        parsed_response = self.parse_response(response)

        json_response = parsed_response.get("response", {})
        results = json_response.get("operation", {}).get("result", {})
        if isinstance(results, dict):
            results = [results]

        return results


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
        self, request_data
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        http_method = "POST"
        headers = self.http_headers
        url = self.base_url

        if "attachmentdata" not in str(request_data):
            self.logger.info(f"Making request to {url} with payload: {request_data}")

        try:
            response = requests.request(
                method=http_method,
                url=url,
                headers=headers,
                data=request_data,
            )
            return response
        
        except requests.RequestException as e:
            self.logger.error(f"Request to {url} failed: {e.__repr__()}")
            raise FatalAPIError(f"HTTP request failed: {e.__repr__()}")

    def format_filter_in(self, field_name, values):
        return {
            "in": {
                "field": field_name,
                "value": values,
            }
        }

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
                data["query"]["filter"] = filter

            if docparid:
                data["query"]["docparid"] = docparid

            try:
                response = self.request_api(data)
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

    # def get_vendors(self):
    #     if IntacctSink.vendors is None:
    #         vendors = self.get_records("VENDOR", ["VENDORID", "NAME"])
    #         IntacctSink.vendors = dictify(vendors, "NAME", "VENDORID")
    #     return IntacctSink.vendors
