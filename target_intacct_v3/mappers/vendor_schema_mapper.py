from typing import Dict
from target_intacct_v3.mappers.base_mapper import BaseMapper

class VendorSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "intacct_field": "RECORDNO", "required_if_present": True},
        {"record_field": "vendorNumber", "intacct_field": "VENDORID", "required_if_present": False},
        {"record_field": "vendorName", "intacct_field": "NAME", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "vendorNumber": "VENDORID",
        "vendorName": "NAME",
        "currency": "CURRENCY"
    }

    def to_intacct(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_contact_info(),
            **self._map_is_active()
        }

        
        self._map_fields(payload)

        return payload
    
    def _map_is_active(self):
        is_active = self.record.get("isActive")
        if is_active is not None:
            return {"STATUS": "active" if is_active else "inactive"}
        return {}

    def _map_contact_info(self):
        contact_info = {}
        contact_field_mappings = {
            "firstName": "FIRSTNAME",
            "lastName": "LASTNAME",
            "email": "EMAIL1",
            "website": "URL1",
            "checkName": "PRINTAS"
        }

        self._map_fields(contact_info, custom_field_mappings=contact_field_mappings)

        phone_numbers = self.record.get("phoneNumbers", [])
        for phone_number in phone_numbers:
            if phone_number.get("type") == "primary":
                contact_info["PHONE1"] = phone_number.get("phoneNumber")
            elif phone_number.get("type") == "mobile":
                contact_info["CELLPHONE"] = phone_number.get("phoneNumber")
            elif phone_number.get("type") == "fax":
                contact_info["FAX"] = phone_number.get("phoneNumber")

        addresses = self.record.get("addresses", [])
        for address in addresses:
            if address.get("addressType") == "billing":
                contact_info["MAILADDRESS"] = {
                    "ADDRESS1": address.get("line1"),
                    "ADDRESS2": address.get("line2"),
                    "ADDRESS3": address.get("line3"),
                    "CITY": address.get("city"),
                    "STATE": address.get("state"),
                    "ZIP": address.get("postalCode"),
                    "COUNTRY": address.get("country")
                }

        return {"DISPLAYCONTACT": contact_info} if contact_info else {}
