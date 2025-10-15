import datetime
from typing import Dict
from target_intacct_v3.mappers.base_mapper import BaseMapper, InvalidInputError


class BillPaymentSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "intacct_field": "RECORDNO", "required_if_present": True},
        {"record_field": "transactionNumber", "intacct_field": "DOCNUMBER", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "transactionNumber": "DOCNUMBER",
        "paymentDate": "PAYMENTDATE",
        "exchangeRate": "EXCHANGE_RATE"
    }

    def to_intacct(self) -> Dict:
        if not self.record.get("paymentDate"):
            self.record["paymentDate"] = datetime.datetime.now(datetime.timezone.utc)
        
        bill = self._find_entity("Bills", record_no_field="billId", record_id_field="billNumber", subsidiary_id=self.subsidiary_id)
        vendor = self._find_entity("Vendors", record_no_field="vendorId", record_id_field="vendorNumber", record_name_field="vendorName", subsidiary_id=self.subsidiary_id, required=False)
        
        if vendor:
            vendor_id = vendor["ENTITYID"]
        else:
            vendor_id = bill["VENDORID"]

        payload = {
            **self._map_internal_id(),
            **self._map_subsidiary(),
            **self._map_bank_account(),
            "VENDORID": vendor_id,
            "CURRENCY": self.record.get("currency") or bill["CURRENCY"],
            **self._map_payment_method(),
            **self._map_details(bill["RECORDNO"]),       
        }
     
        self._map_fields(payload)

        return payload

    def _map_bank_account(self):
        if not self.record.get("accountId") and not self.record.get("accountName"):
            raise InvalidInputError(f"accountId/accountName is required for bill payment")

        found_account  = self._find_entity("CheckingAccounts", record_no_field="accountId", record_id_field="accountName", subsidiary_id=self.subsidiary_id, required=False, required_if_present=False)

        if not found_account:
            found_account = self._find_entity("SavingsAccounts", record_no_field="accountId", record_id_field="accountName", subsidiary_id=self.subsidiary_id, required=False, required_if_present=False)

        if not found_account:
            found_account = self._find_entity("CreditCards", record_no_field="accountId", record_id_field="accountName", subsidiary_id=self.subsidiary_id, required=False, required_if_present=False)

        if not found_account:
            raise InvalidInputError(f"Could not find a bank account / credit card in Intacct with accountId={self.record.get('accountId')}/accountName={self.record.get('accountName')}")

        return {"FINANCIALENTITY": found_account["ENTITYID"]}

    def _map_payment_method(self):
        if not self.record.get("paymentMethod"):
            raise InvalidInputError(f"Payment method is required for bill payment")
        return {"PAYMENTMETHOD": self.record.get("paymentMethod")}

    def _map_details(self, bill_recordno):
        if not self.record.get("amount"):
            raise InvalidInputError(f"Amount is required for bill payment")
        
        return {
            "APPYMTDETAILS": {
                "APPYMTDETAIL": {
                    "RECORDKEY": bill_recordno,
                    "TRX_PAYMENTAMOUNT": self.record.get("amount")
                }
            } 
        }