"""IntacctV3 target class."""
import os
import json
from pendulum import parse
from datetime import datetime
from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_intacct_v3.client import IntacctClient
from target_intacct_v3.sinks.vendor_sink import VendorSink
from target_intacct_v3.sinks.item_sink import ItemSink
from target_intacct_v3.sinks.bill_sink import BillSink
from target_intacct_v3.sinks.bill_payment_sink import BillPaymentSink
from target_intacct_v3.sinks.vendor_credit_sink import VendorCreditSink


class TargetIntacctV3(TargetHotglue):
    """Sample target for IntacctV3."""

    name = "target-intacct-v3"
    session_timeout = None
    session_id = None
    config_jsonschema = th.PropertiesList(
        th.Property(
            "company_id",
            th.StringType,
        ),
        th.Property(
            "sender_id",
            th.StringType,
        ),
        th.Property(
            "sender_password",
            th.StringType,
        ),
        th.Property(
            "user_id",
            th.StringType,
        ),
        th.Property(
            "user_password",
            th.StringType,
        ),
        th.Property(
            "use_locations",
            th.BooleanType,
        ),
    ).to_dict()
    SINK_TYPES = [VendorSink, ItemSink, BillSink, BillPaymentSink, VendorCreditSink]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intacct_client: IntacctClient = IntacctClient(self.config, self.logger)
        self.reference_data = self.get_reference_data()

    def get_reference_data(self):
        self.logger.info(f"Getting reference data...")

        if self.config.get("snapshot_hours"):
            try:
                with open(f'{self.config.get("snapshot_dir", "snapshots")}/reference_data.json') as json_file:
                    reference_data = json.load(json_file)
                    if reference_data.get("write_date"):
                        last_run = parse(reference_data["write_date"])
                        last_run = last_run.replace(tzinfo=None)
                        if (datetime.utcnow()-last_run).total_hours()<int(self.config.get("snapshot_hours")):
                            return reference_data
            except:
                self.logger.info(f"Snapshot not found or not readable.")

        reference_data = {}
        reference_data["Accounts"] = self.intacct_client.get_records("GLACCOUNT")
        reference_data["Subsidiaries"] = self.intacct_client.get_records("LOCATIONENTITY")
        reference_data["Classes"] = self.intacct_client.get_records("CLASS")
        reference_data["Departments"] = self.intacct_client.get_records("DEPARTMENT")
        reference_data["Projects"] = self.intacct_client.get_records("PROJECT")
        reference_data["Locations"] = self.intacct_client.get_records("LOCATION")
        reference_data["CheckingAccounts"] = self.intacct_client.get_records("CHECKINGACCOUNT")
        reference_data["SavingsAccounts"] = self.intacct_client.get_records("SAVINGSACCOUNT")
        reference_data["CreditCards"] = self.intacct_client.get_records("CREDITCARD")


        if self.config.get("snapshot_hours"):
            reference_data["write_date"] = datetime.utcnow().isoformat()
            os.makedirs("snapshots", exist_ok=True)
            with open('snapshots/reference_data.json', 'w') as outfile:
                json.dump(reference_data, outfile)

        self.logger.info(f"Done getting reference data...")
        return reference_data

if __name__ == "__main__":
    TargetIntacctV3.cli()
