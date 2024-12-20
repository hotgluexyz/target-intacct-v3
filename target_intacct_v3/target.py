"""IntacctV3 target class."""
from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_intacct_v3.sinks import (
    APAdjustments,
    Bills,
    JournalEntries,
    PurchaseInvoices,
    Suppliers,
    BillPayment,
)


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
    SINK_TYPES = [Suppliers, APAdjustments, JournalEntries, Bills, PurchaseInvoices, BillPayment]


if __name__ == "__main__":
    TargetIntacctV3.cli()
