"""Unit tests for Customers and Invoices sink preprocessing."""

from target_intacct_v3.client import IntacctSink
from target_intacct_v3.sinks import Customers, Invoices
from target_intacct_v3.target import TargetIntacctV3

SAMPLE_CONFIG = {
    "company_id": "test",
    "sender_id": "test",
    "sender_password": "test",
    "user_id": "test",
    "user_password": "test",
}


def _customers_sink() -> Customers:
    target = TargetIntacctV3(config=SAMPLE_CONFIG, validate_config=False)
    return target.get_sink("Customers", schema={"type": "object", "properties": {}})


def _invoices_sink() -> Invoices:
    target = TargetIntacctV3(config=SAMPLE_CONFIG, validate_config=False)
    return target.get_sink("Invoices", schema={"type": "object", "properties": {}})


def setup_function():
    IntacctSink.customers = {"Acme Manufacturing": "A-000001"}
    IntacctSink.customers_by_id = {"A-000001": "Acme Manufacturing"}
    IntacctSink.accounts = {"Revenue": "4000"}
    IntacctSink.locations = None
    IntacctSink.classes = None
    IntacctSink.departments = None
    IntacctSink.items = None


def test_customers_preprocess_maps_billing_address():
    sink = _customers_sink()
    result = sink.preprocess_record(
        {
            "customerNumber": "A-000001",
            "customerName": "Sage Intacct Demo — Acme Manufacturing",
            "email": "billing+acme@intacct-demo.example.com",
            "currency": "USD",
            "addresses": [
                {
                    "addressType": "billing",
                    "line1": "100 Main St",
                    "city": "San Jose",
                    "state": "CA",
                    "postalCode": "95110",
                    "country": "United States of America",
                }
            ],
        },
        {},
    )

    customer = result["CUSTOMER"]
    assert customer["CUSTOMERID"] == "A-000001"
    assert customer["NAME"] == "Sage Intacct Demo — Acme Manufacturing"
    assert customer["CURRENCY"] == "USD"
    assert customer["DISPLAYCONTACT"]["EMAIL1"] == "billing+acme@intacct-demo.example.com"
    assert customer["DISPLAYCONTACT"]["MAILADDRESS"]["ADDRESS1"] == "100 Main St"


def test_invoices_preprocess_maps_header_and_line(monkeypatch):
    sink = _invoices_sink()
    monkeypatch.setattr(sink, "get_records", lambda *args, **kwargs: [])
    result = sink.preprocess_record(
        {
            "invoiceNumber": "I-000001",
            "customerNumber": "A-000001",
            "issueDate": "2026-08-24",
            "dueDate": "2026-09-23",
            "currency": "USD",
            "description": "Younium demo invoice",
            "lineItems": [
                {
                    "description": "Intacct Demo Subscription",
                    "amount": 100.0,
                    "accountNumber": "4000",
                }
            ],
        },
        {},
    )

    invoice = result["payload"]["ARINVOICE"]
    assert invoice["RECORDID"] == "I-000001"
    assert invoice["CUSTOMERID"] == "A-000001"
    assert invoice["WHENCREATED"] == "2026-08-24"
    assert invoice["WHENPOSTED"] == "2026-08-24"
    assert invoice["WHENDUE"] == "2026-09-23"
    assert invoice["BASECURR"] == "USD"
    line = invoice["ARINVOICEITEMS"]["arinvoiceitem"][0]
    assert line["ACCOUNTNO"] == "4000"
    assert line["TRX_AMOUNT"] == 100.0
    assert line["ENTRYDESCRIPTION"] == "Intacct Demo Subscription"


def test_invoices_whencreated_prefers_created_at(monkeypatch):
    sink = _invoices_sink()
    monkeypatch.setattr(sink, "get_records", lambda *args, **kwargs: [])
    result = sink.preprocess_record(
        {
            "invoiceNumber": "I-000001",
            "customerNumber": "A-000001",
            "createdAt": "2026-08-20T12:00:00Z",
            "issueDate": "2026-08-24",
            "dueDate": "2026-09-23",
            "currency": "USD",
            "lineItems": [{"description": "Line", "amount": 10.0, "accountNumber": "4000"}],
        },
        {},
    )

    invoice = result["payload"]["ARINVOICE"]
    assert invoice["WHENCREATED"] == "2026-08-20"
    assert invoice["WHENPOSTED"] == "2026-08-24"
