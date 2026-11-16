"""Colocated smoke-test runner for target-intacct-v3."""

import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetIntacctV3TestRunner(VCRTargetTestRunner):
    # Intacct reference responses are XML, so mapper lookup fields must remain
    # literal in Singer input for replay to resolve them.
    PRESERVE_KEYS = {
        "externalId",
        "accountName",
        "billNumber",
        "className",
        "departmentName",
        "displayName",
        "itemName",
        "itemNumber",
        "locationName",
        "projectName",
        "subsidiaryName",
        "transactionNumber",
        "vendorCreditNumber",
        "vendorName",
        "vendorNumber",
    }
    # Values that select mapper behavior rather than carrying test data.
    PRESERVE_VALUES = {
        "Inventory",
        "Non-Inventory",
        "Non-Inventory (Purchase only)",
        "Non-Inventory (Sales only)",
        "billing",
        "BILL_CF_BOOL",
        "BILL_CF_NUMBER",
        "BILL_CF_TEXT",
        "BILL_LINE_CF_BOOL",
        "BILL_LINE_CF_NUMBER",
        "BILL_LINE_CF_TEXT",
        "fax",
        "inexistent_file.json",
        "invalid_name_no_extension",
        "mobile",
        "primary",
        "test.invoice.pdf",
        "test.invoice_new.pdf",
        "test@example.com",
        "test",
        "user",
        "VENDOR_CF_TEXT",
        "VC_LINE_CF_TEXT",
        "VC_CF_TEXT",

    }

    def module(self) -> str:
        return "target_intacct_v3"

    def launch(self):
        from target_intacct_v3.target import TargetIntacctV3

        TargetIntacctV3.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        return vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_request=self.before_record_request,
            before_record_response=self.before_record_response,
        )


if __name__ == "__main__":
    TargetIntacctV3TestRunner.main()
