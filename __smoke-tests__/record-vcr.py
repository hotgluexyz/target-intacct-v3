import re

import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetIntacctV3Smoke(VCRTargetTestRunner):
    def module(self) -> str:
        return "target_intacct_v3"

    def launch(self):
        from target_intacct_v3.target import TargetIntacctV3

        TargetIntacctV3.cli()

    def scrub_sensitive_items_from_request(self, request):
        try:
            body = request.body or b""
            if isinstance(body, bytes):
                body_text = body.decode("utf-8", errors="ignore")
            else:
                body_text = str(body)

            if "<" not in body_text:
                return request

            masked_text = body_text
            for tag, placeholder in (
                ("senderid", "SENDER_ID"),
                ("userid", "USER_ID"),
                ("password", "PASSWORD"),
                ("sessionid", "SESSION_ID"),
            ):
                masked_text = re.sub(
                    rf"(<\s*{tag}\b[^>]*>)(.*?)(<\s*/\s*{tag}\s*>)",
                    rf"\1{placeholder}\3",
                    masked_text,
                    flags=re.IGNORECASE | re.DOTALL,
                )
            if masked_text != body_text:
                encoded = masked_text.encode("utf-8")
                if hasattr(request, "_replace"):
                    return request._replace(body=encoded)
                request.body = encoded
        except Exception:
            return request
        return request

    def vcr_use_cassette(self, filter_query_parameters):
        my_vcr = vcr.VCR()
        return my_vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            before_record_request=self.scrub_sensitive_items_from_request,
        )

    def sanitize_cassette(self):
        # Intacct responses are XML; default JSON scrub refuses non-JSON bodies.
        pass


if __name__ == "__main__":
    TargetIntacctV3Smoke.main()
