import base64
from pathlib import Path

import requests
from target_intacct_v3.mappers.base_mapper import InvalidInputError


class AttachmentSchemaMapper:
    def __init__(self, logger):
        self.logger = logger

    def to_intacct(self, input_path, new_attachment, existing_attachments):
        name = new_attachment.get("name")
        if not name:
            raise InvalidInputError(f"Attachment 'name' is required.")

        name_splitted = name.split(".")
        if len(name_splitted) == 1:
            raise InvalidInputError(f"Attachment '{name}' must contain a file extension.")
        
        attachment_name = ".".join(name_splitted[:-1])
        attachment_type = name_splitted[-1]

        attachment_data = None

        url = new_attachment.get("url")
        if url:
            try:
                response = requests.get(url)
            except Exception as e:
                self.logger.exception(f"Error fetching attachment from url: {url}")
                raise Exception(f"Error fetching attachment from url: {url}. Expcetion: {e}")
            
            data = base64.b64encode(response.content)
            data = data.decode()
            attachment_data = data
        else:
            att_path = f"{input_path}/{new_attachment.get('name')}"
            try:
                with open(att_path, "rb") as attach_file:
                    data = base64.b64encode(attach_file.read()).decode()
                    attachment_data = data
            except FileNotFoundError:
                self.logger.error(f"Attachment file not found: {att_path}")
                raise InvalidInputError(f"Attachment file not found: {att_path}")

        if isinstance(existing_attachments, dict):
            existing_attachments = [existing_attachments]

        for existing_attachment in existing_attachments:
            if existing_attachment.get("attachmentname") == attachment_name and existing_attachment.get("attachmenttype") == attachment_type:
                self.logger.info(f"Attachment {name} already exists. Skipping creation.")
                return None

            if existing_attachment.get("attachmentdata") == attachment_data:
                self.logger.info(f"Attachment with the same content already exists for {name}. Skipping creation.")
                return None

        return {
            "attachmentname": attachment_name,
            "attachmenttype": attachment_type,
            "attachmentdata": attachment_data
        }
