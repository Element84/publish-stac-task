#!/usr/bin/env python
import boto3
import logging
import os
from boto3utils import s3
from datetime import datetime, timezone
from dateutil.parser import parse as dateparse
from stactask import Task
from stac_validator import stac_validator
from string import Formatter, Template
from typing import Any, Dict, List, Tuple


# Environment variables from the container
DATA_BUCKET = os.getenv("SWOOP_DATA_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_DEFAULT_REGION,
)

s3session = s3(session, requester_pays=False)


class Publish(Task):
    name = "publish"
    description = "Publishes an input payload to S3."
    version = "0.1.0"

    def get_path(self, item: dict, template: str = "${collection}/${id}") -> str:
        """Get path name based on STAC Item and template string

        Args:
            item (Dict): A STAC Item.
            template (str, optional): Path template using variables referencing Item fields. Defaults to'${collection}/${id}'.

        Returns:
            [str]: A path name
        """
        _template = template.replace(":", "__colon__")
        subs = {}
        for key in [
            i[1] for i in Formatter().parse(_template.rstrip("/")) if i[1] is not None
        ]:
            # collection
            if key == "collection":
                subs[key] = item["collection"]
            # ID
            elif key == "id":
                subs[key] = item["id"]
            # derived from date
            elif key in ["year", "month", "day"]:
                dt = dateparse(item["properties"]["datetime"])
                vals = {"year": dt.year, "month": dt.month, "day": dt.day}
                subs[key] = vals[key]
            # Item property
            else:
                subs[key] = item["properties"][key.replace("__colon__", ":")]
        return Template(_template).substitute(**subs).replace("__colon__", ":")

    def update_links(
        self, item: Dict, template: str, bucket: str, public: bool
    ) -> Tuple[Dict, str]:
        """Updates the links of an item to include self and canonical links.

        Args:
            item (Dict): A STAC Item.
            template (str, optional): Path template using variables referencing Item fields. Defaults to'${collection}/${id}'.
            bucket (str): Name of S3 bucket which will be used in the href for the links.
            public (bool): Boolean value specifying if the S3 bucket is public or private.
        Returns:
            Tuple[Dict, str]: A tuple consisting of an updated STAC item and its S3 url.
        """
        url = os.path.join(self.get_path(item, template), f"{item['id']}.json")

        if url[0:5] != "s3://":
            url = f"s3://{bucket}/{url.lstrip('/')}"
        if public:
            url = s3.s3_to_https(url)

        # add canonical and self links (and remove existing self link if present)
        item["links"] = [
            link for link in item["links"] if link["rel"] not in ["self", "canonical"]
        ]
        item["links"].insert(
            0, {"rel": "canonical", "href": url, "type": "application/json"}
        )
        item["links"].insert(
            0, {"rel": "self", "href": url, "type": "application/json"}
        )

        return item, url

    def update_item_dates(self, item: Dict, url: str) -> Dict:
        """Populates an item's 'created' and 'updated' properties by checking to see
        if the item already exists on S3.

        Args:
            item (Dict): A STAC Item.
            url (str): Path to the item on S3 after templating its properties into
                       the path_template parameter
        Returns:
            Tuple[Dict, str]: A tuple consisting of an updated STAC item and its S3 url.
        """
        now = datetime.now(timezone.utc).isoformat()
        created = None
        if s3session.exists(url):
            old_item = s3session.read_json(url)
            created = old_item["properties"].get("created", None)
        if created is None:
            created = now
        item["properties"]["created"] = created
        item["properties"]["updated"] = now

        return item

    def publish_item_to_s3(self, item: Dict, url: str, headers: str, public: bool):
        """Publishes an item to S3 at a specified url.

        Args:
            item (Dict): A STAC Item.
            url (str): Path to the item on S3 after templating its properties into
                       the path_template parameter
            headers (str): Headers to include in the request to upload to S3
            public (bool): Boolean value specifying if the S3 bucket is public or private.
        Returns:
            None
        """
        extra = {"ContentType": "application/json"}
        extra.update(headers)
        s3session.upload_json(item, url, public=public, extra=extra)
        logging.info("Published to s3")

    def process(self, public: bool, stac_validate: bool) -> List[Dict[str, Any]]:
        # process method overrides Task
        payload = self._payload

        # We shouldn't have to mess with the payload or pulling out config options
        # once stac-task supports process arrays. When we get a new version of
        # stac-task with that support we can clean this code up.

        process = (
            payload["process"][0]
            if isinstance(payload["process"], list)
            else payload["process"]
        )

        upload_options = process.get("upload_options", {})
        path_template = upload_options.get("path_template", {})
        headers = upload_options.get("headers", {})
        config = process.get("tasks", {}).get("publish", {})
        public = config.get("public", False)
        stac_validate = config.get("stac_validate", True)

        items = self.items_as_dicts

        try:
            logging.debug("Publishing items to S3")

            mod_items = []

            stac = stac_validator.StacValidate()

            for item in items:
                link_item, url = self.update_links(
                    item, path_template, DATA_BUCKET, public
                )

                mod_item = self.update_item_dates(link_item, url)

                mod_items.append(mod_item)
                if stac_validate and not stac.validate_dict(mod_item):
                    raise Exception(
                        f"STAC Item validation failed. Error: {stac.message[0]['error_message']}."
                    )
                self.publish_item_to_s3(mod_item, url, headers, public)

            return mod_items

        except Exception as err:
            msg = f"publish: failed publishing output items ({err})"
            logging.exception(msg)
            raise


def handler(event: dict[str, Any], context: dict[str, Any] = {}) -> Task:
    return Publish.handler(event)


if __name__ == "__main__":
    Publish.cli()
