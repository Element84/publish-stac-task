#!/usr/bin/env python
import boto3
import json
import logging
import os
from boto3utils import s3
from datetime import datetime, timezone
from dateutil.parser import parse as dateparse
from stactask import Task
from stactask.exceptions import InvalidInput
from stac_validator import stac_validator
from string import Formatter, Template
from typing import Any, Dict, List


s3_client = s3(requester_pays=False)

# Environment variables from the container
DATA_BUCKET = os.getenv("SWOOP_DATA_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")


class Publish(Task):
    name = "publish"
    description = "Publishes an input payload to S3."
    version = "0.1.0"

    @classmethod
    def validate(cls, payload: dict[str, Any]) -> bool:
        if "publish" not in payload["process"]["tasks"]:
            raise InvalidInput(
                "Publish needs to be specified as a task in the input payload"
            )
        return True

    def get_path(item: dict, template: str = "${collection}/${id}") -> str:
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

    def publish_items_to_s3(payload, bucket, public) -> Dict:
        opts = payload.get("process", {}).get("upload_options", {})
        for item in payload["features"]:
            # determine URL of data bucket to publish to- always do this
            url = os.path.join(
                Publish.get_path(item, opts.get("path_template")), f"{item['id']}.json"
            )

            if url[0:5] != "s3://":
                url = f"s3://{bucket}/{url.lstrip('/')}"
            if public:
                url = s3.s3_to_https(url)

            # add canonical and self links (and remove existing self link if present)
            item["links"] = [
                link
                for link in item["links"]
                if link["rel"] not in ["self", "canonical"]
            ]
            item["links"].insert(
                0, {"rel": "canonical", "href": url, "type": "application/json"}
            )
            item["links"].insert(
                0, {"rel": "self", "href": url, "type": "application/json"}
            )

            # get S3 session
            # TO-DO: ADD requester_pays to secret?

            session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN,
                region_name=AWS_DEFAULT_REGION,
            )

            s3session = s3(session, requester_pays=False)

            # if existing item use created date
            now = datetime.now(timezone.utc).isoformat()
            created = None
            if s3session.exists(url):
                old_item = s3session.read_json(url)
                created = old_item["properties"].get("created", None)
            if created is None:
                created = now
            item["properties"]["created"] = created
            item["properties"]["updated"] = now

            # publish to bucket
            headers = opts.get("headers", {})

            extra = {"ContentType": "application/json"}
            extra.update(headers)
            s3session.upload_json(item, url, public=public, extra=extra)
            logging.info("Published to s3")

        return payload

    def process(self, public: bool) -> List[Dict[str, Any]]:
        # process method overrides Task
        created_items = []
        payload = self._payload
        item = self.items[0]
        process = (
            payload["process"][0]
            if isinstance(payload["process"], list)
            else payload["process"]
        )

        config = process.get("tasks", {}).get("publish", {})
        public = config.get("public", False)

        try:
            logging.debug("Publishing items to S3")

            # publish to s3
            mod_payload = Publish.publish_items_to_s3(payload, DATA_BUCKET, public)

        except Exception as err:
            msg = f"publish: failed publishing output items ({err})"
            logging.error(msg, exc_info=True)
            raise Exception(msg) from err

        # STAC-validate item in payload before completing

        item = mod_payload["features"][0]

        stac = stac_validator.StacValidate()
        valid = stac.validate_dict(item)

        if valid:
            created_items.append(item)
            return created_items
        else:
            raise Exception(
                f"STAC Item validation failed. Error: {stac.message[0]['error_message']}."
            )


def handler(event: dict[str, Any], context: dict[str, Any] = {}) -> Task:
    return Publish.handler(event)


if __name__ == "__main__":
    Publish.cli()
