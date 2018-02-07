import functools
import json

import google.auth
import requests
from google.auth.transport.requests import AuthorizedSession

from ...morphology import MorphologyBuilder
from .corpus_walker import CorpusWalker


class Command(CorpusWalker):

    help = "Build morphology"

    def handle(self, *args, **options):
        self.update_metadata(status="started")
        try:
            super().handle(*args, **options)
        except Exception:
            self.update_metadata(status="failed")
        else:
            self.update_metadata(status="done")

    def update_metadata(self, status):
        credentials, project = google.auth.default()
        if project is None:
            raise RuntimeError("project must be specified")
        http = AuthorizedSession(credentials)
        zone = query_metadata("instance/zone").split("/")[-1]
        instance = query_metadata("instance/name")
        base_url = f"https://www.googleapis.com/compute/v1/projects/{project}/zones/{zone}/"
        r = http.get(f"{base_url}instances/{instance}")
        r.raise_for_status()
        metadata = r.json()["metadata"]
        body = {
            "fingerprint": metadata["fingerprint"],
            "items": [
                dict(key=key, value=value)
                for key, value in {
                    **{
                        item["key"]: item["value"]
                        for item in metadata["items"]
                    },
                    "status": status,
                }.items()
            ],
        }
        r = http.post(
            f"{base_url}instances/{instance}/setMetadata",
            data=json.dumps(body),
            headers={"Content-Type": "application/json"},
        )
        r.raise_for_status()

    def walk(self, executor, **kwargs):
        builder = MorphologyBuilder(executor, **kwargs)
        builder.run()


@functools.lru_cache()
def query_metadata(key):
    url = f"http://metadata.google.internal/computeMetadata/v1/{key}"
    headers = {
        "Metadata-Flavor": "Google",
    }
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.text
