import functools
import io
import json
import mimetypes
import os
import random
import re
import time
import uuid

import google.auth
import requests
from google.auth.transport.requests import AuthorizedSession


class CloudJob:

    def __init__(self, *args, **kwargs):
        self.unique_id = str(uuid.uuid4())
        self.artifacts = {}
        credentials, self.gce_project = google.auth.default()
        if self.gce_project is None:
            raise RuntimeError("project must be specified")
        self.gce_http = AuthorizedSession(credentials)
        self.gce_zone = query_metadata("instance/zone").split("/")[-1]
        self.gce_instance = query_metadata("instance/name")
        self.artifact_bucket = "scaife-indexer-us-central1"
        super().__init__(*args, **kwargs)

    def handle(self, *args, **kwargs):
        self.update_metadata(status="started")
        try:
            self.load_artifacts()
            super().handle(*args, **kwargs)
            self.save_artifacts()
        except Exception:
            self.update_metadata(status="failed")
        else:
            self.update_metadata(
                status="done",
                artifacts=json.dumps(self.artifacts),
            )

    def update_metadata(self, status, artifacts=None):
        base_url = f"https://www.googleapis.com/compute/v1/projects/{self.gce_project}/zones/{self.gce_zone}/"
        attempts = 0
        while True:
            r = self.gce_http.get(f"{base_url}instances/{self.gce_instance}")
            r.raise_for_status()
            metadata = r.json()["metadata"]
            new_metadata = {
                **{
                    item["key"]: item["value"]
                    for item in metadata["items"]
                },
                "status": status,
            }
            if artifacts:
                new_metadata["artifacts"] = artifacts
            body = {
                "fingerprint": metadata["fingerprint"],
                "items": [
                    dict(key=key, value=value)
                    for key, value in new_metadata.items()
                ],
            }
            r = self.gce_http.post(
                f"{base_url}instances/{self.gce_instance}/setMetadata",
                data=json.dumps(body),
                headers={"Content-Type": "application/json"},
            )
            if r.ok:
                break
            if r.status_code == 412:
                attempts += 1
                if attempts == 5:
                    break
                else:
                    time.sleep(0.5)
                    continue
            r.raise_for_status()

    def load_artifacts(self):
        try:
            provided_artifacts = json.loads(query_metadata("instance/attributes/artifacts"))
        except (KeyError, json.JSONDecodeError):
            provided_artifacts = {}
        artifacts = {}
        for key, artifact in provided_artifacts.items():
            data = io.BytesIO()
            if "url" in artifact:
                self.read_external_artifact(artifact["url"], data)
            elif "data" in artifact:
                data.write(artifact["data"])
            data.seek(0)
            artifacts[key] = {
                "save": False,
                "data": data,
            }
        self.artifacts = artifacts

    def read_external_artifact(self, url, buf):
        if url.startswith("gs://"):
            m = re.match(r"gs://(?P<bucket>[^/]+)/(?P<obj>.+)/?$")
            if not m:
                raise RuntimeError(f"invalid GS URL format: {url}")
            bucket = m.group("bucket")
            obj = m.group("obj")
            gcs_url = f"https://www.googleapis.com/storage/v1/b/{bucket}/o/{obj}?alt=media"
            r = self.gce_http.get(gcs_url, stream=True)
            r.raise_for_status()
            for chunk in r.iter_content(1024 * 1024):
                buf.write(chunk)
        elif re.match(r"^https?://", url):
            r = requests.get(url, stream=True)
            r.raise_for_status()
            for chunk in r.iter_content(1024 * 1024):
                buf.write(chunk)
        else:
            raise RuntimeError(f"cannot handle external artifact {url}")

    def save_artifacts(self):
        artifacts = {}
        for key, artifact in self.artifacts.items():
            if not artifact.get("save", True):
                continue
            object_name = f"{self.unique_id}/{key}"
            r = ResumableObjectUpload(
                http=self.gce_http,
                bucket=self.artifact_bucket,
                object_name=object_name,
            )
            r.upload(artifact["data"])
            artifacts[key] = {
                "url": f"gs://{self.artifact_bucket}/{object_name}",
            }
        self.artifacts = artifacts


@functools.lru_cache()
def query_metadata(key):
    url = f"http://metadata.google.internal/computeMetadata/v1/{key}"
    headers = {
        "Metadata-Flavor": "Google",
    }
    r = requests.get(url, headers=headers)
    if r.status_code == 404:
        raise KeyError(key)
    r.raise_for_status()
    return r.text


DEFAULT_CHUNK_SIZE = 512 * 1024


class ResumableObjectUploadError(Exception):
    pass


class ResumableObjectUpload:

    def __init__(self, http, bucket, object_name, cache_control=None):
        self.http = http
        self.bucket = bucket
        self.object_name = object_name
        self.cache_control = cache_control

        self.chunk_size = DEFAULT_CHUNK_SIZE
        self.retry_count = 1

        # upload state
        self.resumable_uri = None
        self.resumable_progress = 0

    @property
    def content_type(self):
        value = mimetypes.guess_type(os.path.basename(self.object_name))[0]
        if value is None:
            return "application/octet-stream"
        return value

    def upload(self, stream):
        self.stream = stream
        body = None
        while body is None:
            body = self.next_chunk()

    def next_chunk(self):
        stream = self.stream
        size = stream.size
        if self.resumable_uri is None:
            url = f"https://www.googleapis.com/upload/storage/v1/b/{self.bucket}/o"
            data = {
                "name": self.object_name,
            }
            if self.cache_control:
                data["cacheControl"] = self.cache_control
            response = retry(
                functools.partial(
                    self.http.post,
                    url,
                    params={"uploadType": "resumable"},
                    headers={
                        "Content-Type": "application/json; charset=UTF-8",
                        "X-Upload-Content-Type": self.content_type,
                        "X-Upload-Content-Length": str(size),
                    },
                    data=json.dumps(data).encode("utf-8"),
                ),
                retry_count=self.retry_count,
            )
            response.raise_for_status()
            self.resumable_uri = response.headers["Location"]
        chunk = StreamSlice(stream, self.resumable_progress, self.chunk_size)
        chunk_end = min(self.resumable_progress + self.chunk_size - 1, size - 1)
        response = retry(
            functools.partial(
                self.http.put,
                self.resumable_uri,
                headers={
                    "Content-Range": "bytes {0:d}-{1:d}/{2:d}".format(self.resumable_progress, chunk_end, size),
                    "Content-Length": str(chunk_end - self.resumable_progress + 1),
                },
                data=chunk,
            ),
            retry_count=self.retry_count,
        )
        if response.status_code in [200, 201]:
            return response.content
        elif response.status_code == 308:
            if "Range" in response.headers:
                self.resumable_progress = int(response.headers["Range"].split("-")[1]) + 1
            else:
                self.resumable_progress = 0
            if "Location" in response.headers:
                self.resumable_uri = response.headers["Location"]
        else:
            raise ResumableObjectUploadError()


def retry(request, retry_count=0):
    for c in range(retry_count + 1):
        if c > 0:
            time.sleep(random.random() * 2**c)
        response = request()
        if not should_retry(response):
            break
    return response


def should_retry(response):
    if response.status_code >= 500:
        return True
    if response.status_code == 429:  # too many requests
        return True
    if response.status_code == 403:
        try:
            data = response.json()
            reason = data["error"]["errors"][0]["reason"]
        except (ValueError, KeyError):
            return False
        if reason in {"userRateLimitExceeded", "rateLimitExceeded"}:
            return True
    return False


class StreamSlice:

    def __init__(self, stream, begin, chunksize):
        self._stream = stream
        self._begin = begin
        self._chunksize = chunksize
        self._stream.seek(begin)

    def read(self, n=-1):
        # data left available to read sits in [cur, end)
        cur = self._stream.tell()
        end = self._begin + self._chunksize
        if n == -1 or cur + n > end:
            n = end - cur
        return self._stream.read(n)
