import os
import re
import json
import shutil
import uuid

from urllib import request
from urllib.request import Request
from urllib.parse import urlparse

from .. import __version__
from .base import ContentProvider


class WEKO3(ContentProvider):
    """Provide contents of WEKO3."""

    def __init__(self):
        self.hosts = [
            {
                "hostname": [
                    "https://test.some.host.nii.ac.jp/",
                ],
                "file_base_url": "https://test.some.host.nii.ac.jp/api/files/",
            }
        ]
        if "WEKO3_HOSTS" in os.environ:
            with open(os.path.expanduser(os.environ["WEKO3_HOSTS"])) as f:
                self.hosts = json.load(f)
        if "WEKO3_HOSTS_JSON" in os.environ:
            self.hosts = json.loads(os.environ["WEKO3_HOSTS_JSON"])
        if isinstance(self.hosts, list):
            for host in self.hosts:
                if "hostname" not in host:
                    raise ValueError("No hostname: {}".format(json.dumps(host)))
                if not isinstance(host["hostname"], list):
                    raise ValueError(
                        "hostname should be list of string: {}".format(
                            json.dumps(host["hostname"])
                        )
                    )
                if "file_base_url" not in host:
                    raise ValueError("No file_base_url: {}".format(json.dumps(host)))

    def detect(self, source, ref=None, extra_args=None):
        """Trigger this provider for directory on WEKO3"""
        for host in self.hosts:
            if any([source.startswith(s) for s in host["hostname"]]):
                u = urlparse(source)
                path = u.path[1:] if u.path.startswith("/") else u.path
                if "/" not in path:
                    raise ValueError("file_names is not defined: {}".format(path))
                self.bucket, file_names = path.split("/", 1)
                self.file_names = file_names.split(",")
                self.uuid = ref if ref is not None else str(uuid.uuid1())
                return {
                    "bucket": self.bucket,
                    "file_names": self.file_names,
                    "host": host,
                    "uuid": self.uuid,
                }
        return None

    def fetch(self, spec, output_dir, yield_output=False):
        """Fetch WEKO3 directory"""
        bucket = spec["bucket"]
        file_names = spec["file_names"]
        host = spec["host"]
        file_base_url = (
            host["file_base_url"][:-1]
            if host["file_base_url"].endswith("/")
            else host["file_base_url"]
        )

        yield "Fetching WEKO3 directory {} on {} at {}.\n".format(
            ", ".join(file_names), bucket, file_base_url
        )
        access_token = host["token"] if "token" in host else os.getenv("WEKO3_TOKEN")
        if access_token is None:
            raise ValueError("Token is not set")

        for file_name in file_names:
            file_url = file_base_url + "/" + bucket + "/" + file_name
            output_file = os.path.join(output_dir, file_name)
            yield "Fetch: {} to {}\n".format(file_url, output_file)
            req = Request(
                file_url,
                headers={"Authorization": "Bearer " + access_token},
            )
            resp = self.urlopen(req)
            with open(output_file, "wb") as f:
                f.write(resp.read())

    @property
    def content_id(self):
        """Content ID of the WEOK3 directory - this provider identifies repos by random UUID"""
        return "{}-{}-{}".format(self.bucket, "-".join(self.file_names), self.uuid)

    def urlopen(self, req, headers=None):
        """A urlopen() helper"""
        req.add_header("User-Agent", "repo2docker {}".format(__version__))
        if headers is not None:
            for key, value in headers.items():
                req.add_header(key, value)

        return request.urlopen(req)
