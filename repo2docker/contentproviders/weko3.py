import os
import re
import json
import re
import shutil
import uuid

from urllib import request
from urllib.request import Request
from urllib.parse import urlparse

from .. import __version__
from .base import ContentProvider

from bs4 import BeautifulSoup


class WEKO3(ContentProvider):
    """Provide contents of WEKO3."""

    def __init__(self):
        super().__init__()
        self.unnamed_files = 0
        self.hosts = [
            {
                "hostname": [
                    "https://test.some.host.nii.ac.jp/",
                ]
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

    def detect(self, source, ref=None, extra_args=None):
        """Trigger this provider for directory on WEKO3"""
        for host in self.hosts:
            if any([source.startswith(s) for s in host["hostname"]]):
                self.url = source
                self.uuid = ref if ref is not None else str(uuid.uuid1())
                return {
                    "url": self.url,
                    "host": host,
                    "uuid": self.uuid,
                }
        return None

    def fetch(self, spec, output_dir, yield_output=False):
        """Fetch WEKO3 directory"""
        url = spec["url"]
        host = spec["host"]

        for msg in self._fetch_url(url, output_dir):
            yield msg

    def _log_403_error(self, url):
        self.log.error(f"403 Error: {url}")

    def _parse_urls(self, soup, depth=0):
        if depth > 0:
            return None
        nodes = soup.find("script", {"type": "application/ld+json"})
        if nodes is None or len(nodes) == 0:
            return None
        content = json.loads("".join(nodes.contents))
        if "distribution" not in content:
            return []
        return [
            dist["contentUrl"]
            for dist in content["distribution"]
            if "contentUrl" in dist
        ]

    def _get_filename(self, url, resp):
        # Content-Disposition考慮
        cd = resp.getheader("Content-Disposition")
        u = urlparse(url)
        _, default_filename = os.path.split(u.path)
        if cd is None:
            return self._normalize_url_filename(default_filename)
        disp = [part.strip() for part in cd.split(";")]
        for part in disp:
            if not part.startswith("filename="):
                continue
            part = part[9:].strip()
            if part.startswith('"') and part.endswith('"'):
                return self._normalize_content_disposition_filename(part[1:-1])
            if part.startswith("'") and part.endswith("'"):
                return self._normalize_content_disposition_filename(part[1:-1])
            return self._normalize_content_disposition_filename(part)
        self.log.warning(f"Unknown Content-Disposition header: {cd}")
        u = urlparse(url)
        _, filename = os.path.split(u.path)
        return self._normalize_url_filename(filename)

    def _normalize_content_disposition_filename(self, filename):
        return re.sub(r"[/¥]", "-", filename)

    def _normalize_url_filename(self, filename):
        if len(filename) == 0:
            self.unnamed_files += 1
            return f"unnamed_{self.unnamed_files}"
        return re.sub(r"[/¥]", "-", filename)

    def _fetch_url(self, url, output_dir, depth=0):
        yield "Fetching WEKO3 URL at {}.\n".format(url)

        req = Request(url)
        resp = self.urlopen(req)
        if resp.status == 403:
            self._log_403_error(url)
            return
        if resp.status == 401:
            # Start OAuth flow
            raise NotImplementedError()
        if resp.status != 200:
            raise IOError(f"Status: {resp.status}")
        filepath = os.path.join(output_dir, self._get_filename(url, resp))
        with open(filepath, "wb") as tf:
            tf.write(resp.read())
        content_type = resp.getheader("Content-Type")
        if content_type is None or not content_type.startswith("text/html"):
            return
        parser = "html.parser"
        soup = BeautifulSoup(open(filepath), parser)
        urls = self._parse_urls(soup, depth=depth)
        if urls is None:
            return
        for content_url in urls:
            for msg in self._fetch_url(content_url, output_dir, depth=depth + 1):
                yield msg

    @property
    def content_id(self):
        """Content ID of the WEKO3 directory - this provider identifies repos by random UUID"""
        return "{}-{}".format(self.url, self.uuid)

    def urlopen(self, req, headers=None):
        """A urlopen() helper"""
        req.add_header("User-Agent", "repo2docker {}".format(__version__))
        if headers is not None:
            for key, value in headers.items():
                req.add_header(key, value)

        return request.urlopen(req)
