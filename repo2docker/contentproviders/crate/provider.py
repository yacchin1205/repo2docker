from datetime import datetime, timezone
import os
import json
import logging
import shutil
import tempfile

from urllib.parse import urlparse, quote
from rocrate.rocrate import ROCrate

from ..base import ContentProvider


CRATE_URL_PREFIX = "crate+"
logger = logging.getLogger(__name__)


class Crate(ContentProvider):
    """Provide contents of Run-Crate."""

    def __init__(self):
        from . import fetchers
        self.fetcher_classes = [
            ("rdm", fetchers.RDMCrateFetcher),
        ]

    def detect(self, source, ref=None, extra_args=None):
        """Trigger this provider for file of Run-Crate"""
        if not source.startswith(CRATE_URL_PREFIX):
            return None
        self.url = source[len(CRATE_URL_PREFIX):]
        fetcher_id, fetcher = self._detect_fetcher(self.url)
        if fetcher is None:
            logger.warn("Unsupported Run-Crate URL: {}".format(self.url))
            return None
        self.fetcher_id = fetcher_id
        self.hash, self.crate = fetcher.fetch(self.url)
        return {
            "url": self.url,
            "hash": self.hash,
            "crate": self.crate,
            "fetcher": self.fetcher_id,
        }

    def fetch(self, spec, output_dir, yield_output=False):
        """Fetch RDM directory"""
        crate = spec["crate"]
        fetcher_class = dict(self.fetcher_classes)[spec["fetcher"]]
        fetcher = fetcher_class()

        yield "Fetching files from Run-Crate {}.\n".format(self.url)
        work_dir = tempfile.mkdtemp()
        try:
            base_crate_path = os.path.join(work_dir, "ro-crate-metadata.json")
            with open(base_crate_path, "w") as f:
                json.dump(crate, f)
            rocrate = ROCrate(work_dir)
            for entity in rocrate.data_entities:
                url = entity.get("rdmURL")
                version = entity.get("version", None)
                dest = os.path.join(output_dir, entity.get("@id"))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                fetcher.download_to(url, dest, version=version)
                self._update_file_timestamp(dest, entity)
                yield "Fetched {} to {}.\n".format(url, dest)
            crate_path = os.path.join(output_dir, ".run-crate-metadata.json")
            with open(crate_path, "w") as f:
                json.dump(crate, f)
            yield "Saved Run-Crate to {}.\n".format(crate_path)
        finally:
            shutil.rmtree(work_dir)

    @property
    def content_id(self):
        """Content ID of the Run-Crate file - this provider identifies repos by filename and hash"""
        filename = self.url.split("/")[-1]
        filename = quote(filename).replace("%", "_")
        return "{}-{}".format(filename, self.hash)

    def _update_file_timestamp(self, path, entity):
        created_str = entity.get("dateCreated", None)
        if created_str is None:
            return
        modified_str = entity.get("dateModified", None)
        if modified_str is None:
            return
        created = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        modified = datetime.strptime(modified_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        os.utime(path, (created.timestamp(), modified.timestamp()))
        
    def _detect_fetcher(self, url):
        for fetcher_id, fetcher_class in self.fetcher_classes:
            fetcher = fetcher_class()
            if fetcher.detect(url):
                return fetcher_id, fetcher
        return None
