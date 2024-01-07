import os
import re
import json
import shutil
import uuid

from urllib.parse import urlparse
from rocrate.rocrate import ROCrate

from ..base import ContentProvider
from ..crate.util import write_json

from .api import OSF
from osfclient.utils import is_path_matched


class RDM(ContentProvider):
    """Provide contents of GakuNin RDM."""

    def __init__(self):
        self.hosts = [
            {
                "hostname": [
                    "https://test.some.host.nii.ac.jp/",
                ],
                "api": "https://api.test.some.host.nii.ac.jp/v2/",
            }
        ]
        if "RDM_HOSTS" in os.environ:
            with open(os.path.expanduser(os.environ["RDM_HOSTS"])) as f:
                self.hosts = json.load(f)
        if "RDM_HOSTS_JSON" in os.environ:
            self.hosts = json.loads(os.environ["RDM_HOSTS_JSON"])
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
                if "api" not in host:
                    raise ValueError("No api: {}".format(json.dumps(host)))

    def detect(self, source, ref=None, extra_args=None):
        """Trigger this provider for directory on RDM"""
        for host in self.hosts:
            if any([source.startswith(s) for s in host["hostname"]]):
                u = urlparse(source)
                path = u.path[1:] if u.path.startswith("/") else u.path
                if "/" in path:
                    self.project_id, self.path = path.split("/", 1)
                    if self.path.startswith("files/dir/"):
                        # URL for folder is like https://test.some.host.nii.ac.jp/123456/files/dir/abc/def
                        self.path = self.path[len("files/dir/") :]
                    if self.path.startswith("files/"):
                        self.path = self.path[len("files/") :]
                else:
                    self.project_id = path
                    self.path = ""
                self.uuid = ref if self._check_ref_defined(ref) else str(uuid.uuid1())
                return {
                    "project_id": self.project_id,
                    "path": self.path,
                    "host": host,
                    "uuid": self.uuid,
                }
        return None

    def _check_ref_defined(self, ref):
        if ref is None or ref == "HEAD":
            return False
        return True

    def fetch(self, spec, output_dir, yield_output=False):
        """Fetch RDM directory"""
        project_id = spec["project_id"]
        path = spec["path"]
        host = spec["host"]
        api_url = host["api"][:-1] if host["api"].endswith("/") else host["api"]

        yield "Fetching RDM directory {} on {} at {}.\n".format(
            path, project_id, api_url
        )
        osf = OSF(
            token=host["token"] if "token" in host else os.getenv("OSF_TOKEN"),
            base_url=api_url,
        )
        project = osf.project(project_id)

        crate_files = []
        if len(path):
            storage = project.storage(path[: path.index("/")] if "/" in path else path)
            subpath = path[path.index("/") :] if "/" in path else "/"
            for line in self._fetch_storage(storage, output_dir, subpath, crate_files=crate_files):
                yield line
        else:
            for storage in project.storages:
                for line in self._fetch_storage(storage, output_dir, crate_files=crate_files):
                    yield line
        crate = ROCrate()
        for abspath, relpath, crate_file in crate_files:
            crate_file.add_to_crate(abspath, relpath, host, crate)
        crate_path = os.path.join(output_dir, ".run-crate-metadata.json")
        write_json(crate, crate_path)

    def _fetch_storage(self, storage, output_dir, path=None, crate_files=None):
        if path is None:
            path_filter = None
        elif path == "/":
            path_filter = None
        else:
            path = path if path.endswith("/") else path + "/"
            path_filter = lambda f: is_path_matched(path, f)
        files = (
            storage.files if path_filter is None else storage.matched_files(path_filter)
        )
        for file_ in files:
            if path is None:
                local_path = storage.provider + file_.path
            else:
                local_path = file_.path[len(path) :]
            local_full_path = os.path.join(output_dir, local_path)
            local_dir, _ = os.path.split(local_full_path)
            if not os.path.isdir(local_dir):
                os.makedirs(local_dir)
            with open(local_full_path, "wb") as f:
                file_.write_to(f)
            if crate_files is not None:
                crate_files.append((local_full_path, local_path, file_))
            yield "Fetch: {} ({} to {})".format(file_.path, local_path, output_dir)

    @property
    def content_id(self):
        """Content ID of the RDM directory - this provider identifies repos by random UUID"""
        return "{}-{}".format(self.project_id, self.uuid)
