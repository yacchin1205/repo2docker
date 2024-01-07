import logging
import hashlib
import io
import json
import os
from urllib.parse import urlparse

from ..fetcher import CrateFetcher
from repo2docker.contentproviders import RDM
from repo2docker.contentproviders.rdm.api import OSF


logger = logging.getLogger(__name__)


def is_wb_path_matched(path, file):
    file_path = file["attributes"]["path"]
    return path == file_path


class RDMCrateFetcher(CrateFetcher):
    """Loader for RDM crates"""

    def __init__(self):
        """Initialize loader"""
        self.rdm = RDM()

    def detect(self, url):
        """Detect compatibility between loader and given url.
        
        Parameters
        ----------
        url : str
            URL of crate

        Returns
        -------
        supported : bool
            Whether loader supports given url
        """
        host = self._get_host(url)
        return host is not None

    def fetch(self, url):
        """Fetch crate from given url
        
        Parameters
        ----------
        url : str
            URL of crate

        Returns
        -------
        (hash, crate) : tuple
            hash of crate and crate object
        """
        file = self._fetch_file(url)
        buf = io.BytesIO()
        buf.mode = 'wb'
        file.write_to(buf)
        buf.seek(0)
        data = buf.getvalue()
        text = data.decode('utf8')
        hash = hashlib.sha256(data).hexdigest()
        logger.debug(f'Run-Crate file: {url}, {text}')
        return hash, json.loads(text)
    
    def download_to(self, url, dest, version=None):
        """Download file from given url to given destination

        Parameters
        ----------
        url : str
            URL of file
        dest : str
            Destination path
        version : str
            Version of file
        """
        file = self._fetch_file(url)
        with open(dest, "wb") as f:
            file.write_to(f, version=version)
    
    def _get_host(self, url):
        for host in self.rdm.hosts:
            if any([url.startswith(s) for s in host["hostname"]]):
                return host
        return None
    
    def _extract_project_id_and_path(self, url):
        u = urlparse(url)
        path = u.path[1:] if u.path.startswith("/") else u.path
        if "/" not in path:
            raise ValueError("Invalid RDM URL: {}".format(url))
        pos = path.index("/")
        project_id = path[:pos]
        files = path[pos + 1:]
        if not files.startswith("files/"):
            raise ValueError("Invalid RDM URL: {}".format(url))
        pos = files.index("/")
        wb_path = files[pos + 1:]
        if "/" not in wb_path:
            raise ValueError("Invalid RDM URL: {}".format(url))
        pos = wb_path.index("/")
        provider = wb_path[:pos]
        files_path = wb_path[pos:]
        return project_id, provider, files_path

    def _fetch_file(self, url):
        host = self._get_host(url)
        project_id, provider, path = self._extract_project_id_and_path(url)
        api_url = host["api"][:-1] if host["api"].endswith("/") else host["api"]

        osf = OSF(
            token=host["token"] if "token" in host else os.getenv("OSF_TOKEN"),
            base_url=api_url,
        )
        project = osf.project(project_id)
        storage = project.storage(provider)
        path_filter = lambda f: is_wb_path_matched(path, f)
        files = list(storage.matched_files(path_filter))
        if len(files) != 1:
            raise ValueError("Crate file not found: {}".format(url))
        return files[0]
