import logging
import os
from urllib.parse import urljoin, quote

from osfclient.api import OSF as OriginalOSF
from osfclient.models import Project as OriginalProject
from osfclient.models import Storage as OriginalStorage
from osfclient.models import File as OriginalFile
from osfclient.models.file import copyfileobj
from osfclient.exceptions import OSFException, UnauthorizedException


logger = logging.getLogger(__name__)


class File(OriginalFile):
    def _update_attributes(self, file):
        super()._update_attributes(file)
        self.provider = self._get_attribute(file, 'attributes', 'provider')
        self.node_id = self._get_attribute(file, 'relationships', 'node', 'data', 'id')
        self.current_version = self._get_attribute(
            file, 'attributes', 'current_version', default=None,
        )

    def add_to_crate(self, source_abspath, source_relpath, host, crate):
        relpath = os.path.join(
            '.',
            source_relpath,
        )
        crate.add_file(
            source_abspath,
            relpath,
            properties=self.to_crate_properties(host),
        )

    def get_rdm_url(self, host):
        base = host['hostname'][0]
        node_url = urljoin(base, self.node_id + '/')
        files_url = urljoin(node_url, 'files/')
        return urljoin(files_url, self.provider + self.osf_path)

    @property
    def rdm_hashes(self):
        candidates = ['sha1', 'sha256', 'sha512', 'md5']
        return dict([
            (k, self.hashes[k])
            for k in candidates
            if k in self.hashes
        ])

    def to_crate_properties(self, host):
        p = {
            'name': self.name,
            'size': self.size,
            'dateCreated': self.date_created,
            'dateModified': self.date_modified,
            'rdmURL': self.get_rdm_url(host),
        }
        if self.current_version is not None:
            p['version'] = str(self.current_version)
        p.update(self.rdm_hashes)
        return p

    def write_to(self, fp, version=None):
        """Write contents of this file to a local file.

        Pass in a filepointer `fp` that has been opened for writing in
        binary mode.
        """
        if 'b' not in fp.mode:
            raise ValueError("File has to be opened in binary mode.")

        query = f'version={quote(version)}' if version is not None else ''
        try:
            response = self._get(self._add_version_to_url(self._download_url, query), stream=True)
        except UnauthorizedException:
            response = self._get(self._add_version_to_url(self._upload_url, query), stream=True)
        if response.status_code == 200:
            response.raw.decode_content = True
            copyfileobj(response.raw, fp,
                        int(response.headers['Content-Length'])
                        if 'Content-Length' in response.headers else None)

        else:
            raise RuntimeError("Response has status "
                               "code {}.".format(response.status_code))
        
    def _add_version_to_url(self, url, query):
        if query == '':
            return url
        if '?' in url:
            return url + '&' + query
        return url + '?' + query

class Storage(OriginalStorage):
    @property
    def files(self):
        """Iterate over all files in this storage.

        Recursively lists all files in all subfolders.
        """
        return self._iter_children(self._files_url, 'file', File,
                                   self._files_key)

    def matched_files(self, target_filter):
        """Iterate all matched files in this storage.

        Recursively lists files in all subfolders.
        """
        return self._iter_children(self._files_url, 'file', File,
                                   self._files_key, target_filter)

class Project(OriginalProject):
    def storage(self, provider='osfstorage'):
        """Return storage `provider`."""
        stores = self._json(self._get(self._storages_url), 200)
        stores = stores['data']
        for store in stores:
            provides = self._get_attribute(store, 'attributes', 'provider')
            if provides == provider:
                return Storage(store, self.session)

        raise RuntimeError("Project has no storage "
                           "provider '{}'".format(provider))

class OSF(OriginalOSF):
    def project(self, project_id):
        """Fetch project `project_id`."""
        type_ = self.guid(project_id)
        url = self._build_url(type_, project_id)
        if type_ in Project._types:
            return Project(self._json(self._get(url), 200), self.session)
        raise OSFException('{} is unrecognized type {}. Clone supports projects and registrations'.format(project_id, type_))


