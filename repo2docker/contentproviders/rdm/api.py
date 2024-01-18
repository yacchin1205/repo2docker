import logging
import os
from urllib.parse import urljoin, quote

from osfclient.api import OSF as OriginalOSF
from osfclient.models import Project as OriginalProject
from osfclient.models import Storage as OriginalStorage
from osfclient.models import File as OriginalFile
from osfclient.models.file import copyfileobj
from osfclient.exceptions import OSFException, UnauthorizedException
from rocrate.model import Person
from rocrate.model.contextentity import ContextEntity


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

    def get_file(self, path):
        path = path.lstrip('/')
        url = urljoin(self._files_url, path)
        file = self._json(self._get(url), 200)
        return File(file['data'], self.session)

class Project(OriginalProject):
    institutions = {}

    def _update_attributes(self, project):
        super()._update_attributes(project)
        if not project:
            return
        project = project['data']
        self.category = self._get_attribute(project, 'attributes', 'category')
        self._creator_url = self._get_attribute(project, 'relationships', 'creator', 'links', 'related', 'href')
        self._contributors_url = self._get_attribute(project, 'relationships', 'contributors', 'links', 'related', 'href')

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

    def add_to_crate(self, crate, node_ids=None, user_ids=None, institution_ids=None):
        if self.id not in node_ids:
            node_id = f'#node.{len(node_ids)}'
            node_ids[self.id] = node_id
        else:
            node_id = node_ids[self.id]
        creator_entity = self._add_creator_to_crate(
            crate,
            user_ids=user_ids,
            institution_ids=institution_ids,
        )
        contributor_entities = self._add_contributors_to_crate(
            crate,
            user_ids=user_ids,
            institution_ids=institution_ids,
        )
        crate.add(ContextEntity(crate, node_id, properties={
            '@type': 'RDMProject',
            'about': {
                '@id': './',
            },
            'name': self.title,
            'description': self.description,
            'category': self.category,
            'dateCreated': self.date_created,
            'dateModified': self.date_modified,
            'creator': {
                '@id': creator_entity.id,
            },
            'contributor': [
                {
                    '@id': entity.id,
                }
                for entity in contributor_entities
            ],
        }))

    def _add_creator_to_crate(self, crate, user_ids=None, institution_ids=None):
        creator = self._json(self._get(self._creator_url), 200)
        self.creator = self._get_attribute(creator, 'data', 'id')
        return self._add_user_to_crate(
            creator,
            crate,
            user_ids=user_ids,
            institution_ids=institution_ids,
        )

    def _add_user_to_crate(self, user, crate, user_ids=None, institution_ids=None):
        if user['data']['id'] not in user_ids:
            user_id = f'#user.{len(user_ids)}'
            user_ids[user['data']['id']] = user_id
        else:
            user_id = user_ids[user['data']['id']]
        institution_url = self._get_attribute(user, 'data', 'relationships', 'institutions', 'links', 'related', 'href', default=None)
        institution = None
        if institution_url is not None:
            institution = self._add_institution_to_crate(
                institution_url,
                crate,
                institution_ids=institution_ids,
            )
        props = {
            '@type': 'Person',
            'name': user['data']['attributes']['full_name'],
            'givenName': [
                {
                    '@value': user['data']['attributes']['given_name'],
                    '@language': 'en',
                }
            ],
            'middleNames': [
                {
                    '@value': user['data']['attributes']['middle_names'],
                    '@language': 'en',
                }
            ],
            'familyName': [
                {
                    '@value': user['data']['attributes']['family_name'],
                    '@language': 'en',
                }
            ],
        }
        if institution is not None:
            props['affiliation'] = {
                '@id': institution.id,
            }
        person = Person(crate, user_id, properties=props)
        crate.add(person)
        return person

    def _add_contributors_to_crate(self, crate, user_ids=None, institution_ids=None):
        contributors = self._json(self._get(self._contributors_url), 200)
        return [
            self._add_user_to_crate(
                contributor['embeds']['users'],
                crate,
                user_ids=user_ids,
                institution_ids=institution_ids,
            )
            for contributor in contributors['data']
            if contributor['embeds']['users']['data']['id'] != self.creator
        ]

    def _add_institution_to_crate(self, institution_url, crate, institution_ids=None):
        if institution_url not in self.institutions:
            institution = self._json(self._get(institution_url), 200)['data']
            self.institutions[institution_url] = institution
        else:
            institution = self.institutions[institution_url]
        if len(institution) == 0:
            return None
        institution = institution[0]
        if institution['id'] not in institution_ids:
            institution_id = f'#institution.{len(institution_ids)}'
            institution_ids[institution['id']] = institution_id
        else:
            institution_id = institution_ids[institution['id']]
        organization = ContextEntity(crate, institution_id, properties={
            '@type': 'Organization',
            'name': [{
                '@value': institution['attributes']['name'],
                '@language': 'ja',
            }],
        })
        crate.add(organization)
        return organization

class OSF(OriginalOSF):
    def project(self, project_id):
        """Fetch project `project_id`."""
        type_ = self.guid(project_id)
        url = self._build_url(type_, project_id)
        if type_ in Project._types:
            return Project(self._json(self._get(url), 200), self.session)
        raise OSFException('{} is unrecognized type {}. Clone supports projects and registrations'.format(project_id, type_))


