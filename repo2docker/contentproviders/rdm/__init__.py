import asyncio
import os
import logging
import json
from typing import Any, Dict, Optional, Union
import shutil
import tempfile
import uuid
from ruamel.yaml import YAML

from ..base import ContentProvider

import aiofiles
from osfclient.api import OSF
from osfclient.models import Project, Folder, Storage
from osfclient.utils import find_by_path
from .paths import PathsMapping
from .provisioner import Provisioner
from .hash import compute_directory_hash
from .url import RDMURL


logger = logging.getLogger(__name__)


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
                u = RDMURL(source)
                self.project_id = u.project_id
                self.path = u.project_path
                if self._check_ref_defined(ref):
                    self.uuid = ref
                else:
                    # Calculate the hash of the .binder directory asynchronously
                    loop = asyncio.new_event_loop()
                    queue = asyncio.Queue()
                    loop.create_task(self._calculate_hash_with_error(host, queue))
                    try:
                        result = loop.run_until_complete(queue.get())
                        if isinstance(result, BaseException):
                            raise result
                        if result is None:
                            # No .binder directory found, generate a random UUID
                            self.uuid = str(uuid.uuid1())
                        else:
                            self.uuid = result
                    finally:
                        loop.close()
                logger.debug(
                    "Detected RDM project: {}, path: {}, host: {}, uuid: {}".format(
                        self.project_id, self.path, host, self.uuid
                    )
                )
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
        # Perform the async fetch synchronously
        loop = asyncio.new_event_loop()
        queue = asyncio.Queue()
        loop.create_task(self._fetch_with_error(spec, output_dir, queue))

        try:
            while True:
                result = loop.run_until_complete(queue.get())
                if isinstance(result, BaseException):
                    raise result
                if result is None:
                    break
                yield result
        finally:
            loop.close()

    async def _fetch_with_error(self, spec: Dict[str, Any], output_dir: str, queue: asyncio.Queue):
        try:
            await self._fetch(spec, output_dir, queue)
        except BaseException as e:
            await queue.put(e)
        finally:
            await queue.put(None)

    async def _calculate_hash_with_error(self, host: Dict[str, Any], queue: asyncio.Queue):
        try:
            hash = await self._calculate_hash(host, queue)
            await queue.put(hash)
        except BaseException as e:
            await queue.put(e)
        finally:
            await queue.put(None)

    async def _calculate_hash(self, host: Dict[str, Any], queue: asyncio.Queue):
        """Calculate the hash of the .binder directory asynchronously"""
        api_url = host["api"][:-1] if host["api"].endswith("/") else host["api"]
        osf = OSF(
            token=host["token"] if "token" in host else os.getenv("OSF_TOKEN"),
            base_url=api_url,
        )
        project = await osf.project(self.project_id)
        path = self.path.rstrip("/")
        storage_name = path[:path.index("/")] if "/" in path else path
        storage = await project.storage(storage_name)
        binder_folders = []
        async for folder in storage.folders:
            if folder.name not in [".binder", "binder"]:
                continue
            binder_folders.append(folder)
        if len(binder_folders) == 0:
            return None
        binder_folders = sorted(binder_folders, key=lambda x: 0 if x.name == "binder" else 1)
        work_dir = tempfile.mkdtemp()
        try:
            async for _ in self._fetch_all(binder_folders[0], work_dir, None):
                pass
            hash = compute_directory_hash(work_dir)
            logger.debug(
                "Computed hash for RDM project {} at path {}: {}".format(
                    self.project_id, self.path, hash
                )
            )
            return hash
        finally:
            shutil.rmtree(work_dir)

    async def _fetch(self, spec: Dict[str, Any], output_dir: str, queue: asyncio.Queue):
        project_id = spec["project_id"]
        path = spec["path"]
        host = spec["host"]
        api_url = host["api"][:-1] if host["api"].endswith("/") else host["api"]

        await queue.put("Fetching RDM directory {} on {} at {}.\n".format(
            path, project_id, api_url
        ))
        osf = OSF(
            token=host["token"] if "token" in host else os.getenv("OSF_TOKEN"),
            base_url=api_url,
        )
        project = await osf.project(project_id)

        if len(path):
            path = path.rstrip("/")
            storage_name = path[:path.index("/")] if "/" in path else path
            storage = await project.storage(storage_name)
            if "/" in path:
                storage = await find_by_path(storage, path[path.index("/") + 1:])
                if storage is None:
                    raise RuntimeError(f"Could not find path {path}")
            async for line in self._fetch_binder(project, path, storage, output_dir, None):
                await queue.put(line)
        else:
            async for storage in project.storages:
                async for line in self._fetch_all(storage, output_dir, storage.name):
                    await queue.put(line)

    async def _fetch_binder(
        self,
        project: Project,
        default_storage_path: str,
        storage: Union[Storage, Folder],
        output_dir: str,
        local_dir: Optional[str],
        mnt_rdm_dir: Optional[str] = "/mnt/rdm/",
    ):
        binder_folders = []
        non_binder_folders = []
        async for folder in storage.folders:
            if folder.name in [".binder", "binder"]:
                binder_folders.append(folder)
            else:
                non_binder_folders.append(folder)
        if len(binder_folders) == 0:
            async for line in self._fetch_all(storage, output_dir, None):
                yield line
            return
        binder_folders = sorted(binder_folders, key=lambda x: 0 if x.name == "binder" else 1)

        # Fetch .binder folder first and search for paths.yaml(paths.yml)
        binder_output_dirs = []
        for binder_folder in binder_folders:
            binder_output_dir = os.path.join(output_dir, binder_folder.name)
            binder_output_dirs.append(binder_output_dir)
            os.makedirs(binder_output_dir, exist_ok=True)
        for binder_folder in binder_folders:
            binder_output_dir = os.path.join(output_dir, binder_folder.name)
            async for line in self._fetch_all(binder_folder, binder_output_dir, local_dir):
                yield line
        binder_folders_yaml_candidates = sum(
            [[
                os.path.join(binder_output_dir, "paths.yaml"),
                os.path.join(binder_output_dir, "paths.yml"),
            ] for binder_output_dir in binder_output_dirs],
            [],
        )
        binder_folders_yaml = None
        for binder_folders_yaml_candidate in binder_folders_yaml_candidates:
            if os.path.exists(binder_folders_yaml_candidate):
                binder_folders_yaml = binder_folders_yaml_candidate
                break
        if binder_folders_yaml is None:
            # default configuration
            paths_mapping = PathsMapping({
                'paths': [],
            })
        else:
            yaml = YAML(typ='safe', pure=True)
            with open(binder_folders_yaml) as f:
                paths_mapping = PathsMapping(yaml.load(f))
        provisioner = Provisioner(project, default_storage_path.strip("/"))
        for path_mapping in paths_mapping.get_paths_to_copy():
            source = path_mapping.get_source(default_storage_path.strip("/"))
            target = path_mapping.get_target()
            yield "Mapping(Copy): {} -> {}\n".format(source, target)
            await provisioner.add_copy_mapping(path_mapping)

        for path_mapping in paths_mapping.get_paths_to_link():
            source = path_mapping.get_source(default_storage_path.strip("/"))
            target = path_mapping.get_target()
            yield "Mapping(Link): {} -> {}\n".format(source, target)
            await provisioner.add_link_mapping(path_mapping)

        for binder_output_dir in binder_output_dirs:
            provisioner.save_provision_script(
                os.path.join(binder_output_dir, "provision.sh"),
                mnt_rdm_dir,
            )

    async def _fetch_all(
        self,
        storage: Union[Storage, Folder],
        output_dir: str,
        local_dir: Optional[str],
    ):
        async for file_ in storage.files:
            if "/" in file_.name or "\\" in file_.name:
                raise ValueError(f"File.name cannot include path separators: {file_.name}")
            local_path = os.path.join(local_dir, file_.name) if local_dir is not None else file_.name
            local_dir_path = os.path.join(output_dir, local_dir) if local_dir is not None else output_dir
            local_file_path = os.path.join(output_dir, local_path)
            if os.path.exists(local_file_path):
                yield "Skip: {} ({} to {})\n".format(file_.path, local_path, output_dir)
                continue
            if not os.path.isdir(local_dir_path):
                os.makedirs(local_dir_path)
            async with aiofiles.open(local_file_path, "wb") as f:
                await file_.write_to(f)
            yield "Fetch: {} ({} to {})\n".format(file_.path, local_path, output_dir)
        async for folder in storage.folders:
            if "/" in folder.name or "\\" in folder.name:
                raise ValueError(f"Folder.name cannot include path separators: {folder.name}")
            local_folder_dir = os.path.join(local_dir, folder.name) if local_dir is not None else folder.name
            async for line in self._fetch_all(folder, output_dir, local_folder_dir):
                yield line

    @property
    def content_id(self):
        """Content ID of the RDM directory - this provider identifies repos by random UUID"""
        return "{}-{}".format(self.project_id, self.uuid)
