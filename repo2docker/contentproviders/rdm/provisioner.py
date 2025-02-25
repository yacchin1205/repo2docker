import os
import shlex

from osfclient.models import Project
from osfclient.utils import find_by_path

from .paths import PathMapping


class Provisioner:
    """Class that defines the provisioner for the GakuNin RDM project.
    The /provison.sh script is used to copy and link files to the container based on the source and target definition.
    The script is executed in the container to provision the environment.

    The script is like the following:

    ```bash
    #!bin/bash
    set -xe

    # Copy files from the default storage path to the container
    cp -fr '/mnt/rdm/osfstorage/custom-home-dir/'* '.'

    # Copy files from the googledrive subdir to the container
    mkdir -p ./external/dataset  # Ensure the target directory exists if target is subdir
    cp -fr '/mnt/rdm/googledrive/subdir/'* './external/dataset'

    # Copy specific file from the default storage path to the container
    cp '/mnt/rdm/osfstorage/custom-home-dir/.bashrc' '.'
    mkdir -p './specific-dir/'  # Ensure the target directory exists if target is subdir
    cp '/mnt/rdm/osfstorage/specific-file.txt' './specific-dir/'

    # Copy specific file from the onedrive subdir to the container
    mkdir -p './external/specific-file.txt'  # Ensure the target directory exists if target is subdir
    cp '/mnt/rdm/onedrive/external-specific-file.txt' './external/specific-file.txt'

    # Link files from the default storage path to the container
    ln -s '/mnt/rdm/onedrive/custom-home-dir/' './external/custom-home-dir'
    ```
    """
    def __init__(self, project: Project, default_storage_path: str):
        """Initialize the provisioner."""
        self._project = project
        self._default_storage_path = default_storage_path
        self._copy_mappings = []
        self._link_mappings = []

    async def add_copy_mapping(self, path_mapping: PathMapping):
        """Add a path mapping to copy files to the provisioner."""
        source = await self._resolve_source(path_mapping)
        self._copy_mappings.append((path_mapping, source))

    async def add_link_mapping(self, path_mapping: PathMapping):
        """Add a path mapping to link files to the provisioner."""
        if path_mapping.get_target().rstrip("/") == ".":
            raise ValueError("Target path cannot be '.' for link mapping")
        source = await self._resolve_source(path_mapping)
        self._link_mappings.append((path_mapping, source))

    def save_provision_script(self, script_path: str, source_mount_dir='/mnt/rdm/'):
        """Save the provision script to the specified path."""
        with open(script_path, "w") as f:
            f.write("#!/bin/bash\n")
            f.write("set -xe\n")
            for path_mapping, source in self._copy_mappings:
                source_path = os.path.join(
                    source_mount_dir,
                    path_mapping.get_source(self._default_storage_path)
                )
                target_path = path_mapping.get_target()
                if target_path != "./" and target_path.endswith("/"):
                    # target is directory
                    if target_path.strip("/") != "." and target_path.strip("/") != "":
                        f.write(f"mkdir -p {shlex.quote(target_path)}\n")
                elif target_path != "./" and "/" in target_path:
                    # target is subdir
                    parent_dir = os.path.dirname(target_path)
                    if parent_dir.strip("/") != "." and parent_dir.strip("/") != "":
                        f.write(f"mkdir -p {shlex.quote(parent_dir)}\n")
                if source.path.endswith("/"):
                    # folder
                    if target_path != "." and not target_path.endswith("/"):
                        target_path += "/"
                    if not source_path.endswith("/"):
                        source_path += "/"
                    if target_path.strip("/") != "." and target_path.strip("/") != "":
                        f.write(f"mkdir -p {shlex.quote(target_path)}\n")
                    f.write(f"cp -fr {shlex.quote(source_path)}* {shlex.quote(target_path)}\n")
                else:
                    # file
                    f.write(f"cp {shlex.quote(source_path)} {shlex.quote(target_path)}\n")
            for path_mapping, source in self._link_mappings:
                source_path = os.path.join(
                    source_mount_dir,
                    path_mapping.get_source(self._default_storage_path)
                )
                target_path = path_mapping.get_target()
                if target_path != "./" and "/" in target_path.strip("/"):
                    # target is subdir
                    parent_dir = os.path.dirname(target_path)
                    if parent_dir.strip("/") != "." and parent_dir.strip("/") != "":
                        f.write(f"mkdir -p {shlex.quote(parent_dir)}\n")
                f.write(f"ln -s {shlex.quote(source_path)} {shlex.quote(target_path)}\n")

    async def _resolve_source(self, path_mapping: PathMapping):
        """Validate the path mapping."""
        source = path_mapping.get_source(self._default_storage_path)
        source_storage = None
        source_storage_name = source[:source.index("/")] if "/" in source else source
        async for s in self._project.storages:
            if s.name == source_storage_name:
                source_storage = s
                break
        if source_storage is None:
            raise ValueError(f"Could not find storage {source_storage_name}")
        if "/" not in source:
            return source_storage
        source_folder = await find_by_path(source_storage, source[source.index("/") + 1:])
        if source_folder is None:
            raise ValueError(f"Could not find path {source}")
        return source_folder
