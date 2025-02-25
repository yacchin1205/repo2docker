import logging
from typing import List, Dict


logger = logging.getLogger(__name__)


class PathMapping:
    """Interface that defines a folder mapping from the GakuNin RDM project."""
    def get_source(self, default_storage_path: str) -> str:
        """Get the source path of the folder."""
        raise NotImplementedError

    def get_target(self) -> str:
        """Get the target path of the folder."""
        raise NotImplementedError


class PathMappingImpl(PathMapping):
    """Class that defines a folder mapping from the GakuNin RDM project."""
    def __init__(self, mapping: Dict):
        """Initialize the folder mapping with a dictionary."""
        self._mapping = mapping
        self._validate_mapping()

    def get_type(self) -> str:
        """Get the type of the folder mapping."""
        return self._mapping["type"]

    def get_source(self, default_storage_path: str) -> str:
        return self._mapping["source"].replace("$default_storage_path", default_storage_path).strip("/")

    def get_target(self) -> str:
        return self._mapping["target"].rstrip("/")

    def _validate_mapping(self):
        if "type" not in self._mapping:
            raise ValueError("No type key in mapping.")
        if self._mapping["type"] not in ["copy", "link"]:
            raise ValueError(f"Invalid type in mapping: {self._mapping['type']}")
        if "source" not in self._mapping:
            raise ValueError("No source key in mapping.")
        if "target" not in self._mapping:
            raise ValueError("No target key in mapping.")
        if not self._mapping["target"].startswith("./") and self._mapping["target"] != ".":
            raise ValueError("Target path must be relative to the output directory")


class PathsMapping:
    """Class that defines whether to include or link to folders within the GakuNin RDM project.
    This class defines whether to copy or link folders using the following YAML format file.

    ```
    override: true
    paths:
      - type: copy
        source: $default_storage_path/custom-home-dir
        target: .
      - type: link
        source: /googledrive/subdir
        target: ./external/googledrive
    ```

    The `override` key is a optional boolean value that determines whether to override the default behavior of copying all files.
    By omitting `override`, the default value is `false`.
    The default behavior is to copy all files in the default storage(osfstorage) like the following:

    ```
    paths:
      - type: copy
        source: $default_storage_path
        target: .
    ```

    The `paths` key is a list of dictionaries that define the behavior of each folder.
    The entries in the list are dictionaries with the following keys:
    - `type`: The type of behavior to apply to the folder. This can be either `copy` or `link`.
    - `source`: The source path of the folder. `$default_storage_path` is a special variable that refers to the default storage path.
    - `target`: The target path of the folder. The path should be relative to the output directory.

    The `source` and `target` paths cannot specify files and must be directories.
    """
    def __init__(self, mappings: Dict):
        """Initialize the folder mappings with a dictionary."""
        self._override = mappings.get("override", False)
        if "paths" not in mappings:
            raise ValueError("No paths key in mapping.")
        self._mappings = [PathMappingImpl(mapping) for mapping in mappings["paths"]]

    def get_paths_to_copy(self) -> List[PathMapping]:
        """Get the list of folders to copy."""
        r = [mapping for mapping in self._mappings if mapping.get_type() == "copy"]
        if not self._override:
            return [PathMappingImpl({
                "type": "copy",
                "source": "$default_storage_path",
                "target": ".",
            })] + r
        return r

    def get_paths_to_link(self) -> List[PathMapping]:
        """Get the list of folders to link."""
        return [mapping for mapping in self._mappings if mapping.get_type() == "link"]
