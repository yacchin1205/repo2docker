from urllib.parse import urlparse


class RDMURL:
    """Class that defines a URL for the GakuNin RDM project."""

    def __init__(self, url: str):
        """Initialize the RDM URL with a string."""
        self._url = urlparse(url)

    @property
    def project_id(self) -> str:
        """Get the project ID from the URL."""
        return self._url.path.lstrip("/").split("/")[0]
    
    @property
    def project_path(self) -> str:
        """Get the project path from the URL."""
        if "/" not in self._url.path.lstrip("/"):
            return ""
        _, path = self._url.path.lstrip("/").split("/", 1)
        if not path.startswith("files/"):
            return path
        path = path[len("files/"):]
        if not path.startswith("dir/"):
            return path
        path = path[len("dir/"):]
        return path
