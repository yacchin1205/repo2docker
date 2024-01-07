class CrateFetcher:
    """Abstract Class for loading a crate from a given source"""
    
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
        raise NotImplementedError()

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
        raise NotImplementedError()

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
