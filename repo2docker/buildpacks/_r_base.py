"""
Base information for using R in BuildPacks.

Keeping this in r.py would lead to cyclic imports.
"""

import shlex

from ..semver import parse_version as V
from ._rstudio import rstudio_server_installer


def rstudio_base_scripts(r_version, rstudio_config):
    """Base steps to install RStudio and shiny-server."""

    # Shiny server (not the package!) seems to be the same version for all R versions
    shiny_server_url = "https://download3.rstudio.org/ubuntu-18.04/x86_64/shiny-server-1.5.22.1017-amd64.deb"
    shiny_proxy_version = "1.4"
    shiny_sha256sum = "0fa40054f038de464a26f3f8c40180a072228454762b7a12ed50568b3256c236"

    rstudio_url, rstudio_sha256sum = rstudio_server_installer(r_version, rstudio_config)
    # the URL comes from external metadata (downloads.json) or is derived
    # from repository-provided rstudio.yml; never splice it in unquoted
    rstudio_url = shlex.quote(rstudio_url)
    if rstudio_sha256sum:
        rstudio_verify = f'echo "{rstudio_sha256sum} /tmp/rstudio.deb" | sha256sum -c -'
    else:
        # user-pinned version without a hash; trust HTTPS
        rstudio_verify = "true"
    rsession_proxy_version = "2.3.0"

    return [
        (
            "root",
            # we should have --no-install-recommends on all our apt-get install commands,
            # but here it's important because these recommend r-base,
            # which will upgrade the installed version of R, undoing our pinned version
            #
            # RStudio's postinst initializes /var/lib/rstudio-server (session-rpc-key
            # etc.) as root; rserver runs as NB_USER and needs to read/write it
            rf"""
            apt-get update > /dev/null && \
            curl --silent --show-error --location --fail {rstudio_url} > /tmp/rstudio.deb && \
            curl --silent --show-error --location --fail {shiny_server_url} > /tmp/shiny.deb && \
            {rstudio_verify} && \
            echo '{shiny_sha256sum} /tmp/shiny.deb' | sha256sum -c - && \
            apt install -y --no-install-recommends /tmp/rstudio.deb /tmp/shiny.deb && \
            chown -R ${{NB_USER}}:${{NB_USER}} /var/lib/rstudio-server && \
            rm /tmp/*.deb && \
            apt-get -qq purge && \
            apt-get -qq clean && \
            rm -rf /var/lib/apt/lists/*
            """,
        ),
        (
            "${NB_USER}",
            # Install jupyter-rsession-proxy
            rf"""
                pip install --no-cache \
                    jupyter-rsession-proxy=={rsession_proxy_version} \
                    jupyter-shiny-proxy=={shiny_proxy_version}
                """,
        ),
        (
            # Not all of these locations are configurable; so we make sure
            # they exist and have the correct permissions
            "root",
            r"""
                install -o ${NB_USER} -g ${NB_USER} -d /var/log/shiny-server && \
                install -o ${NB_USER} -g ${NB_USER} -d /var/lib/shiny-server && \
                install -o ${NB_USER} -g ${NB_USER} /dev/null /var/log/shiny-server.log && \
                install -o ${NB_USER} -g ${NB_USER} /dev/null /var/run/shiny-server.pid
                """,
        ),
    ]
