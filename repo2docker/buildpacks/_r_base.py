"""
Base information for using R in BuildPacks.

Keeping this in r.py would lead to cyclic imports.
"""

from ..semver import parse_version as V


def rstudio_base_scripts(r_version):
    """Base steps to install RStudio and shiny-server."""

    # Shiny server (not the package!) seems to be the same version for all R versions
    shiny_server_url = "https://download3.rstudio.org/ubuntu-14.04/x86_64/shiny-server-1.5.17.973-amd64.deb"
    shiny_proxy_version = "1.1"
    shiny_sha256sum = "80f1e48f6c824be7ef9c843bb7911d4981ac7e8a963e0eff823936a8b28476ee"

    # RStudio server has different builds based on wether OpenSSL 3 or 1.1 is available in the base
    # image. 3 is present Jammy+, 1.1 until then. Instead of hardcoding URLs based on distro, we actually
    # check for the dependency itself directly in the code below. You can find these URLs in
    # https://posit.co/download/rstudio-server/, toggling between Ubuntu 22 (for openssl3) vs earlier versions (openssl 1.1)
    # you may forget about openssl, but openssl never forgets you.

    # RStudio server 2023.12.1 seems to be provided for only Ubuntu 20.04 and later
    # and it does not work with Ubuntu 18.04(bionic)
    rstudio_url = "https://download2.rstudio.org/server/jammy/amd64/rstudio-server-2023.12.1-402-amd64.deb"
    rstudio_sha256sum = (
        "2ceeebe5d1d77068b36e85f7cf366cd1409f7642a80261b6bbeb3da945ef0888"
    )
    rsession_proxy_version = "2.2.0"

    return [
        (
            "root",
            # we should have --no-install-recommends on all our apt-get install commands,
            # but here it's important because these recommend r-base,
            # which will upgrade the installed version of R, undoing our pinned version
            rf"""
            apt-get update > /dev/null && \
            RSTUDIO_URL="{rstudio_url}" && \
            RSTUDIO_HASH="{rstudio_sha256sum}"  && \
            curl --silent --location --fail ${{RSTUDIO_URL}} > /tmp/rstudio.deb && \
            curl --silent --location --fail {shiny_server_url} > /tmp/shiny.deb && \
            echo "${{RSTUDIO_HASH}} /tmp/rstudio.deb" | sha256sum -c - && \
            echo '{shiny_sha256sum} /tmp/shiny.deb' | sha256sum -c - && \
            apt install -y --no-install-recommends /tmp/rstudio.deb /tmp/shiny.deb && \
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
