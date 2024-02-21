"""BuildPack for conda environments"""
import os
import re
import warnings
from collections.abc import Mapping
from functools import lru_cache

import requests
from ruamel.yaml import YAML

from ...semver import parse_version as V
from ...utils import is_local_pip_requirement
from .._r_base import rstudio_base_scripts
from ..base import BaseImage
from .matlab import (
    matlab_requirements_scripts,
    matlab_installation_scripts,
    matlab_python_engine_installation_scripts,
    matlab_proxy_installation_scripts,
)

# pattern for parsing conda dependency line
PYTHON_REGEX = re.compile(r"python\s*=+\s*([\d\.]*)")
R_REGEX = re.compile(r"r-base\s*=+\s*([\d\.]*)")
# current directory
HERE = os.path.dirname(os.path.abspath(__file__))


class CondaBuildPack(BaseImage):
    """A conda BuildPack.

    Uses miniconda since it is more lightweight than Anaconda.

    """

    # The kernel conda environment file, if any.
    # As an absolute path within the container.
    _kernel_environment_file = ""
    # extra pip requirements.txt for the kernel
    _kernel_requirements_file = ""

    # The notebook server environment file.
    # As an absolute path within the container.
    _nb_environment_file = ""
    # extra pip requirements.txt for the notebook env
    _nb_requirements_file = ""

    def _conda_platform(self):
        """Return the conda platform name for the current platform"""
        if self.platform == "linux/amd64":
            return "linux-64"
        if self.platform == "linux/arm64":
            return "linux-aarch64"
        raise ValueError(f"Unknown platform {self.platform}")

    @lru_cache()
    def get_build_env(self):
        """Return environment variables to be set.

        We set `CONDA_DIR` to the conda install directory and
        the `NB_PYTHON_PREFIX` to the location of the jupyter binary.

        """
        if not self._nb_environment_file:
            # get_build_script_files locates requirements/environment files,
            # populating the _nb_environment_file attribute and others.
            # FIXME: move file detection and initialization of those attributes to its own step?
            self.get_build_script_files()

        env = super().get_build_env() + [
            ("CONDA_DIR", "${APP_BASE}/conda"),
            ("NB_PYTHON_PREFIX", "${CONDA_DIR}/envs/notebook"),
            # We install npm / node from conda-forge
            ("NPM_DIR", "${APP_BASE}/npm"),
            ("NPM_CONFIG_GLOBALCONFIG", "${NPM_DIR}/npmrc"),
            ("NB_ENVIRONMENT_FILE", self._nb_environment_file),
            ("MAMBA_ROOT_PREFIX", "${CONDA_DIR}"),
            # this exe should be used for installs after bootstrap with micromamba
            # switch this to /usr/local/bin/micromamba to use it for all installs
            ("MAMBA_EXE", "${CONDA_DIR}/bin/mamba"),
            ("CONDA_PLATFORM", self._conda_platform()),
        ]
        if self._nb_requirements_file:
            env.append(("NB_REQUIREMENTS_FILE", self._nb_requirements_file))

        if self._kernel_environment_file:
            # if kernel environment file is separate
            env.extend(
                [
                    ("KERNEL_PYTHON_PREFIX", "${CONDA_DIR}/envs/kernel"),
                    ("KERNEL_ENVIRONMENT_FILE", self._kernel_environment_file),
                ]
            )
            if self._kernel_requirements_file:
                env.append(("KERNEL_REQUIREMENTS_FILE", self._kernel_requirements_file))
        else:
            env.append(("KERNEL_PYTHON_PREFIX", "${NB_PYTHON_PREFIX}"))
        return env

    @lru_cache()
    def get_env(self):
        """Make kernel env the default for `conda install`"""
        env = super().get_env() + [("CONDA_DEFAULT_ENV", "${KERNEL_PYTHON_PREFIX}")]
        env += self._get_env_for_matlab()
        return env

    @lru_cache()
    def get_path(self):
        """Return paths (including conda environment path) to be added to
        the PATH environment variable.

        """
        path = super().get_path()
        path.insert(0, "${CONDA_DIR}/bin")
        if self.separate_kernel_env:
            path.insert(0, "${KERNEL_PYTHON_PREFIX}/bin")
        path.insert(0, "${NB_PYTHON_PREFIX}/bin")
        # This is at the end of $PATH, for backwards compat reasons
        path.append("${NPM_DIR}/bin")
        return path

    @lru_cache()
    def get_build_scripts(self):
        """
        Return series of build-steps common to all Python 3 repositories.

        All scripts here should be independent of contents of the repository.

        This sets up through `install-base-env.bash` (found in this directory):

        - a directory for the conda environment and its ownership by the
          notebook user
        - a Python 3 interpreter for the conda environment
        - a Python 3 jupyter kernel
        - a frozen base set of requirements, including:
            - support for Jupyter widgets
            - support for JupyterLab

        """
        return super().get_build_scripts() + [
            (
                "root",
                r"""
                TIMEFORMAT='time: %3R' \
                bash -c 'time /tmp/install-base-env.bash' && \
                rm -rf /tmp/install-base-env.bash /tmp/env
                """,
            ),
            (
                "root",
                r"""
                mkdir -p ${NPM_DIR} && \
                chown -R ${NB_USER}:${NB_USER} ${NPM_DIR}
                """,
            ),
        ]

    major_pythons = {"2": "2.7", "3": "3.10"}

    @lru_cache()
    def get_build_script_files(self):
        """
        Dict of files to be copied to the container image for use in building.

        This is copied before the `build_scripts` & `assemble_scripts` are
        run, so can be executed from either of them.

        It's a dictionary where the key is the source file path in the host
        system, and the value is the destination file path inside the
        container image.

        This currently adds a frozen set of Python requirements to the dict
        of files.

        """
        files = {
            "conda/install-base-env.bash": "/tmp/install-base-env.bash",
            "conda/activate-conda.sh": "/etc/profile.d/activate-conda.sh",
        }
        py_version = self.python_version
        if not py_version or len(py_version.split(".")) != 2:
            raise ValueError(
                f"{self.__class__.__name__}.python_version must always be specified as 'x.y', e.g. '3.10', got {py_version}."
            )
        self.log.info(f"Building conda environment for python={py_version}\n")
        # Select the frozen base environment based on Python version.
        # avoids expensive and possibly conflicting upgrades when changing
        # major Python versions during upgrade.
        conda_platform = self._conda_platform()

        if self.separate_kernel_env:
            # setup kernel environment (separate from server)
            # server runs with default env
            server_py_version = self.major_pythons["3"]
            self.log.warning(
                f"User-requested packages for legacy Python version {py_version} will be installed in a separate kernel environment in $KERNEL_PYTHON_PREFIX.\n"
                f"Jupyter Server will run with {server_py_version} in $NB_PYTHON_PREFIX.\n"
            )
            lockfile_name = f"environment.py-{py_version}-{conda_platform}.lock"
            if not os.path.exists(os.path.join(HERE, lockfile_name)):
                raise ValueError(
                    f"Python version {py_version} on {conda_platform} is not supported!"
                )
            files[f"conda/{lockfile_name}"] = self._kernel_environment_file = (
                "/tmp/env/kernel-environment.lock"
            )

            requirements_file_name = f"requirements.py-{py_version}.pip"
            if os.path.exists(os.path.join(HERE, requirements_file_name)):
                files[f"conda/{requirements_file_name}"] = (
                    self._kernel_requirements_file
                ) = "/tmp/env/kernel-requirements.txt"
        else:
            # server and kernel are the same
            server_py_version = py_version

        # setup the server Python environment
        conda_frozen_name = f"environment.py-{server_py_version}-{conda_platform}.lock"
        pip_frozen_name = f"requirements.py-{server_py_version}.pip"

        if not os.path.exists(os.path.join(HERE, conda_frozen_name)):
            # no env, not supported
            raise ValueError(
                f"Python version {server_py_version} on {conda_platform} is not supported!"
            )

        files["conda/" + conda_frozen_name] = self._nb_environment_file = (
            "/tmp/env/environment.lock"
        )

        # add requirements.txt, if present
        if os.path.exists(os.path.join(HERE, pip_frozen_name)):
            files["conda/" + pip_frozen_name] = self._nb_requirements_file = (
                "/tmp/env/requirements.txt"
            )

        files.update(super().get_build_script_files())
        return files

    _environment_yaml = None

    @property
    def environment_yaml(self):
        if self._environment_yaml is not None:
            return self._environment_yaml

        environment_yml = self.binder_path("environment.yml")
        if not os.path.exists(environment_yml):
            self._environment_yaml = {}
            return self._environment_yaml

        with open(environment_yml) as f:
            env = YAML().load(f)
            # check if the env file is empty, if so instantiate an empty dictionary.
            if env is None:
                env = {}
            # check if the env file provided a dict-like thing not a list or other data structure.
            if not isinstance(env, Mapping):
                raise TypeError(
                    "environment.yml should contain a dictionary. Got %r" % type(env)
                )
            self._environment_yaml = env

        return self._environment_yaml

    @property
    def _should_preassemble_env(self):
        """Check for local pip requirements in environment.yaml

        If there are any local references, e.g. `-e .`,
        stage the whole repo prior to installation.
        """
        dependencies = self.environment_yaml.get("dependencies", [])
        pip_requirements = None
        for dep in dependencies:
            if isinstance(dep, dict) and dep.get("pip"):
                pip_requirements = dep["pip"]
        if isinstance(pip_requirements, list):
            for line in pip_requirements:
                if is_local_pip_requirement(line):
                    return False
        return True

    @property
    def python_version(self):
        """Detect the Python version for a given `environment.yml`

        Will always return an `x.y` version.
        If no version is found, the default Python version is used,
        via self.major_pythons['3'].

        Version information below the minor level is dropped.
        """
        if not hasattr(self, "_python_version"):
            py_version = None
            env = self.environment_yaml
            for dep in env.get("dependencies", []):
                if not isinstance(dep, str):
                    continue
                match = PYTHON_REGEX.match(dep)
                if not match:
                    continue
                py_version = match.group(1)
                break

            # extract major.minor
            if py_version:
                py_version_info = py_version.split(".")
                if len(py_version_info) == 1:
                    self._python_version = self.major_pythons[py_version_info[0]]
                else:
                    # return major.minor
                    self._python_version = ".".join(py_version_info[:2])
            else:
                self._python_version = self.major_pythons["3"]
                self.log.warning(
                    f"Python version unspecified, using current default Python version {self._python_version}. This will change in the future."
                )

        return self._python_version

    @property
    def r_version(self):
        """Detect the R version for a given `environment.yml`

        Will return 'x.y.z' if version is found (e.g '4.1.1'),
        or a Falsy empty string '' if not found.

        """
        if not hasattr(self, "_r_version"):
            self._r_version = ""
            env = self.environment_yaml
            for dep in env.get("dependencies", []):
                if not isinstance(dep, str):
                    continue
                match = R_REGEX.match(dep)
                if not match:
                    continue
                self._r_version = match.group(1)
                break

        return self._r_version

    @property
    def uses_r(self):
        """Detect whether the user also installs R packages.

        Will return True when a package prefixed with 'r-' is being installed.
        """
        if not hasattr(self, "_uses_r"):
            deps = self.environment_yaml.get("dependencies", [])
            self._uses_r = False
            for dep in deps:
                if not isinstance(dep, str):
                    continue
                if dep.startswith("r-"):
                    self._uses_r = True
                    break

        return self._uses_r

    @property
    def py2(self):
        """Am I building a Python 2 kernel environment?"""
        warnings.warn(
            "CondaBuildPack.py2 is deprecated in 2023.2. Use CondaBuildPack.separate_kernel_env.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.python_version and self.python_version.split(".")[0] == "2"

    # Python versions _older_ than this get a separate kernel env
    kernel_env_cutoff_version = "3.7"

    @property
    def separate_kernel_env(self):
        """Whether the kernel should be installed into a separate env from the server

        Applies to older versions of Python that aren't kept up-to-date
        """
        return self.python_version and V(self.python_version) < V(
            self.kernel_env_cutoff_version
        )

    @lru_cache()
    def get_preassemble_script_files(self):
        """preassembly only requires environment.yml

        enables caching assembly result even when
        repo contents change
        """
        assemble_files = super().get_preassemble_script_files()
        if self._should_preassemble_env:
            environment_yml = self.binder_path("environment.yml")
            if os.path.exists(environment_yml):
                assemble_files[environment_yml] = environment_yml
        return assemble_files

    @lru_cache()
    def get_env_scripts(self):
        """Return series of build-steps specific to this source repository."""
        scripts = []
        environment_yml = self.binder_path("environment.yml")
        env_prefix = (
            "${KERNEL_PYTHON_PREFIX}"
            if self.separate_kernel_env
            else "${NB_PYTHON_PREFIX}"
        )
        if os.path.exists(environment_yml):
            # TODO: when using micromamba, we call $MAMBA_EXE install -p ...
            # whereas mamba/conda need `env update -p ...` when it's an env.yaml file
            scripts.append(
                (
                    "${NB_USER}",
                    rf"""
                TIMEFORMAT='time: %3R' \
                bash -c 'time ${{MAMBA_EXE}} env update -p {env_prefix} --file "{environment_yml}" && \
                time ${{MAMBA_EXE}} clean --all -f -y && \
                ${{MAMBA_EXE}} list -p {env_prefix} \
                '
                """,
                )
            )

        if self.uses_r:
            if self.r_version:
                r_pin = "=" + self.r_version
            else:
                r_pin = ""
            scripts.append(
                (
                    "${NB_USER}",
                    rf"""
                ${{MAMBA_EXE}} install -p {env_prefix} r-base{r_pin} r-irkernel r-devtools -y && \
                ${{MAMBA_EXE}} clean --all -f -y && \
                ${{MAMBA_EXE}} list -p {env_prefix}
                """,
                )
            )
            if self.platform != "linux/amd64":
                raise RuntimeError(
                    f"RStudio is only available for linux/amd64 ({self.platform})"
                )
            scripts += rstudio_base_scripts(self.r_version)
            scripts += [
                (
                    "root",
                    rf"""
                    echo auth-none=1 >> /etc/rstudio/rserver.conf && \
                    echo auth-minimum-user-id=0 >> /etc/rstudio/rserver.conf && \
                    echo "rsession-which-r={env_prefix}/bin/R" >> /etc/rstudio/rserver.conf && \
                    echo "rsession-ld-library-path={env_prefix}/lib" >> /etc/rstudio/rserver.conf && \
                    echo www-frame-origin=same >> /etc/rstudio/rserver.conf
                    """,
                ),
                (
                    "${NB_USER}",
                    # Register the jupyter kernel
                    rf"""
                 R --quiet -e "IRkernel::installspec(prefix='{env_prefix}')"
                 """,
                ),
            ]
        return scripts

    @lru_cache()
    def get_preassemble_scripts(self):
        scripts = super().get_preassemble_scripts()
        if self._should_preassemble_env:
            scripts.extend(self.get_env_scripts())
        return scripts

    @lru_cache()
    def get_assemble_scripts(self):
        scripts = super().get_assemble_scripts()
        if not self._should_preassemble_env:
            scripts.extend(self.get_env_scripts())
        scripts += self._get_assemble_scripts_for_r()
        scripts += self._get_assemble_scripts_for_matlab()
        return scripts

    def _get_assemble_scripts_for_r(self):
        installR_path = self.binder_path("install.R")
        if not os.path.exists(installR_path):
            return []
        repo_url = 'https://cran.ism.ac.jp/'
        return [
            (
                "${NB_USER}",
                # Delete any downloaded packages in /tmp, as they aren't reused by R
                f"""echo 'r = getOption("repos")' > /tmp/install.R && \
                echo 'r["CRAN"] = "{repo_url}"' >> /tmp/install.R && \
                echo 'options(repos = r)' >> /tmp/install.R && \
                cat {installR_path} >> /tmp/install.R && \
                Rscript /tmp/install.R && rm -rf /tmp/downloaded_packages""",
            )
        ]

    def _get_matlab_yaml(self):
        install_matlab_path = self.binder_path("mpm.yml")
        if not os.path.exists(install_matlab_path):
            return None
        with open(install_matlab_path) as f:
            return YAML().load(f)

    def _get_assemble_scripts_for_matlab(self):
        config = self._get_matlab_yaml()
        if config is None:
            return []
        if "release" not in config:
            raise ValueError("mpm.yml must contain a 'release' field")
        scripts = matlab_requirements_scripts(config["release"], self.base_image)
        matlab_dir = "/opt/matlab"
        scripts += matlab_installation_scripts(config["release"], config.get("products", []), matlab_dir)
        scripts += matlab_python_engine_installation_scripts(config["release"], matlab_dir)
        scripts += matlab_proxy_installation_scripts()
        return scripts

    def _get_env_for_matlab(self):
        config = self._get_matlab_yaml()
        if config is None:
            return []
        return [("MW_CONTEXT_TAGS", "MATLAB_PROXY:JUPYTER:MPM:V1")]

    def _get_jlab_extension_script(
        self, grdm_jlab_release_tag, grdm_jlab_filename_body, jupyter_resource_usage_tag,
        perform_jlpm_cache_clean=True,
    ):
        grdm_jlab_release_url = (
            f"https://github.com/RCOSDP/CS-jupyterlab-grdm/releases/download/{grdm_jlab_release_tag}"
        )
        jupyter_resource_usage_release_url = "https://github.com/RCOSDP/CS-jupyter-resource-usage"
        grdm_jlab_filename_tar_gz = f"{grdm_jlab_filename_body}.tar.gz"
        grdm_jlab_filename_tgz = "{}.tgz".format(grdm_jlab_filename_body.replace("_", "-"))
        jlpm_cache_clean = ""
        if perform_jlpm_cache_clean:
            jlpm_cache_clean = "jlpm cache clean"
        jlab_ext_scripts = f"""
pip3 install --no-cache-dir {grdm_jlab_release_url}/{grdm_jlab_filename_tar_gz}
pip3 install --no-cache-dir git+{jupyter_resource_usage_release_url}@{jupyter_resource_usage_tag}
jupyter labextension install {grdm_jlab_release_url}/{grdm_jlab_filename_tgz}
jupyter labextension enable rdm-binderhub-jlabextension
jupyter server extension enable rdm_binderhub_jlabextension
jupyter nbextension install --py rdm_binderhub_jlabextension --user
jupyter nbextension enable --py rdm_binderhub_jlabextension --user
jupyter labextension enable jupyter_resource_usage
jupyter serverextension enable --py jupyter_resource_usage
jupyter nbextension install --py jupyter_resource_usage --user
jupyter nbextension enable --py jupyter_resource_usage --user
{jlpm_cache_clean}
npm cache clean --force
pip3 cache purge
rm -fr ~/.cache/pip
"""
        return " && ".join(
            [
                line.strip()
                for line in jlab_ext_scripts.split("\n")
                if len(line.strip()) > 0
            ]
        )

    def get_custom_extension_script(self, post):
        grdm_jlab3_release_tag = "0.2.0"
        grdm_jlab3_filename_body = f"rdm_binderhub_jlabextension-refs.tags.{grdm_jlab3_release_tag}"

        grdm_jlab4_release_tag = "0.3.0"
        grdm_jlab4_filename_body = f"rdm_binderhub_jlabextension-{grdm_jlab4_release_tag}"

        jlab3_ext_script = self._get_jlab_extension_script(
            grdm_jlab3_release_tag, grdm_jlab3_filename_body, 'v2023.07',
        )
        jlab4_ext_script = self._get_jlab_extension_script(
            grdm_jlab4_release_tag, grdm_jlab4_filename_body, 'v2024.04',
            perform_jlpm_cache_clean=False,
        )

        if post:
            install_grdm = 'if(\\"devtools\\" %in% rownames(installed.packages())) ' \
                + 'devtools::install_github(\\"RCOSDP/CS-rstudio-grdm\\", type = \\"source\\")'
            bash_scripts = f"""
if [ -x \\"$(command -v R)\\" ]; then R -e '{install_grdm}'; fi
"""
        else:
            bash_scripts = f"""
[ -x \\"$(command -v pip3)\\" ]
jupyter lab --version
env DISABLE_V8_COMPILE_CACHE=1 bash -c 'if [ \\"$(jupyter lab --version | cut -d . -f 1)\\" -eq 3 ]; then {jlab3_ext_script}; fi'
env DISABLE_V8_COMPILE_CACHE=1 bash -c 'if [ \\"$(jupyter lab --version | cut -d . -f 1)\\" -eq 4 ]; then {jlab4_ext_script}; fi'
"""
        return " && ".join(
            [line.strip() for line in bash_scripts.split("\n") if len(line.strip()) > 0]
        )

    def detect(self):
        """Check if current repo should be built with the Conda BuildPack."""
        return os.path.exists(self.binder_path("environment.yml")) and super().detect()
