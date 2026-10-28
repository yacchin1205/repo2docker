"""
Test MATLAB installation scripts
"""

from unittest.mock import patch

import pytest
from requests.models import Response

from repo2docker.app import Repo2Docker
from repo2docker.buildpacks import CondaBuildPack
from repo2docker.buildpacks.conda.matlab import matlab_requirements_scripts

DEPS_URL = "https://raw.githubusercontent.com/mathworks-ref-arch/container-images/main/matlab-deps/r2025b/{}/base-dependencies.txt"


def deps_response():
    r = Response()
    r.status_code = 200
    r._content = b"# comment\nlibasound2t64\nlibxt6t64\n"
    return r


@pytest.mark.parametrize(
    "base_image, ubuntu_name",
    [
        (Repo2Docker.base_image.default_value, "ubuntu24.04"),
        ("docker.io/library/buildpack-deps:jammy", "ubuntu22.04"),
    ],
)
def test_requirements_for_base_image(base_image, ubuntu_name):
    with patch("requests.get", return_value=deps_response()) as get:
        scripts = matlab_requirements_scripts("R2025b", base_image)
    get.assert_called_once_with(DEPS_URL.format(ubuntu_name))
    [(user, script)] = scripts
    assert user == "root"
    assert "libasound2t64 libxt6t64" in script
    assert "# comment" not in script


def test_unknown_base_image():
    with pytest.raises(ValueError):
        matlab_requirements_scripts("R2025b", "buildpack-deps:focal")


def test_conda_assemble_scripts_with_mpm_yml(tmpdir):
    tmpdir.chdir()
    with open("environment.yml", "w") as f:
        f.write("dependencies:\n  - numpy\n")
    with open("mpm.yml", "w") as f:
        f.write("release: R2025b\nproducts:\n  - Simulink\n")

    bp = CondaBuildPack(Repo2Docker.base_image.default_value)
    with patch("requests.get", return_value=deps_response()) as get:
        scripts = bp._get_assemble_scripts_for_matlab()
    get.assert_called_once_with(DEPS_URL.format("ubuntu24.04"))
    joined = "\n".join(script for _, script in scripts)
    assert "--release=R2025b" in joined
    assert "--products MATLAB Simulink" in joined
    assert "python3-distutils" not in joined
    assert "jupyter-matlab-proxy" in joined
    assert bp._get_env_for_matlab() == [
        ("MW_CONTEXT_TAGS", "MATLAB_PROXY:JUPYTER:MPM:V1")
    ]


def test_mpm_yml_requires_release(tmpdir):
    tmpdir.chdir()
    with open("environment.yml", "w") as f:
        f.write("dependencies:\n  - numpy\n")
    with open("mpm.yml", "w") as f:
        f.write("products:\n  - Simulink\n")

    bp = CondaBuildPack(Repo2Docker.base_image.default_value)
    with pytest.raises(ValueError):
        bp._get_assemble_scripts_for_matlab()
