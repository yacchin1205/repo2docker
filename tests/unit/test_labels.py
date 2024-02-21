"""
Test if labels are supplied correctly to the container
"""

from unittest.mock import Mock

import pytest

from repo2docker import __version__
from repo2docker.app import Repo2Docker
from repo2docker.buildpacks import BuildPack

URL = "https://github.com/binderhub-ci-repos/repo2docker-ci-clone-depth"


def test_buildpack_labels_rendered(base_image):
    bp = BuildPack(base_image)
    assert "LABEL" not in bp.render()
    bp.labels["first_label"] = "firstlabel"
    assert 'LABEL first_label="firstlabel"\n' in bp.render()
    bp.labels["second_label"] = "anotherlabel"
    assert 'LABEL second_label="anotherlabel"\n' in bp.render()


@pytest.mark.parametrize(
    "ref, repo, expected_repo_label",
    [(None, URL, URL), ("some-ref", None, "local"), (None, None, "local")],
)
def test_Repo2Docker_labels(ref, repo, expected_repo_label, tmpdir):
    app = Repo2Docker(dry_run=True)
    # Add mock BuildPack to app
    mock_buildpack = Mock()
    mock_buildpack.return_value.labels = {}
    app.buildpacks = [mock_buildpack]

    if repo is None:
        repo = str(tmpdir)
    app.repo = repo
    if ref is not None:
        app.ref = ref

    app.initialize()
    app.start()
    expected_labels = {
        "repo2docker.ref": ref,
        "repo2docker.repo": expected_repo_label,
        "repo2docker.version": __version__,
    }

    assert mock_buildpack().labels == expected_labels
