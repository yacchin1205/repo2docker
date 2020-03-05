import os
import json
from tempfile import TemporaryDirectory, NamedTemporaryFile

from unittest.mock import patch

from osfclient.api import OSF
from repo2docker.contentproviders import RDM


def test_detect_rdm_url():
    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == ""
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"

    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234", "test/xxx")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == "test/xxx"
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"

    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234", "/test/xxx")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == "test/xxx"
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"

    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234/", "/test/xxx")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == "test/xxx"
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"


def test_not_detect_rdm_url():
    rdm = RDM()
    spec = rdm.detect("https://unknown.some.host.nii.ac.jp/x1234")

    assert spec is None, spec


def test_detect_external_rdm_url():
    with NamedTemporaryFile('w+') as f:
        try:
            f.write(json.dumps([
                {
                    "hostname": [
                        "https://test1.some.host.nii.ac.jp/",
                    ],
                    "api": "https://api.test1.some.host.nii.ac.jp/v2/",
                }
            ]))
            f.flush()
            os.environ["RDM_HOSTS"] = f.name

            rdm = RDM()
            spec = rdm.detect("https://test1.some.host.nii.ac.jp/x1234")

            assert spec is not None, spec
            assert spec["project_id"] == "x1234"
            assert spec["path"] == ""
            assert spec["host"]["api"] == "https://api.test1.some.host.nii.ac.jp/v2/"

            rdm = RDM()
            spec = rdm.detect("https://test1.some.host.nii.ac.jp/x1234", "test/xxx")

            assert spec is not None, spec
            assert spec["project_id"] == "x1234"
            assert spec["path"] == "test/xxx"
            assert spec["host"]["api"] == "https://api.test1.some.host.nii.ac.jp/v2/"

            rdm = RDM()
            spec = rdm.detect("https://test1.some.host.nii.ac.jp/x1234", "/test/xxx")

            assert spec is not None, spec
            assert spec["project_id"] == "x1234"
            assert spec["path"] == "test/xxx"
            assert spec["host"]["api"] == "https://api.test1.some.host.nii.ac.jp/v2/"
        finally:
            del os.environ["RDM_HOSTS"]


def test_content_id_is_unique():
    rdm1 = RDM()
    rdm1.detect("https://test.some.host.nii.ac.jp/x1234")
    rdm2 = RDM()
    rdm2.detect("https://test.some.host.nii.ac.jp/y5678")
    assert rdm1.content_id != rdm2.content_id

    rdm1 = RDM()
    rdm1.detect("https://test.some.host.nii.ac.jp/x1234")
    rdm2 = RDM()
    rdm2.detect("https://test.some.host.nii.ac.jp/x1234")
    assert rdm1.content_id != rdm2.content_id


def test_fetch_content():
    with TemporaryDirectory() as d:
        rdm = RDM()
        spec = {"project_id": "x1234", "path": "", "host": {"api": "https://test.some.host/v2/"}}
        with patch.object(OSF, "project") as fake_project:
            for _ in rdm.fetch(spec, d):
                pass
