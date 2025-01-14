import asyncio
import os
import json
import re
from tempfile import TemporaryDirectory, NamedTemporaryFile

from unittest.mock import patch, MagicMock, PropertyMock, AsyncMock

from osfclient.api import OSF
from repo2docker.contentproviders import RDM


def FutureWrapper(value=None):
    f = asyncio.Future()
    f.set_result(value)
    return f


def AsyncIterator(items):
    mock = AsyncMock()
    mock.__aiter__.return_value = items
    return mock


def MockFile(name):
    mock = MagicMock(name=f"File-{name}", path=name)
    path = PropertyMock(return_value=name)
    type(mock).path = path
    mock._path_mock = path
    name = PropertyMock(return_value=name.split("/")[-1])
    type(mock).name = name
    mock._name_mock = name
    hashes_dict = dict(md5="0" * 32, sha256="0" * 64)
    hashes = PropertyMock(return_value=hashes_dict)
    type(mock).hashes = hashes
    mock._hashes_mock = hashes
    mock.write_to = MagicMock(return_value=FutureWrapper())
    mock.move_to = MagicMock(return_value=FutureWrapper())
    mock.remove = MagicMock(return_value=FutureWrapper())
    return mock


def MockFolder(name, files=None, folders=None):
    mock = MagicMock(
        name=f"Folder-{name}",
        path=name,
        files=AsyncIterator(files or []),
        folders=AsyncIterator(folders or []),
        children=AsyncIterator((files or []) + (folders or [])))
    path = PropertyMock(return_value=name)
    type(mock).path = path
    mock._path_mock = path
    name = PropertyMock(return_value=name.rstrip("/").split("/")[-1])
    type(mock).name = name
    mock._name_mock = name
    mock.move_to = MagicMock(return_value=FutureWrapper())
    mock.create_file = MagicMock(return_value=FutureWrapper())
    mock.create_folder = MagicMock(side_effect=lambda name: FutureWrapper(mock))
    mock.remove = MagicMock(return_value=FutureWrapper())
    return mock


def test_detect_rdm_url():
    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == ""
    assert re.match(r"^[0-9A-Fa-f\-]+$", spec["uuid"]) is not None
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"

    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234/files/test/xxx", "X1234")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == "test/xxx"
    assert spec["uuid"] == "X1234"
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"

    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234/test/xxx", "A5678")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == "test/xxx"
    assert spec["uuid"] == "A5678"
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"

    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234/files/test", "X1234")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == "test"
    assert spec["uuid"] == "X1234"
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"

    rdm = RDM()
    spec = rdm.detect("https://test.some.host.nii.ac.jp/x1234/files/test", "HEAD")

    assert spec is not None, spec
    assert spec["project_id"] == "x1234"
    assert spec["path"] == "test"
    assert re.match(r"^[0-9A-Fa-f\-]+$", spec["uuid"]) is not None
    assert spec["host"]["api"] == "https://api.test.some.host.nii.ac.jp/v2/"


def test_not_detect_rdm_url():
    rdm = RDM()
    spec = rdm.detect("https://unknown.some.host.nii.ac.jp/x1234")

    assert spec is None, spec


def test_detect_external_rdm_url():
    with NamedTemporaryFile("w+") as f:
        try:
            f.write(
                json.dumps(
                    [
                        {
                            "hostname": [
                                "https://test1.some.host.nii.ac.jp/",
                            ],
                            "api": "https://api.test1.some.host.nii.ac.jp/v2/",
                        }
                    ]
                )
            )
            f.flush()
            os.environ["RDM_HOSTS"] = f.name

            rdm = RDM()
            spec = rdm.detect("https://test1.some.host.nii.ac.jp/x1234")

            assert spec is not None, spec
            assert spec["project_id"] == "x1234"
            assert spec["path"] == ""
            assert spec["host"]["api"] == "https://api.test1.some.host.nii.ac.jp/v2/"

            rdm = RDM()
            spec = rdm.detect(
                "https://test1.some.host.nii.ac.jp/x1234/files/test/xxx", ""
            )

            assert spec is not None, spec
            assert spec["project_id"] == "x1234"
            assert spec["path"] == "test/xxx"
            assert spec["host"]["api"] == "https://api.test1.some.host.nii.ac.jp/v2/"

            rdm = RDM()
            spec = rdm.detect("https://test1.some.host.nii.ac.jp/x1234/test/xxx", "")

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
        spec = {
            "project_id": "x1234",
            "path": "",
            "host": {"api": "https://test.some.host/v2/"},
        }
        with patch.object(OSF, "project") as fake_project:
            fake_file1 = MockFile("/file1.txt")
            fake_file2 = MockFile("/test/file2.txt")
            fake_storage1 = MagicMock(files=AsyncIterator([fake_file1]))
            fake_storage1.name = "samplestorage1"
            fake_folder2 = MockFolder("/test/", files=[fake_file2])
            fake_storage2 = MagicMock(name="samplestorage2", folders=AsyncIterator([fake_folder2]))
            fake_storage2.name = "samplestorage2"
            fake_project_obj = MagicMock(storages=AsyncIterator([fake_storage1, fake_storage2]))
            fake_project.return_value = fake_project_obj
            fetches = 0
            for msg in rdm.fetch(spec, d):
                assert msg.endswith("\n"), msg
                if msg.startswith("Fetching"):
                    assert "x1234 at https://test.some.host/v2" in msg
                elif msg.startswith("Fetch:") and "/file1.txt" in msg:
                    assert "(samplestorage1/file1.txt to {})".format(d) in msg
                    fetches += 1
                elif msg.startswith("Fetch:") and "/test/file2.txt" in msg:
                    assert "(samplestorage2/test/file2.txt to {})".format(d) in msg
                    fetches += 1
                else:
                    assert False, msg
            assert fetches == 2, fetches

        rdm = RDM()
        spec = {
            "project_id": "x1234",
            "path": "samplestorage2/test",
            "host": {"api": "https://test.some.host/v2/"},
        }
        with patch.object(OSF, "project") as fake_project:
            fake_file1 = MockFile("/file1.txt")
            fake_file2 = MockFile("/test/file2.txt")
            fake_folder2 = MockFolder("/test/", files=[fake_file2])
            fake_storage1 = MagicMock(name="samplestorage1", files=AsyncIterator([fake_file1]))
            fake_storage1.name = "samplestorage1"
            fake_storage2 = MagicMock(
                name="samplestorage2",
                folders=AsyncIterator([fake_folder2]),
                children=AsyncIterator([fake_folder2]),
            )
            fake_storage2.name = "samplestorage2"
            fake_storage = MagicMock()
            fake_storage.return_value = FutureWrapper(fake_storage2)
            fake_project_obj = MagicMock(
                storages=AsyncIterator([fake_storage1, fake_storage2]), storage=fake_storage
            )
            fake_project.return_value = fake_project_obj
            fetches = 0
            for msg in rdm.fetch(spec, d):
                assert msg.endswith("\n"), msg
                if msg.startswith("Fetching"):
                    assert "x1234 at https://test.some.host/v2" in msg
                elif msg.startswith("Fetch:") and "/file2.txt" in msg:
                    assert "(file2.txt to {})".format(d) in msg
                    fetches += 1
                else:
                    assert False, msg
            assert fetches == 1, fetches
            fake_storage.assert_called_once_with("samplestorage2")

        rdm = RDM()
        spec = {
            "project_id": "x1234",
            "path": "samplestorage1",
            "host": {"api": "https://test.some.host/v2/"},
        }
        with patch.object(OSF, "project") as fake_project:
            fake_file1 = MockFile("/file1.txt")
            fake_file2 = MockFile("/test/file2.txt")
            fake_storage1 = MagicMock(name="samplestorage1", files=AsyncIterator([fake_file1]))
            fake_storage1.name = "samplestorage1"
            fake_folder2 = MockFolder("/test/", files=[fake_file2])
            fake_storage2 = MagicMock(name="samplestorage2", folders=AsyncIterator([fake_folder2]))
            fake_storage2.name = "samplestorage2"
            fake_storage = MagicMock()
            fake_storage.return_value = FutureWrapper(fake_storage1)
            fake_project_obj = MagicMock(
                storages=AsyncIterator([fake_storage1, fake_storage2]), storage=fake_storage
            )
            fake_project.return_value = fake_project_obj
            fetches = 0
            for msg in rdm.fetch(spec, d):
                assert msg.endswith("\n"), msg
                if msg.startswith("Fetching"):
                    assert "x1234 at https://test.some.host/v2" in msg
                elif msg.startswith("Fetch:") and "/file1.txt" in msg:
                    assert "(file1.txt to {})".format(d) in msg
                    fetches += 1
                else:
                    assert False, msg
            assert fetches == 1, fetches
            fake_storage.assert_called_once_with("samplestorage1")
