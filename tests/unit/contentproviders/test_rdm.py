import asyncio
import os
import json
import re
from ruamel.yaml import YAML
import pytest
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
    # Mock _calculate_hash_with_error to avoid network calls
    async def mock_hash_calc(self, host, queue):
        await queue.put("abc123-456-789")

    with patch.object(RDM, "_calculate_hash_with_error", new=mock_hash_calc):
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
    with patch.object(RDM, "_calculate_hash_with_error", new=mock_hash_calc):
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
    # Mock _calculate_hash_with_error to avoid network calls
    async def mock_hash_calc(self, host, queue):
        await queue.put("external-hash-123")

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
            with patch.object(RDM, "_calculate_hash_with_error", new=mock_hash_calc):
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
    # Mock _calculate_hash_with_error to avoid network calls
    # Use a counter to generate different hashes each time
    counter = [0]
    async def mock_hash_calc(self, host, queue):
        counter[0] += 1
        await queue.put(f"unique-hash-{counter[0]}")

    with patch.object(RDM, "_calculate_hash_with_error", new=mock_hash_calc):
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


def test_fetch_with_paths_yaml_generates_correct_provision_script():
    """Test that paths.yaml configuration generates correct provision.sh script"""
    with TemporaryDirectory() as d:
        rdm = RDM()
        spec = {
            "project_id": "x1234",
            "path": "osfstorage",
            "host": {"api": "https://test.some.host/v2/"},
        }

        with patch.object(OSF, "project") as fake_project:
            fake_paths_yaml = MockFile("/binder/paths.yaml")
            fake_binder_folder = MockFolder("/binder/", files=[fake_paths_yaml], folders=[])

            fake_storage = MagicMock(
                name="osfstorage",
                folders=AsyncIterator([fake_binder_folder]),
                files=AsyncIterator([])
            )
            fake_storage.name = "osfstorage"

            async def mock_storage(name):
                return fake_storage

            fake_project_obj = MagicMock(storages=AsyncIterator([fake_storage]))
            fake_project_obj.storage = mock_storage
            fake_project.return_value = fake_project_obj

            binder_dir = os.path.join(d, "binder")
            os.makedirs(binder_dir)
            paths_yaml_content = {
                "override": True,
                "paths": [
                    {
                        "type": "copy",
                        "source": "$default_storage_path/data",
                        "target": "./dataset"
                    },
                    {
                        "type": "link",
                        "source": "external_storage/large_files",
                        "target": "./external"
                    }
                ]
            }
            yaml = YAML(typ='safe', pure=True)
            with open(os.path.join(binder_dir, "paths.yaml"), "w") as f:
                yaml.dump(paths_yaml_content, f)

            # Mock Provisioner._resolve_source to avoid storage validation
            from repo2docker.contentproviders.rdm.provisioner import Provisioner

            async def mock_resolve_source(self, path_mapping):
                # Return different mock objects based on the source
                mock_obj = MagicMock()
                source = path_mapping.get_source(self._default_storage_path)
                if "/" in source:
                    # It's a folder
                    mock_obj.path = source.split("/", 1)[1] + "/"
                else:
                    # It's a storage root
                    mock_obj.path = ""
                return mock_obj

            with patch.object(Provisioner, "_resolve_source", new=mock_resolve_source):

                messages = []
                for msg in rdm.fetch(spec, d):
                    messages.append(msg)

                # Check that provision.sh was created
                provision_script_path = os.path.join(binder_dir, "provision.sh")
                assert os.path.exists(provision_script_path), "provision.sh should be created"

                with open(provision_script_path, 'r') as f:
                    script_content = f.read()

                # Verify script content
                assert "#!/bin/bash" in script_content
                assert "set -xe" in script_content
                assert "cp -fr /mnt/rdm/osfstorage/data/* ./dataset/" in script_content
                assert "ln -s /mnt/rdm/external_storage/large_files ./external" in script_content


def test_fetch_with_binder_but_no_paths_yaml():
    """Test that when binder folder exists but no paths.yaml, uses default behavior"""
    with TemporaryDirectory() as d:
        rdm = RDM()
        spec = {
            "project_id": "x1234", 
            "path": "osfstorage",
            "host": {"api": "https://test.some.host/v2/"},
        }

        with patch.object(OSF, "project") as fake_project:
            # Mock binder folder with other files but no paths.yaml
            fake_env_file = MockFile("/binder/environment.yml")
            fake_binder_folder = MockFolder("/binder/", files=[fake_env_file], folders=[])

            fake_storage = MagicMock(
                name="osfstorage",
                folders=AsyncIterator([fake_binder_folder]),
                files=AsyncIterator([])
            )
            fake_storage.name = "osfstorage"

            async def mock_storage(name):
                return fake_storage

            fake_project_obj = MagicMock(storages=AsyncIterator([fake_storage]))
            fake_project_obj.storage = mock_storage
            fake_project.return_value = fake_project_obj

            # Mock Provisioner._resolve_source to avoid storage validation
            from repo2docker.contentproviders.rdm.provisioner import Provisioner

            async def mock_resolve_source(self, path_mapping):
                mock_obj = MagicMock()
                mock_obj.path = "/"
                return mock_obj

            with patch.object(Provisioner, "_resolve_source", new=mock_resolve_source):
                messages = []
                for msg in rdm.fetch(spec, d):
                    messages.append(msg)

                # Verify binder folder content is fetched
                assert any("environment.yml" in msg for msg in messages)

                # Check that provision.sh was created
                binder_dir = os.path.join(d, "binder")
                provision_script_path = os.path.join(binder_dir, "provision.sh")
                assert os.path.exists(provision_script_path), "provision.sh should be created"

                with open(provision_script_path, 'r') as f:
                    script_content = f.read()

                # Verify default mapping is added (copy entire storage to current directory)
                assert "#!/bin/bash" in script_content
                assert "set -xe" in script_content
                assert "cp -fr /mnt/rdm/osfstorage/* ." in script_content
                # Should not have any link commands
                assert "ln -s" not in script_content


def test_rdmurl_project_id():
    """Test project ID extraction from URL"""
    from repo2docker.contentproviders.rdm.url import RDMURL

    url = RDMURL("https://rdm.example.com/abc123")
    assert url.project_id == "abc123"

    url = RDMURL("https://rdm.example.com/xyz789/files/data")
    assert url.project_id == "xyz789"

    url = RDMURL("https://rdm.example.com/project-1/some/path")
    assert url.project_id == "project-1"


def test_rdmurl_project_path():
    """Test project path extraction from URL"""
    from repo2docker.contentproviders.rdm.url import RDMURL

    # Empty path cases
    url = RDMURL("https://rdm.example.com/abc123")
    assert url.project_path == ""

    url = RDMURL("https://rdm.example.com/abc123/")
    assert url.project_path == ""

    # files/dir prefix
    url = RDMURL("https://rdm.example.com/abc123/files/dir/data/notebook.ipynb")
    assert url.project_path == "data/notebook.ipynb"

    url = RDMURL("https://rdm.example.com/abc123/files/document.pdf")
    assert url.project_path == "document.pdf"

    url = RDMURL("https://rdm.example.com/abc123/files/")
    assert url.project_path == ""

    # Regular paths
    url = RDMURL("https://rdm.example.com/abc123/some/path")
    assert url.project_path == "some/path"

    url = RDMURL("https://rdm.example.com/abc123/data")
    assert url.project_path == "data"


def test_path_mapping_validation():
    """Test PathMapping validation"""
    from repo2docker.contentproviders.rdm.paths import PathMappingImpl

    # Valid mapping
    mapping = PathMappingImpl({
        "type": "copy",
        "source": "data",
        "target": "./dataset"
    })
    assert mapping.get_type() == "copy"
    assert mapping.get_source("osfstorage") == "data"
    assert mapping.get_target() == "./dataset"

    # Missing required field
    with pytest.raises(ValueError, match="No source key"):
        PathMappingImpl({"type": "copy", "target": "."})

    # Invalid target (absolute path)
    with pytest.raises(ValueError, match="must be relative"):
        PathMappingImpl({"type": "copy", "source": "data", "target": "/etc"})


def test_path_mapping_substitution():
    """Test $default_storage_path substitution"""
    from repo2docker.contentproviders.rdm.paths import PathMappingImpl

    mapping = PathMappingImpl({
        "type": "copy",
        "source": "$default_storage_path/subdir",
        "target": "./data"
    })
    assert mapping.get_source("osfstorage") == "osfstorage/subdir"
    assert mapping.get_source("googledrive") == "googledrive/subdir"

    # Without substitution
    mapping = PathMappingImpl({
        "type": "link",
        "source": "external_storage/data",
        "target": "./external"
    })
    assert mapping.get_source("osfstorage") == "external_storage/data"


def test_paths_mapping_override():
    """Test PathsMapping override behavior"""
    from repo2docker.contentproviders.rdm.paths import PathsMapping

    # override: false (default) - adds default mapping
    config = PathsMapping({
        "paths": [{
            "type": "copy",
            "source": "custom/path",
            "target": "./custom"
        }]
    })
    assert config._override == False
    copy_paths = config.get_paths_to_copy()
    assert len(copy_paths) == 2  # Default + custom
    assert copy_paths[0].get_source("osfstorage") == "osfstorage"
    assert copy_paths[0].get_target() == "."
    assert copy_paths[1].get_source("osfstorage") == "custom/path"

    # override: true - no default mapping
    config = PathsMapping({
        "override": True,
        "paths": [{
            "type": "copy",
            "source": "custom/path",
            "target": "./custom"
        }]
    })
    assert config._override == True
    copy_paths = config.get_paths_to_copy()
    assert len(copy_paths) == 1  # Only custom
    assert copy_paths[0].get_source("osfstorage") == "custom/path"


def test_compute_directory_hash():
    """Test directory hash computation with various structures"""
    from repo2docker.contentproviders.rdm.hash import compute_directory_hash

    with TemporaryDirectory() as tmpdir:
        # Test 1: Simple directory with files
        test_dir = os.path.join(tmpdir, "test")
        os.makedirs(test_dir)

        with open(os.path.join(test_dir, "file1.txt"), "w") as f:
            f.write("content1")

        hash1 = compute_directory_hash(test_dir)
        assert len(hash1) == 64  # SHA256 hex digest length

        # Test 2: Same structure produces same hash (deterministic)
        hash2 = compute_directory_hash(test_dir)
        assert hash1 == hash2

        # Test 3: Complex nested structure
        subdir1 = os.path.join(test_dir, "subdir1")
        subdir2 = os.path.join(test_dir, "subdir2")
        nested = os.path.join(subdir1, "nested")
        os.makedirs(subdir1)
        os.makedirs(subdir2)
        os.makedirs(nested)

        with open(os.path.join(subdir1, "file2.txt"), "w") as f:
            f.write("content2")
        with open(os.path.join(subdir2, "file3.txt"), "w") as f:
            f.write("content3")
        with open(os.path.join(nested, "deep.txt"), "w") as f:
            f.write("deep content")

        hash3 = compute_directory_hash(test_dir)
        assert hash1 != hash3  # Structure changed
        # Verify deterministic for complex structure
        hash3_verify = compute_directory_hash(test_dir)
        assert hash3 == hash3_verify

        # Test 4: Multiple files in same directory
        for i in range(5):
            with open(os.path.join(test_dir, f"data{i}.txt"), "w") as f:
                f.write(f"data content {i}")

        hash4 = compute_directory_hash(test_dir)
        assert hash3 != hash4
        # Verify deterministic
        hash4_verify = compute_directory_hash(test_dir)
        assert hash4 == hash4_verify

        # Test 5: Empty directories affect hash
        empty_dir = os.path.join(test_dir, "empty")
        os.makedirs(empty_dir)
        hash5 = compute_directory_hash(test_dir)
        assert hash4 != hash5  # Empty dir changes hash

        # Test 6: File content changes affect hash
        with open(os.path.join(test_dir, "file1.txt"), "w") as f:
            f.write("modified content")
        hash6 = compute_directory_hash(test_dir)
        assert hash5 != hash6

        # Test 7: Binary files
        with open(os.path.join(test_dir, "binary.bin"), "wb") as f:
            f.write(b"\x00\x01\x02\x03\xff\xfe\xfd")
        hash7 = compute_directory_hash(test_dir)
        assert hash6 != hash7
        # Verify deterministic for binary files
        hash7_verify = compute_directory_hash(test_dir)
        assert hash7 == hash7_verify

        # Test 8: Same content in different location produces same hash
        test_dir2 = os.path.join(tmpdir, "test2")
        os.makedirs(test_dir2)
        with open(os.path.join(test_dir2, "file1.txt"), "w") as f:
            f.write("content1")

        hash8 = compute_directory_hash(test_dir2)
        # This should match hash1 (same content, different location)
        assert hash8 == hash1

        # Test 9: Verify all hashes are unique (no random collisions)
        all_hashes = [hash1, hash3, hash4, hash5, hash6, hash7]
        assert len(set(all_hashes)) == len(all_hashes)  # All unique


def test_path_mapping_validation_errors():
    """Test PathMappingImpl validation error cases"""
    from repo2docker.contentproviders.rdm.paths import PathMappingImpl
    import pytest

    # Missing type key
    with pytest.raises(ValueError, match="No type key in mapping"):
        PathMappingImpl({
            "source": "osfstorage",
            "target": "."
        })

    # Invalid type value
    with pytest.raises(ValueError, match="Invalid type in mapping"):
        PathMappingImpl({
            "type": "invalid",
            "source": "osfstorage",
            "target": "."
        })

    # Missing source key
    with pytest.raises(ValueError, match="No source key in mapping"):
        PathMappingImpl({
            "type": "copy",
            "target": "."
        })

    # Missing target key
    with pytest.raises(ValueError, match="No target key in mapping"):
        PathMappingImpl({
            "type": "copy",
            "source": "osfstorage"
        })

    # Invalid target path (not relative)
    with pytest.raises(ValueError, match="Target path must be relative"):
        PathMappingImpl({
            "type": "copy",
            "source": "osfstorage",
            "target": "/absolute/path"
        })


def test_paths_mapping_missing_paths_key():
    """Test PathsMapping error when paths key is missing"""
    from repo2docker.contentproviders.rdm.paths import PathsMapping
    import pytest

    with pytest.raises(ValueError, match="No paths key in mapping"):
        PathsMapping({
            "override": True
        })


def test_path_mapping_interface():
    """Test PathMapping base interface"""
    from repo2docker.contentproviders.rdm.paths import PathMapping
    import pytest

    # Create instance of base class
    mapping = PathMapping()

    # Test that interface methods raise NotImplementedError
    with pytest.raises(NotImplementedError):
        mapping.get_source("default")

    with pytest.raises(NotImplementedError):
        mapping.get_target()
