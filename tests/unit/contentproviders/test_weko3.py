import os
import json
import re
from tempfile import TemporaryDirectory, NamedTemporaryFile

from unittest.mock import patch, MagicMock

from repo2docker.contentproviders import WEKO3


def test_detect_weko3_url():
    weko3 = WEKO3()
    spec = weko3.detect("https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt")

    assert spec is not None, spec
    assert spec["bucket"] == "abcdefgh-12345678"
    assert spec["file_names"] == ["test1.txt"]
    assert re.match(r'^[0-9A-Fa-f\-]+$', spec["uuid"]) is not None
    assert spec["host"]["file_base_url"] == "https://test.some.host.nii.ac.jp/api/files/"

    weko3 = WEKO3()
    spec = weko3.detect("https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt,test2.txt", "X1234")

    assert spec is not None, spec
    assert spec["bucket"] == "abcdefgh-12345678"
    assert spec["file_names"] == ["test1.txt", "test2.txt"]
    assert spec["uuid"] == "X1234"
    assert spec["host"]["file_base_url"] == "https://test.some.host.nii.ac.jp/api/files/"


def test_not_detect_rdm_url():
    weko3 = WEKO3()
    spec = weko3.detect("https://unknown.some.host.nii.ac.jp/x1234")

    assert spec is None, spec


def test_detect_external_rdm_url():
    with NamedTemporaryFile('w+') as f:
        try:
            f.write(json.dumps([
                {
                    "hostname": [
                        "https://test1.some.host.nii.ac.jp/",
                    ],
                    "file_base_url": "https://test1.some.host.nii.ac.jp/api/files/",
                }
            ]))
            f.flush()
            os.environ["WEKO3_HOSTS"] = f.name

            weko3 = WEKO3()
            spec = weko3.detect("https://test1.some.host.nii.ac.jp/x1234/t.txt")

            assert spec is not None, spec
            assert spec["bucket"] == "x1234"
            assert spec["file_names"] == ["t.txt"]
            assert spec["host"]["file_base_url"] == "https://test1.some.host.nii.ac.jp/api/files/"

            weko3 = WEKO3()
            spec = weko3.detect("https://test1.some.host.nii.ac.jp/x1234/t1.txt,t2.txt", "")

            assert spec is not None, spec
            assert spec["bucket"] == "x1234"
            assert spec["file_names"] == ["t1.txt", "t2.txt"]
            assert spec["host"]["file_base_url"] == "https://test1.some.host.nii.ac.jp/api/files/"
        finally:
            del os.environ["WEKO3_HOSTS"]


def test_content_id_is_unique():
    weko3_1 = WEKO3()
    weko3_1.detect("https://test.some.host.nii.ac.jp/x1234/t.txt")
    weko3_2 = WEKO3()
    weko3_2.detect("https://test.some.host.nii.ac.jp/y5678/t.txt")
    assert weko3_1.content_id != weko3_2.content_id

    weko3_1 = WEKO3()
    weko3_1.detect("https://test.some.host.nii.ac.jp/x1234/t.txt")
    weko3_2 = WEKO3()
    weko3_2.detect("https://test.some.host.nii.ac.jp/x1234/t.txt")
    assert weko3_1.content_id != weko3_2.content_id


def test_fetch_content():
    with TemporaryDirectory() as d:
        weko3 = WEKO3()
        spec = {"bucket": "x1234", "file_names": ["t1.txt", "t2.txt"], "host": {"file_base_url": "https://test.some.host/api/files/", "token": "TEST"}}
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b'1234567890'
            fake_urlopen.return_value = MagicMock(read=fake_read)
            for msg in weko3.fetch(spec, d):
                if msg.startswith('Fetching'):
                    assert 'x1234 at https://test.some.host/api/files' in msg
                elif msg.startswith('Fetch:') and '/t1.txt' in msg:
                    assert 'to {}/t1.txt'.format(d) in msg
                elif msg.startswith('Fetch:') and '/t2.txt' in msg:
                    assert 'to {}/t2.txt'.format(d) in msg
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 2
            assert fake_urlopen.call_args_list[0][0][0].full_url == "https://test.some.host/api/files/x1234/t1.txt"
            assert fake_urlopen.call_args_list[0][0][0].get_header('Authorization') == "Bearer TEST"
            assert fake_urlopen.call_args_list[1][0][0].full_url == "https://test.some.host/api/files/x1234/t2.txt"
            assert fake_urlopen.call_args_list[1][0][0].get_header('Authorization') == "Bearer TEST"
