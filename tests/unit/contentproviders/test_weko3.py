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
    assert spec["url"] == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
    assert re.match(r"^[0-9A-Fa-f\-]+$", spec["uuid"]) is not None
    assert spec["host"]["hostname"] == ["https://test.some.host.nii.ac.jp/"]


def test_not_detect_weko3_url():
    weko3 = WEKO3()
    spec = weko3.detect("https://unknown.some.host.nii.ac.jp/x1234")

    assert spec is None, spec


def test_detect_external_weko3_url():
    with NamedTemporaryFile("w+") as f:
        try:
            f.write(
                json.dumps(
                    [
                        {
                            "hostname": [
                                "https://test1.some.host.nii.ac.jp/",
                            ],
                            "file_base_url": "https://test1.some.host.nii.ac.jp/api/files/",
                        }
                    ]
                )
            )
            f.flush()
            os.environ["WEKO3_HOSTS"] = f.name

            weko3 = WEKO3()
            spec = weko3.detect("https://test1.some.host.nii.ac.jp/x1234/t.txt")

            assert spec is not None, spec
            assert spec["url"] == "https://test1.some.host.nii.ac.jp/x1234/t.txt"
            assert spec["host"]["hostname"] == ["https://test1.some.host.nii.ac.jp/"]
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
        spec = {
            "url": "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt",
            "host": {
                "file_base_url": "https://test.some.host/api/files/",
                "token": "TEST",
            },
        }
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b"1234567890"
            fake_urlopen.return_value = MagicMock(read=fake_read, status=200)
            for msg in weko3.fetch(spec, d):
                if msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
                        in msg
                    )
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 1
            assert (
                fake_urlopen.call_args_list[0][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
            )
            assert list(os.listdir(d)) == ["test1.txt"]


def test_fetch_named_content():
    with TemporaryDirectory() as d:
        weko3 = WEKO3()
        spec = {
            "url": "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt",
            "host": {
                "file_base_url": "https://test.some.host/api/files/",
                "token": "TEST",
            },
        }
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b"1234567890"

            def fake_getheader(name):
                if name == "Content-Disposition":
                    return 'attachment; filename="example.txt"'
                return None

            fake_urlopen.return_value = MagicMock(
                read=fake_read, status=200, getheader=fake_getheader
            )
            for msg in weko3.fetch(spec, d):
                if msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
                        in msg
                    )
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 1
            assert (
                fake_urlopen.call_args_list[0][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
            )
            assert list(os.listdir(d)) == ["example.txt"]


def test_fetch_invalid_named_content():
    with TemporaryDirectory() as d:
        weko3 = WEKO3()
        spec = {
            "url": "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt",
            "host": {
                "file_base_url": "https://test.some.host/api/files/",
                "token": "TEST",
            },
        }
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b"1234567890"

            def fake_getheader(name):
                if name == "Content-Disposition":
                    return 'attachment; filename="../../example.txt"'
                return None

            fake_urlopen.return_value = MagicMock(
                read=fake_read, status=200, getheader=fake_getheader
            )
            for msg in weko3.fetch(spec, d):
                if msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
                        in msg
                    )
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 1
            assert (
                fake_urlopen.call_args_list[0][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
            )
            assert list(os.listdir(d)) == ["..-..-example.txt"]


def test_fetch_unnamed_content():
    with TemporaryDirectory() as d:
        weko3 = WEKO3()
        spec = {
            "url": "https://test.some.host.nii.ac.jp/abcdefgh-12345678/",
            "host": {
                "file_base_url": "https://test.some.host/api/files/",
                "token": "TEST",
            },
        }
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b"1234567890"
            fake_urlopen.return_value = MagicMock(read=fake_read, status=200)
            for msg in weko3.fetch(spec, d):
                if msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/" in msg
                    )
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 1
            assert (
                fake_urlopen.call_args_list[0][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/"
            )
            assert list(os.listdir(d)) == ["unnamed_1"]


def test_forbidden_fetch_content():
    with TemporaryDirectory() as d:
        weko3 = WEKO3()
        spec = {
            "url": "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt",
            "host": {
                "file_base_url": "https://test.some.host/api/files/",
                "token": "TEST",
            },
        }
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b"1234567890"
            fake_urlopen.return_value = MagicMock(read=fake_read, status=403)
            for msg in weko3.fetch(spec, d):
                if msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
                        in msg
                    )
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 1
            assert (
                fake_urlopen.call_args_list[0][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.txt"
            )


def test_fetch_ld_json_content():
    with TemporaryDirectory() as d:
        weko3 = WEKO3()
        spec = {
            "url": "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.html",
            "host": {
                "file_base_url": "https://test.some.host/api/files/",
                "token": "TEST",
            },
        }
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b"""
<html>
<head>
  <title>Sample Database</title>
  <script type="application/ld+json">
  {
    "@context":"https://schema.org/",
    "@type":"Dataset",
    "distribution":[
       {
          "@type":"DataDownload",
          "encodingFormat":"CSV",
          "contentUrl":"https://test.some.host.nii.ac.jp/abcdefgh-12345678/test2.csv"
       },
       {
          "@type":"DataDownload",
          "encodingFormat":"XML",
          "contentUrl":"https://test.some.host.nii.ac.jp/abcdefgh-12345678/test3.xml"
       }
    ]
  }
  </script>
</head>
<body>
</body>
</html>
"""

            def fake_getheader(name):
                if name == "Content-Type":
                    return "text/html"
                return None

            fake_urlopen.return_value = MagicMock(
                read=fake_read, status=200, getheader=fake_getheader
            )
            for i, msg in enumerate(weko3.fetch(spec, d)):
                if i == 0 and msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.html"
                        in msg
                    )
                elif i == 1 and msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test2.csv"
                        in msg
                    )
                elif i == 2 and msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test3.xml"
                        in msg
                    )
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 3
            assert (
                fake_urlopen.call_args_list[0][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.html"
            )
            assert (
                fake_urlopen.call_args_list[1][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test2.csv"
            )
            assert (
                fake_urlopen.call_args_list[2][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test3.xml"
            )
            assert sorted(list(os.listdir(d))) == [
                "test1.html",
                "test2.csv",
                "test3.xml",
            ]


def test_fetch_unnamed_ld_json_content():
    with TemporaryDirectory() as d:
        weko3 = WEKO3()
        spec = {
            "url": "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.html",
            "host": {
                "file_base_url": "https://test.some.host/api/files/",
                "token": "TEST",
            },
        }
        with patch.object(WEKO3, "urlopen") as fake_urlopen:
            fake_read = MagicMock()
            fake_read.return_value = b"""
<html>
<head>
  <title>Sample Database</title>
  <script type="application/ld+json">
  {
    "@context":"https://schema.org/",
    "@type":"Dataset",
    "distribution":[
       {
          "@type":"DataDownload",
          "encodingFormat":"CSV",
          "contentUrl":"https://test.some.host.nii.ac.jp/abcdefgh-12345678/"
       },
       {
          "@type":"DataDownload",
          "encodingFormat":"XML",
          "contentUrl":"https://test.some.host.nii.ac.jp/abcdefgh-12345678/"
       }
    ]
  }
  </script>
</head>
<body>
</body>
</html>
"""

            def fake_getheader(name):
                if name == "Content-Type":
                    return "text/html"
                return None

            fake_urlopen.return_value = MagicMock(
                read=fake_read, status=200, getheader=fake_getheader
            )
            for i, msg in enumerate(weko3.fetch(spec, d)):
                if i == 0 and msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.html"
                        in msg
                    )
                elif i == 1 and msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/" in msg
                    )
                elif i == 2 and msg.startswith("Fetching"):
                    assert (
                        "at https://test.some.host.nii.ac.jp/abcdefgh-12345678/" in msg
                    )
                else:
                    assert False, msg
            assert fake_urlopen.call_count == 3
            assert (
                fake_urlopen.call_args_list[0][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/test1.html"
            )
            assert (
                fake_urlopen.call_args_list[1][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/"
            )
            assert (
                fake_urlopen.call_args_list[2][0][0].full_url
                == "https://test.some.host.nii.ac.jp/abcdefgh-12345678/"
            )
            assert sorted(list(os.listdir(d))) == [
                "test1.html",
                "unnamed_1",
                "unnamed_2",
            ]
