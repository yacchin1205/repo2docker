import os
import shutil
import tempfile

def write_json(crate, path):
    work_dir = tempfile.mkdtemp()
    try:
        crate.write(work_dir)
        crate_path = os.path.join(work_dir, "ro-crate-metadata.json")
        assert os.path.exists(os.path.join(crate_path))
        shutil.copy(crate_path, path)
    finally:
        shutil.rmtree(work_dir)
