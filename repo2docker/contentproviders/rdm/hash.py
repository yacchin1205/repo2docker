import logging
import hashlib
import os


logger = logging.getLogger(__name__)


def compute_directory_hash(path):
    sha256 = hashlib.sha256()
    all_paths = []
    for root, dirs, files in os.walk(path):
        for name in sorted(dirs + files):
            full_path = os.path.join(root, name)
            relative_path = os.path.relpath(full_path, path)
            logger.debug(f'Adding path to hash: {relative_path}')
            all_paths.append(relative_path)
    for relative_path in sorted(all_paths):
        sha256.update(f'PATH:{relative_path}\n'.encode('utf-8'))
    for relative_path in sorted(all_paths):
        full_path = os.path.join(path, relative_path)
        if os.path.isfile(full_path):
            sha256.update(f'CONTENT:{relative_path}\n'.encode('utf-8'))
            with open(full_path, 'rb') as f:
                while chunk := f.read(8192):
                    sha256.update(chunk)
    r = sha256.hexdigest()
    logger.debug(f'Final hash: {r}')
    return r
