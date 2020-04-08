from operator import (
    attrgetter,
)

import os
from pathlib import Path
import hashlib


def identical(compared_objects, update=None):
    local_object, remote_object = compared_objects

    def etag_match(local_object, remote_object):
        """
        See https://zihao.me/post/calculating-etag-for-aws-s3-objects/
        """
        def md5_checksum(path):
            m = hashlib.md5()
            with open(path, 'rb') as f:
                for data in iter(lambda: f.read(1024 ** 2), b''):
                    m.update(data)
                    if update is not None:
                        update(len(data))
            return m.hexdigest()

        def etag_checksum(path, chunk_size=8 * 1024 ** 2):
            md5s = []
            with open(path, 'rb') as f:
                for data in iter(lambda: f.read(chunk_size), b''):
                    md5s.append(
                        hashlib.md5(data).digest()
                    )
                    if update is not None:
                        update(len(data))
            m = hashlib.md5(
                b''.join(md5s)
            )
            return f'{m.hexdigest()}-{len(md5s)}'

        etag = remote_object.remote_object().e_tag[1:-1]
        path = local_object.local_path()

        if '-' in etag:
            calculated_etag = etag_checksum(path)

            return etag == calculated_etag

        if '-' not in etag:
            calculated_etag = md5_checksum(path)

            return etag == calculated_etag

        return False

    if local_object.is_dir() or remote_object.is_dir():
        return local_object.is_dir() == remote_object.is_dir()
    else:
        return etag_match(local_object, remote_object)

