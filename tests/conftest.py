import os
from io import BytesIO
import shutil
import tempfile
from pathlib import Path

import pytest

from data_kale.remote_resource import (
    create_resource,
)


@pytest.fixture
def repository_name():
    return 'data-kale-test-fixture'

@pytest.fixture
def repository(repository_name):
    resource = create_resource()
    repository = resource.Bucket(repository_name)
    repository.objects.all().delete()

    yield repository

    repository.objects.all().delete()

@pytest.fixture
def root():
    root = Path(tempfile.mkdtemp())

    yield root

    shutil.rmtree(root)

@pytest.fixture
def repository_path(root, repository_name):
    path = root / repository_name
    path.mkdir()

    yield path

    shutil.rmtree(path)

@pytest.fixture
def get_path(repository_path):
    class LocalObject:
        def __init__(self, file_name):
            self._path = repository_path / file_name

        def exists(self):
            return self._path.exists()
        def mkdir(self):
            return self._path.mkdir()
        def isdir(self):
            return self._path.is_dir()
        def read(self):
            with open(self._path, 'rb') as f:
                return f.read()
        def write(self, value):
            with open(self._path, 'wb') as f:
                f.write(value)

    def _get_path(file_name):
        return LocalObject(file_name)
    
    return _get_path

@pytest.fixture
def get_object(repository):
    class RemoteObject:
        def __init__(self, object_name):
            self._object_name = object_name
            self._object = repository.Object(object_name)

        def exists(self):
            import botocore

            try:
                self._object.load()
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return False
                else:
                    raise
            else:
                return True
                
        def mkdir(self):
            directory_key = self._object.key.endswith('/')
            if directory_key:
                self.write(b'')
            else:
                raise Exception(f'Not able to create a directory {self._object_name}')

        def isdir(self):
            exists = self.exists()
            directory_key = self._object.key.endswith('/')
            no_content = self.read() == b''

            return exists and directory_key and no_content

        def read(self):
            if not self.exists():
                raise Exception(f'{self._object_name} does not exist')
            with BytesIO() as f:
                print(self._object.download_fileobj(f, Callback=print))

                return f.getvalue()

        def write(self, value):
            with BytesIO(value) as f:
                self._object.upload_fileobj(f, Callback=print)

    def _get_object(object_name):
        return RemoteObject(object_name)

    return _get_object