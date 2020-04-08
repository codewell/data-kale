from pathlib import Path
import os


class LocalObject:
    def __init__(self, key, repository_name, root, resource=None):
        self._key = key
        self._repository_name = repository_name
        self._root = root

        self._resource = resource
        if resource is None:
            self._remote_object = None
        else:
            self._remote_object = resource.Bucket(self._repository_name).Object(self._key)

        self._local_path = root / repository_name / key

    def with_resource(self, resource):
        return LocalObject(
            key=self._key,
            repository_name=self._repository_name,
            root=self._root,
            resource=resource,
        )

    def key(self):
        return self._key

    def size(self):
        if not self.is_dir():
            return os.path.getsize(self.local_path())
        else:
            return 0

    def is_dir(self):
        return self.local_path().is_dir()

    def remove(self):
        if not self.is_dir():
            self.local_path().unlink()
        else:
            self.local_path().rmdir()

    def local_path(self):
        return self._local_path

    def upload(self, update=None, n_retries=0):
        from data_kale.object_.remote_object import RemoteObject

        remote_object = RemoteObject(
            key=self._key,
            repository_name=self._repository_name,
            root=self._root,
        ).with_resource(self._resource)

        extra_args = dict(
        )
        if not remote_object.is_dir():
            import botocore
            try:
                remote_object.remote_object().upload_file(
                    str(self.local_path()),
                    Callback=update,
                    ExtraArgs=extra_args,
                )
            except botocore.parsers.ResponseParserError as e:
                # HACK Wasabi has some issues on upload/download every 10k requests or so, remidy this with a retry
                import time
                if n_retries < 16:
                    time.sleep(1)
                    self.upload(update=update, n_retries=n_retries + 1)
                else:
                    raise

        else:
            remote_object.remote_object().put(
                '',
                **extra_args,
            )

        return remote_object

    def __str__(self):
        return f'{type(self).__name__}(key=\'{self._key}\', resource={self._resource})'

    def __repr__(self):
        return str(self)

def list_local_objects(repository_name, root, resource):
    repository_path = root / repository_name

    def key(path):
        relative_path = str(Path(*path.parts[len(repository_path.parts):]))
        if not path.is_dir():
            return relative_path
        else:
            return f'{relative_path}/'

    keys = list(map(
        key,
        repository_path.rglob('*')
    ))

    return dict(map(
        lambda key: (key, LocalObject(
            key=key,
            repository_name=repository_name,
            root=root,
        )),
        keys,
    ))
