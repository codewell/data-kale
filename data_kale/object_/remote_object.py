from operator import (
    attrgetter,
)


class RemoteObject:
    def __init__(self, key, repository_name, root, resource=None):
        self._key = key
        self._repository_name = repository_name
        self._root = root

        self._resource = resource
        if resource is None:
            self._remote_object = None
        else:
            self._remote_object = resource.Bucket(self._repository_name).Object(self._key)

    def with_resource(self, resource):
        return RemoteObject(
            key=self._key,
            repository_name=self._repository_name,
            root=self._root,
            resource=resource,
        )

    def key(self):
        return self._key

    def size(self):
        return self.remote_object().content_length

    def is_dir(self):
        return self._key.endswith('/')

    def remove(self):
        self._remote_object.delete()

    def remote_object(self):
        return self._remote_object

    def load(self):
        return self.remote_object().load()

    def download(self, update=None, n_retries=0):
        from data_kale.object_.local_object import LocalObject

        local_object = LocalObject(
            key=self._key,
            repository_name=self._repository_name,
            root=self._root,
            resource=self._resource,
        )

        if not self.is_dir():
            import botocore
            local_object.local_path().parent.mkdir(parents=True, exist_ok=True)

            try:
                self.remote_object().download_file(
                    str(local_object.local_path()),
                    Callback=update,
                )
            except botocore.parsers.ResponseParserError as e:
                # HACK Wasabi has some issues on upload/download every 10k requests or so, remidy this with a retry
                import time
                if n_retries < 16:
                    time.sleep(1)
                    self.download(update=update, n_retries=n_retries + 1)
                else:
                    raise

 
        else:
            local_object.local_path().mkdir(parents=True, exist_ok=True)

        return local_object

    def __str__(self):
        return f'{type(self).__name__}(key=\'{self._key}\', resource={self._resource})'

    def __repr__(self):
        return str(self)

def list_remote_objects(repository_name, root, resource):
    repository = resource.Bucket(repository_name)

    keys = list(map(
        attrgetter('key'),
        repository.objects.all()
    ))

    return dict(map(
        lambda key: (key, RemoteObject(
            key=key,
            repository_name=repository_name,
            root=root,
        )),
        keys,
    ))
