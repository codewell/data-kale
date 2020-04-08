import os
from glob import glob
from pathlib import Path
from operator import (
    methodcaller,
)
import multiprocessing as mp

from tqdm import tqdm

from data_kale.configuration import configuration
from data_kale.path import data_root
from data_kale.utils import (
    itemgetter,
    print_list,
    progress,
)
from data_kale.remote_resource import (
    create_resource,
)

from data_kale.object_.identical import identical
from data_kale.object_.local_object import list_local_objects
from data_kale.object_.remote_object import list_remote_objects


def init_worker(*fs):
    for f in fs:
        f._resource = create_resource()

def loader(remote_object):
    remote_object.with_resource(loader._resource).load()

def remover(remote_object):
    remote_object.with_resource(remover._resource).remove()


# FIXME naming
def changed(compared_objects):
    local_object, remote_object = compared_objects

    return not identical((
        local_object.with_resource(changed._resource),
        remote_object.with_resource(changed._resource),
    ))

def uploader(local_object):
    local_object.with_resource(uploader._resource).upload()

def upload(repository_name, root=data_root()):
    if type(root) == str:
        root = Path(root)

    pool = mp.Pool(
        processes=16,
        initializer=init_worker, initargs=(loader, changed, remover, uploader),
    )

    resource = create_resource()
    repository_bucket = resource.Bucket(repository_name)

    import botocore
    try:
        repository_bucket.create()
    except:
        pass

    local_objects = list_local_objects(repository_name, root=root, resource=resource)
    remote_objects = list_remote_objects(repository_name, root=root, resource=resource)

    removed_object_keys = remote_objects.keys() - local_objects.keys()
    created_object_keys = local_objects.keys() - remote_objects.keys()
    potentially_updated_object_keys = local_objects.keys() & remote_objects.keys()

    removed_objects = itemgetter(*removed_object_keys)(remote_objects)
    created_objects = itemgetter(*created_object_keys)(local_objects)
    potentially_updated_objects = tuple(map(
        lambda key: (local_objects[key], remote_objects[key]),
        potentially_updated_object_keys,
    ))

    sorted_removed_objects = sorted(
        removed_objects,
        key=methodcaller('key'),
        reverse=True,
    )

    list(tqdm(
        pool.imap(remover, sorted_removed_objects, 2),
        desc=f'remove',
        total=len(sorted_removed_objects),
    ))

    potentially_updated_changed = list(tqdm(
        pool.imap(changed, potentially_updated_objects, 2),
        desc=f'checking changes',
        total=len(potentially_updated_objects),
    ))

    updated_objects = tuple(map(
        lambda x: x[1][0].with_resource(None),
        filter(
            lambda x: x[0],
            zip(potentially_updated_changed, potentially_updated_objects)
        )
    ))

    sorted_created_updated_objects = sorted(
        created_objects + updated_objects,
        key=methodcaller('key'),
    )
    list(tqdm(
        pool.imap(uploader, sorted_created_updated_objects, 2),
        desc=f'upload',
        total=len(sorted_created_updated_objects),
    ))

    pool.close()

    return repository_bucket


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Upload puddle')
    parser.add_argument('repository_name')

    args = parser.parse_args()

    upload(**vars(args))
