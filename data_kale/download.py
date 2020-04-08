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

def changed(compared_objects):
    local_object, remote_object = compared_objects

    return not identical((
        local_object.with_resource(changed._resource),
        remote_object.with_resource(changed._resource),
    ))

def downloader(remote_object):
    remote_object.with_resource(downloader._resource).download()

def download(repository_name, root=data_root()):
    if type(root) == str:
        root = Path(root)

    pool = mp.Pool(
        processes=16,
        initializer=init_worker, initargs=(loader, changed, downloader),
    )

    resource = create_resource()

    repository_path = root / repository_name
    repository_path.mkdir(parents=True, exist_ok=True)

    local_objects = list_local_objects(repository_name, root=root, resource=resource)
    remote_objects = list_remote_objects(repository_name, root=root, resource=resource)

    removed_object_keys = local_objects.keys() - remote_objects.keys()
    created_object_keys = remote_objects.keys() - local_objects.keys()
    potentially_updated_object_keys = local_objects.keys() & remote_objects.keys()

    removed_objects = itemgetter(*removed_object_keys)(local_objects)
    created_objects = itemgetter(*created_object_keys)(remote_objects)
    potentially_updated_objects = tuple(map(
        lambda key: (local_objects[key], remote_objects[key]),
        potentially_updated_object_keys,
    ))

    sorted_removed_objects = sorted(
        removed_objects,
        key=methodcaller('key'),
        reverse=True,
    )

    list(map(
        methodcaller('remove'),
        sorted_removed_objects,
    ))

    # FIXME remove duplication
    potentially_updated_changed = list(tqdm(
        pool.imap(changed, potentially_updated_objects, 2),
        desc=f'checking changes',
        total=len(potentially_updated_objects),
    ))

    updated_objects = tuple(map(
        lambda x: x[1][1].with_resource(None), # NOTE [1][1] and not [1][0]
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
        pool.imap(downloader, sorted_created_updated_objects),
        desc=f'download',
        total=len(sorted_created_updated_objects),
    ))

    pool.close()

    return repository_path

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download puddle')
    parser.add_argument('repository_name')

    args = parser.parse_args()

    download(**vars(args))
