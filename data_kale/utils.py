import operator
from operator import (
    itemgetter,
    methodcaller,
)

import tqdm


def itemgetter(*args):
    if len(args) > 1:
        return operator.itemgetter(*args)
    if len(args) == 1:
        return lambda d: tuple([d[args[0]], ])
    else:
        return lambda d: tuple()

def print_list(xs):
    list(map(print, xs))

def progress(objects, repository_name, operation_name):
    return tqdm.tqdm(
        total=sum(map(methodcaller('size'), objects)),
        unit='B',
        unit_scale=True,
        desc=f'{operation_name} \'{repository_name}\'',
    )
