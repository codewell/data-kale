from operator import attrgetter

from data_kale.remote_resource import (
    create_resource,
)

def list_remote():
    resource = create_resource()

    return list(map(
        attrgetter('name'),
        resource.buckets.all()
    ))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='List puddles')

    args = parser.parse_args()

    list(map(
        print,
        list_remote(**vars(args))
    ))
