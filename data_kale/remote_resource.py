import os

import boto3

from data_kale.configuration import configuration


def create_resource():
    credentials = configuration()['credentials']

    region = 'eu-central-1'
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    session = boto3.session.Session(
        region_name=region,
        aws_access_key_id=credentials['s3-access-key'],
        aws_secret_access_key=credentials['s3-secret-key'],
    )

    resource = session.resource(
        's3',
        endpoint_url=f'https://s3.{region}.wasabisys.com',
    )

    return resource
