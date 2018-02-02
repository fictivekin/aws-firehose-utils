# -*- coding: utf-8 -*-

import base64
import boto3
import json
import os
import re
import sys

from elasticsearch import Elasticsearch, RequestsHttpConnection, TransportError
from manager import Manager
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

manager = Manager()

boto_session = boto3.Session()

es = {}

def get_es_conn(es_domain_arn):
    if es_domain_arn not in es:
        es_domain = re.sub(
            r'^arn:aws:es:[a-z]*-[a-z]*-[0-9]*:[0-9]*:domain/(.*)$',
            r'\1',
            es_domain_arn)
        aws_es = boto_session.client('es')
        es_config = aws_es.describe_elasticsearch_domain(DomainName=es_domain)

        awsauth = BotoAWSRequestsAuth(
            aws_host=es_config['DomainStatus']['Endpoint'],
            aws_region=boto_session.region_name,
            aws_service='es'
        )

        es.update({es_domain_arn:
            Elasticsearch(
                hosts=[{'host': es_config['DomainStatus']['Endpoint'], 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                retry_on_timeout=True
            )
        })

    return es[es_domain_arn]


def get_firehose_config(stream=None):
    if stream is None and not os.environ.get('STREAM_NAME', False):
        print('No stream name passed and STREAM_NAME not set in environment')
        sys.exit(1)

    firehose = boto_session.client('firehose')
    # Intentionally not catching the ResourceNotFoundException
    try:
        exists_resp = firehose.describe_delivery_stream(DeliveryStreamName=stream)
        if 'DeliveryStreamDescription' in exists_resp:
            return exists_resp['DeliveryStreamDescription']
    except firehose.exceptions.ResourceNotFoundException as e:
        print(e.message)
    except Exception:
        print(e.message)

    print('Something went wrong in retrieving the Firehose Stream configuration')
    sys.exit(1)


def get_s3info_from_config(config):
    info = {}

    if 'S3BackupMode' not in config:
        return info

    # S3 and Redshift destinations use this key
    if config['S3BackupMode'] == 'enabled':
        info.update({
            'ARN': config['S3BackupDescription']['BucketARN'],
            'prefix': config['S3BackupDescription']['Prefix'],
            'bucket': config['S3BackupDescription']['BucketARN'].replace('arn:aws:s3:::','')
        })

    # ES destinations use this key
    elif config['S3BackupMode'] == 'FailedDocumentsOnly':
        info.update({
            'ARN': config['S3DestinationDescription']['BucketARN'],
            'prefix': config['S3DestinationDescription']['Prefix'],
            'bucket': config['S3DestinationDescription']['BucketARN'].replace('arn:aws:s3:::','')
        })

    return info


@manager.command
def review(stream=None, pattern=None):
    """
    Review the list of failures from a Kinesis Firehose Stream
    """

    stream_config = get_firehose_config(stream)

    buckets = []

    for destination in stream_config['Destinations']:
        if 'ExtendedS3DestinationDescription' in destination:
            info = get_s3info_from_config(destination['ExtendedS3DestinationDescription'])
            if info:
                buckets.append(info)

        if 'RedshiftDestinationDescription' in destination:
            info = get_s3info_from_config(destination['RedshiftDestinationDescription'])
            if info:
                buckets.append(info)

        if 'ElasticsearchDestinationDescription' in destination:
            info = get_s3info_from_config(destination['ElasticsearchDestinationDescription'])
            if info:
                buckets.append(info)

    if len(buckets) < 1:
        print('Nothing to review, no stream destinations have S3 backups enabled.')
        sys.exit(1)

    elif len(buckets) > 1:
        print('WARNING: Review is possible, but cannot resubmit to firehose for delivery since there are multiple destinations defined on the stream.')
        print('Some destinations may have already accepted the records.')

    for destination in buckets:
        print('Keys in {}'.format(destination['bucket']))
        bucket = boto_session.resource('s3').Bucket(destination['bucket'])
        bucketlist = bucket.objects.filter(Prefix=destination['prefix'])
        for subobj in sorted(bucketlist, key=lambda k: k.last_modified):
            if pattern:
                if subobj.key.contains(pattern):
                    print('  {}'.format(subobj.key))
            else:
                print('  {}'.format(subobj.key))


@manager.command
def show(bucket, key):
    """
    Show the contents of a single failure report. Needs complete S3 key as provided by the review command
    """
    print(json.dumps(get_failure_report(bucket, key), indent=4))


def get_failure_report(bucket, key):
    """
    Retrieves and decodes a failure report for display or resubmission
    """
    obj = boto_session.resource('s3').Object(bucket, key).get()
    body = obj['Body'].read()

    lines = []
    for line in body.split('\n'):
        if not line:
            continue
        try:
            newline = json.loads(line.replace('\r',''))
            newline['rawData'] = json.loads(base64.b64decode(newline['rawData']))
            lines.append(newline)

        except ValueError as e:
            print(line)
            raise e
        except TypeError as e1:
            print(line)
            raise e1

    return lines


@manager.command
def resubmit_to_es(stream, year, month=None, day=None):
    """
    Resubmit a day of failed records to ElasticSearch
    """
    stream_config = get_firehose_config(stream)
    s3_info = None
    for destination in stream_config['Destinations']:
        if 'ElasticsearchDestinationDescription' in destination:
            s3_info = get_s3info_from_config(destination['ElasticsearchDestinationDescription'])

    if not s3_info:
        print('Cannot resubmit to ES, no ES destination defined in stream config.')
        sys.exit(1)

    if s3_info['prefix'].endswith('/'):
        s3_info['prefix'] = s3_info['prefix'][:-1]

    prefix = '{}/elasticsearch-failed/{}'.format(s3_info['prefix'], year)
    if month:
        prefix = '{}/{}'.format(prefix, month)

        if day:
            prefix = '{}/{}'.format(prefix, day)

    print('Prefix: {}'.format(prefix))

    bucket = boto_session.resource('s3').Bucket(s3_info['bucket'])
    bucketlist = bucket.objects.filter(Prefix=prefix)

    for subobj in sorted(bucketlist, key=lambda k: k.last_modified):
        print('Attempting to resubmit: {}'.format(subobj.key))
        _resubmit_to_es(s3_info['bucket'], subobj.key, stream_config)


def _resubmit_to_es(bucket, key, stream_config):
    """
    Resubmit a failed record to ElasticSearch
    """


    es_destination_arn = None
    for destination in stream_config['Destinations']:
        if 'ElasticsearchDestinationDescription' in destination:
            es_destination_arn = destination['ElasticsearchDestinationDescription']['DomainARN']
            break

    if not es_destination_arn:
        print('There is no elasticsearch destination configured for stream {}'.format(stream))
        sys.exit(1)

    report = get_failure_report(bucket, key)

    es_conn = get_es_conn(es_destination_arn)
    all_resubmitted = True
    for failures in report:
        try:
            if 'esIndexName' not in failures:
                continue

            exception_message = failures['rawData'].get('response', {}).get('exception', {}).get('message', {})
            if exception_message and isinstance(exception_message, dict):

                for keyname in exception_message.keys():
                    if isinstance(exception_message[keyname], list) and len(exception_message[keyname]) == 1:
                        failures['rawData']['response']['exception']['message'] = exception_message[keyname][0]

                    elif isinstance(exception_message[keyname], dict):
                        for subkeyname in exception_message[keyname].keys():
                            if isinstance(exception_message[keyname][subkeyname], list) and len(exception_message[keyname][subkeyname]) == 1:
                                failures['rawData']['response']['exception']['message'] = exception_message[keyname][subkeyname][0]
                            elif isinstance(exception_message[keyname][subkeyname], str):
                                failures['rawData']['response']['exception']['message'] = exception_message[keyname][subkeyname]
                    elif isinstance(exception_message[keyname], str):
                        failures['rawData']['response']['exception']['message'] = exception_message[keyname]

            print('Resubmitting document id: {}'.format(failures['esDocumentId']))
            response = es_conn.create(
                index=failures['esIndexName'],
                doc_type=failures['esTypeName'],
                body=failures['rawData'],
                id=failures['esDocumentId']
            )

        except TransportError as e:
            if e.status_code == 409 and e.error == 'version_conflict_engine_exception':
                pass
            else:
                all_resubmitted = False

        except Exception as e:
            print('An error occurred resubmitting the data for {}: {}'.format(failures['esDocumentId'], e))
            all_resubmitted = False


    if all_resubmitted:
        boto_session.resource('s3').Object(bucket, key).delete()
        print('Removed {}'.format(key))

    else:
        print('Did not remove {}'.format(key))


if __name__ == '__main__':
    manager.main()
