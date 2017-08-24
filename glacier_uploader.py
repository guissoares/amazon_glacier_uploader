#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import cStringIO
import json
import boto3
from botocore.utils import calculate_tree_hash
from botocore.exceptions import ClientError

parser = argparse.ArgumentParser(description='Utility that simplifies the process of performing a multipart upload to Amazon Glacier.')
parser.add_argument('filepath', help='path to the archive to be upload')
parser.add_argument('-v', '--vault-name', default='', help='vault name')
parser.add_argument('-d', '--archive-description', default='', help='string describing the archive')
parser.add_argument('-s', '--part-size', type=int, default=4294967296/1024, help='size of each part (in bytes)')
args = parser.parse_args()

filepath = args.filepath
vault_name = args.vault_name
archive_description = args.archive_description
part_size = args.part_size

while vault_name == '':
    vault_name = raw_input('Vault name: ')
if archive_description == '':
    archive_description = raw_input('Archive description: ')

client = boto3.client('glacier')

print 'Starting upload...'
response = client.initiate_multipart_upload(
    vaultName=vault_name,
    archiveDescription=archive_description,
    partSize=str(part_size)
)
print json.dumps(response, indent=4, sort_keys=True)
upload_id = response['uploadId']

try:
    with open(filepath, 'r') as fp:

        fp.seek(0, 2)
        total_size = fp.tell()
        fp.seek(0)
        offsets_start = range(0, total_size, part_size)
        offsets_end = [x+part_size-1 for x in offsets_start]
        offsets_end[-1] = total_size-1
        for i, offset_start, in enumerate(offsets_start):
            offset_end = offsets_end[i]
            range_str = 'bytes {}-{}/*'.format(offset_start, offset_end)
            data = fp.read(part_size)
            part_tree_hash = calculate_tree_hash(cStringIO.StringIO(data))
            print 'Sending part {} of {} ({})...'.format(i, len(offsets_start), range_str)
            print '- SHA256 tree hash (local):  {}'.format(part_tree_hash)
            while True:
                try:
                    response = client.upload_multipart_part(
                        vaultName=vault_name,
                        uploadId=upload_id,
                        checksum=part_tree_hash,
                        range=range_str,
                        body=data
                    )
                    break
                except ClientError:
                    print 'Trying again...'
            print '- SHA256 tree hash (remote): {}'.format(response['checksum'])

        print 'Calculating tree hash of the entire archive...'
        fp.seek(0)
        total_tree_hash = calculate_tree_hash(fp)
        print total_tree_hash

    print 'Completing upload...'
    response = client.complete_multipart_upload(
        vaultName=vault_name,
        uploadId=upload_id,
        archiveSize=str(total_size),
        checksum=total_tree_hash
    )
    print json.dumps(response, indent=4, sort_keys=True)

except:
    response = client.abort_multipart_upload(
        vaultName=vault_name,
        uploadId=upload_id
    )
    raise
