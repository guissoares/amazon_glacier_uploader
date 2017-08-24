# amazon_glacier_uploader
Python script that simplifies the process of performing a multipart upload to Amazon Glacier.

#### Usage
    glacier_uploader.py [-h] [-v VAULT_NAME] [-d ARCHIVE_DESCRIPTION]
                             [-s PART_SIZE]
                             filepath

#### Known issues
In order to avoid splitting the file into multiple parts and saving them all to the disk, this script loads each part into the memory before sending it, which can lead to `MemoryError` exceptions. Reducing the size of each part can solve this problem.
