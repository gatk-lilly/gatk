import boto.s3
import boto.s3.connection
from filechunkio import FileChunkIO
import logging
import math
import mimetypes
from multiprocessing import Pool
import os
from time import strftime
import sys

def get_conn():

    aws_access_key_id = os.environ["AWS_ACCESS_KEY"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    
    calling_format=boto.s3.connection.OrdinaryCallingFormat()
    conn = boto.s3.connection.S3Connection(aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)

    return conn


def _upload_part(bucketname, multipart_id, part_num, source_path, offset, bytes, amount_of_retries=10):
    """
    Uploads a part with retries.
    """
    def _upload(retries_left=amount_of_retries):
        try:
            log('Start uploading part #%d ...' % part_num)
            conn = get_conn()
            bucket = conn.get_bucket(bucketname)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_path, 'r', offset=offset,
                        bytes=bytes) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num)
                    break
        except Exception, exc:
            if retries_left:
                _upload(retries_left=retries_left - 1)
            else:
                log('... Failed uploading part #%d' % part_num)
                raise exc
        else:
            log('... Uploaded part #%d' % part_num)

    _upload()


def split_url(url):
    protocol, host_port = url.split("//")
    host, port = host_port.split(":")
    
    return host, port


def upload(bucketname, source_path, keyname, acl='private', headers={}, guess_mimetype=True, parallel_processes=40):
    """
    Parallel multipart upload.
    """
    conn = get_conn()
    bucket = conn.get_bucket(bucketname)
    
    if guess_mimetype:
        mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
        headers.update({'Content-Type': mtype})

    mp = bucket.initiate_multipart_upload(keyname, headers=headers)

    source_size = os.stat(source_path).st_size
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)),
        5242880)
    chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
  
    log("Uploading %s bytes in %s chunks" % (source_size, chunk_amount))

    pool = Pool(processes=parallel_processes)
    for i in range(chunk_amount):
        offset = i * bytes_per_chunk
        remaining_bytes = source_size - offset
        bytes = min([bytes_per_chunk, remaining_bytes])
        part_num = i + 1
        pool.apply_async(_upload_part, [bucketname, mp.id, part_num, source_path, offset, bytes])
    pool.close()
    pool.join()

    if len(mp.get_all_parts()) == chunk_amount:
        mp.complete_upload()
        key = bucket.get_key(keyname)
        key.set_acl(acl)
    else:
        mp.cancel_upload()


def log(message):
    print("[%s] - %s" % (strftime("%Y-%m-%d %H:%M:%S"), message))
    

if __name__ == "__main__":
    
    if len(sys.argv) > 1:
         log("start")
         
         bucket_name = sys.argv[1]
         file_path = sys.argv[2]
         s3_path = sys.argv[3]

         file_name = os.path.basename(file_path)
         s3_file_path = s3_path + "/" + file_name
         
         upload(bucket_name, file_path, s3_file_path)
         log("end")   
    else:
        help = """
usage: python s3_upload.py [s3 bucket name] [file path] [s3 path]

There are three environment variables that you need you need to have set:

AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY 
"""
        print help
        
