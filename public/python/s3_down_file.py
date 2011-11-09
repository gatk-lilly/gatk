import base64
import boto
import boto.s3
import boto.s3.connection
from boto.s3.key import Key
import commands
from filechunkio import FileChunkIO
from key_gen import SecureS3
import getopt
import hashlib
import logging
import math
from math import ceil
import mimetypes
from multiprocessing import Pool
import os
import subprocess
import sys
import time
from time import strftime


def get_conn(proxy_server):

    aws_access_key_id = os.environ["AWS_ACCESS_KEY"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    
    calling_format=boto.s3.connection.OrdinaryCallingFormat()
    conn = boto.s3.connection.S3Connection(aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)

    return conn

    
def split_url(url):
    protocol, host_port = url.split("//")
    host, port = host_port.split(":")
    
    return host, port


def log(message):
    print("[%s] - %s" % (strftime("%Y-%m-%d %H:%M:%S"), message))


def do_part_download(args):
    """
    Download a part of an S3 object using Range header

    We utilize the existing S3 GET request implemented by Boto and tack on the
    Range header. We then read in 1Mb chunks of the file and write out to the
    correct position in the target file

    :type args: tuple of (string, string, int, int)
    :param args: The actual arguments of this method. Due to lameness of
                 multiprocessing, we have to extract these outside of the
                 function definition.

                 The arguments are: S3 Bucket name, S3 key, local file name,
                 chunk size, and part number
    """
    bucket_name, key_name, file_name, min_byte, max_byte = args
    conn = get_conn()

    # Make the S3 request
    resp = conn.make_request("GET", bucket=bucket_name,
            key=key_name, headers={'Range':"bytes=%d-%d" % (min_byte, max_byte)})

    # Open the target file, seek to byte offset
    fd = os.open(file_name, os.O_WRONLY)
    log("Opening file {0}, seeking to {1}".format(file_name, min_byte))
    os.lseek(fd, min_byte, os.SEEK_SET)

    chunk_size = min((max_byte-min_byte), 32*1024*1024)
    log("Reading HTTP stream into {0}M chunks".format(chunk_size / (1024.0 * 1024.0)))
    start_time = time.time()
    s = 0
    while True:
        data = resp.read(chunk_size)
        if data == "":
            break
        os.write(fd, data)
        s += len(data)
    time_diff = time.time() - start_time
    os.close(fd)
    s /= (1024.0 * 1024.0)
    log("Downloaded %0.2fM in %0.2fs (%0.2fMbps)" % (s, time_diff, s/time_diff))


def gen_byte_ranges(size, num_parts):
    part_size = int(ceil(1.0 * size / num_parts))
    for i in range(num_parts):
        yield (part_size*i, min(part_size*(i+1)-1, size-1))


def multi_download(bucket_name, key_name, dest_file_path, num_processes=10, force_overwrite=True):
    
    # Check that dest does not exist
    if os.path.exists(dest_file_path) and force_overwrite:
        os.remove(dest_file_path)
    elif os.path.exists(dest_file_path):
        raise ValueError("Destination file '{0}' exists".format(dest_file_path))

    # Touch the file
    fd = os.open(dest_file_path, os.O_CREAT)
    os.close(fd)

    conn = get_conn()
    bucket = conn.lookup(bucket_name, validate=False)
    key = bucket.get_key(key_name)
    size = key.size
    
    num_parts = num_processes

    def arg_iterator(num_parts):
        for min_byte, max_byte in gen_byte_ranges(size, num_parts):
            yield (bucket.name, key.name, dest_file_path, min_byte, max_byte)

    s = size / (1024.0 * 1024.0)
    
    try:
        start_time = time.time()
        pool = Pool(processes=num_processes)
        pool.map_async(do_part_download, arg_iterator(num_parts)).get(9999999)
        time_diff = time.time() - start_time
        log("Finished downloading %0.2fM in %0.2fs (%0.2fMbps)" % (s, time_diff, s/time_diff))
    except KeyboardInterrupt:
        log("User terminated")
    except Exception, err:
        log(err)


def aria2_download(bucket_name, key_name, dest_dir, file_name):
		
    bucket = bucket_name
    filepath = key_name
    dir = dest_dir
    secondsToExpire = 1200
    
    aws_access_key_id = os.environ["AWS_ACCESS_KEY"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    
    s3 = SecureS3(aws_access_key_id, aws_secret_access_key)
    url = s3.get_auth_link(bucket, filepath, secondsToExpire)
    print url
    
    
    log("Starting aria2c download of " + key_name)
    output = commands.getoutput("aria2c --dir=" + dest_dir + " --out=" + file_name + " --split=4 --min-split-size=100M --max-connection-per-server=4 --check-certificate=false --file-allocation=none '" + url + "'")
    log("Finished aria2c download.  Created " + dest_dir + "/" + file_name)


def usage():
    print("""
python s3_down_file.py [option]

    The following options are available: 

    -b bucket name
    --bucket-name=bucket name

    -d destination directory
    --dir=destination directory
    
    -f destination file name
    --file-name=destination file name
    
    -p proxy server (http://40.0.40.10:9000)
    --proxy-server=proxy server
    
    -s S3 file path
    --s3-file-path=S3 file path
    
    -h
    --help
      Print this help message.

    There are three environment variables that you need you need to have set:

    AWS_ACCESS_KEY
    AWS_SECRET_ACCESS_KEY 
    http_proxy (when inside the Lilly firewall)

    You also must be using a python instance that has the boto package installed: 

    export PATH=/lrlhps/apps/python/Python-2.7.2/bin:$PATH
      
""")


if __name__ == "__main__":
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hb:d:f:s:",
                                   ["help", "bucket-name=", "dir=", "file-name=", "s3-file-path="])
    except getopt.GetoptError, err:
        # Print help information and exit.
        print str(err) 
        usage()
        sys.exit(2)

    # These will always be specified.    
    bucket_name = None
    s3_file_path = None
    dest_dir = None

    # Optional arguements
    file_name = None
    
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-b", "--bucket-name"):
            bucket_name = a
        elif o in ("-d", "--dir"):
            dest_dir = a
        elif o in ("-f", "--file-name"):
            file_name = a
        elif o in ("-s", "--s3-file-path"):
            s3_file_path = a
        else:
            assert False, "unhandled option"
    
    if bucket_name is None:
        # Print help information and exit.
        usage()
        sys.exit(2)
        
    if dest_dir is None:
        # Print help information and exit.
        usage()
        sys.exit(2)
        
    if s3_file_path is None:
        # Print help information and exit.
        usage()
        sys.exit(2)
                
    log("start")
 
    if file_name is None:
       file_name = os.path.basename(s3_file_path)
       
    aria2_download(bucket_name, s3_file_path, dest_dir, file_name)
    
    log("end")
