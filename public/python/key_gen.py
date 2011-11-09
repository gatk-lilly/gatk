import hmac
import time
import base64
import urllib
import hashlib

class SecureS3(object):
    def __init__(self, key, secret_key):
        ''' key: S3 Access Key (login)
            secret_key: S3 Secret Access Key (password)
        '''
        self.key = key
        self.secret_key = secret_key
    def gen_signature(self, string_to_sign):
        return base64.encodestring(
            hmac.new(
                self.secret_key,
                string_to_sign,
                hashlib.sha1
            ).digest()
        ).strip()
    def get_auth_link(self, bucket, filename, expires, timestamp=None):
        ''' Return a secure S3 link with an expiration on the download.
            bucket: Bucket name
            filename: file path
            expires: Seconds from NOW the link expires
            timestamp: Epoch timestamp. If present, "expires" will not be used.
        '''
        filename = urllib.quote_plus(filename)
        filename = filename.replace('%2F', '/')
        path = '/%s/%s' % (bucket, filename)
        if timestamp is not None:
            expire_time = float(timestamp)
        else:
            expire_time = time.time() + expires
        expire_str = '%.0f' % (expire_time)
        string_to_sign = u'GET\n\n\n%s\n%s' % (expire_str, path)
        params = {
            'AWSAccessKeyId': self.key,
            'Expires': expire_str,
            'Signature': self.gen_signature(string_to_sign.encode('utf-8')),
        }
        return 'https://%s.s3.amazonaws.com/%s?%s' % (
                                    bucket, filename, urllib.urlencode(params))

