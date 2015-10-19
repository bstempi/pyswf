from urlparse import urlparse

import urllib2


def get_from_s3(s3_client, s3_url):
    """
    Convenience method to download data from S3 and return it as a string
    :param s3_client: A valid S3 client
    :param s3_url: A URL as it appears in the S3 console.  Eg, https://s3.amazonaws.com/somebucket/somekey
    :return:
    """
    url = urlparse(s3_url)

    # Split the bucket from the key
    bucket_name = urllib2.unquote(url.netloc).decode('utf8')
    key_name = urllib2.unquote(url.path[1:]).decode('utf8')

    # We're done parsing; start doing some S3 ops
    bucket = s3_client.get_bucket(bucket_name, validate=False)
    key = bucket.get_key(key_name)
    return key.get_contents_as_string()
