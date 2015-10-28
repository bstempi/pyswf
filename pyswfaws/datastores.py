import boto

import pyswfaws.util


class DataStore:
    """
    Defines places that we'll store messages meant for SWF.
    """

    def put(self, message, key):
        """
        Stores a piece of data
        :param message: the serialized message to store
        :param key: unique key to identify this message
        """
        pass

    def get(self, key):
        """
        Retrieves a piece of data
        :param key: unique key to identify this message
        """
        pass


class SwfDataStore(DataStore):
    """
    This data store uses the "input" field in the SWF API to transmit data.

    This class handles enforcing the character limit.
    """
    _message_size_limit = 32000

    def __init__(self):
        pass

    def put(self, message, key):
        """
        Stores a piece of data
        :param message: the serialized message to store
        :param key: unique key to identify this message
        """

        """
        Sneacky hack:  The key returned by this method call ends up in an input
        or result field somewhere.  Because we want the message to end up
        there, we'll just return it as-is.  JSON escaping needs to happen
        at the caller's end.
        """
        return message

    def get(self, key):
        """
        Retrieves a piece of data
        :param key: unique key to identify this message
        """

        """
        Sneacky hack:  The key passed into this method call came from an input
        or result field.  The message is the key itself.  Since the caller is
        responsible for JSON encoding/decoding, we can safely return it as-is.
        """
        return key

    def _message_size_check(self, message):
        if len(message) > self._message_size_limit:
            raise Exception('SWF cannot store messages greater than 32k in size.')


class S3DataStore(DataStore):
    """
    Uses S3 to store serialized messages.
    """

    def __init__(self, bucket, base_key, aws_key=None, aws_secret=None):
        """
        Initializes the datastore, creating an S3 client via Boto.
        :param aws_key: The key to access S3.  If None is passed, then Boto will attempt to find credentials elsewhere.
        :param aws_secret: The secret key to access S3.  See aws_key.
        :return:
        """
        self._s3_client = boto.connect_s3(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        self._bucket = self._s3_client.get_bucket(bucket, validate=False)
        if base_key != '' and base_key[-1:] != '/':
            self._base_key_name = base_key + '/'
        else:
            self._base_key_name = ''

    def put(self, message, key):
        """
        Stores a piece of data
        :param message: the serialized message to store
        :param key: unique key to identify this message
        """

        s3_key = boto.s3.key.Key(self._bucket)
        s3_key.key = '{}{}'.format(self._base_key_name, key)
        s3_key.set_contents_from_string(message)
        return 's3://{}/{}'.format(self._bucket.name, s3_key.key)

    def get(self, key):
        """
        Retrieves a piece of data
        :param id: the id returned by put()
        :param key: unique key to identify this message
        """
        return pyswfaws.util.get_from_s3(self._s3_client, key)
