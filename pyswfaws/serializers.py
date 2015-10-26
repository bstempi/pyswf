import json

from abc import ABCMeta, abstractmethod

from google.protobuf import message


class Serializer():
    """
    Defines how to serialize/unserialize messages used in SWF communications
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def deserialize_input(self, raw_inputs):
        """
        Deserializes a function input as a tuple with (args, kwargs).

        Example of how this function might be used:
        a = some_serializer.deserialize_input(raw_input)
        some_func(*a[0], **a[1])
        :param raw_inputs:
        :return:
        """
        pass

    @abstractmethod
    def deserialize_result(self, raw_result):
        """
        Deserializes a single-value result
        :param raw_result:
        :return:
        """
        pass

    @abstractmethod
    def serialize_input(self, args, kwargs):
        """
        Serializes the input to a function.

        :param args: an iterable of values
        :param kwargs: a dict of values
        :return:
        """
        pass

    @abstractmethod
    def serialize_result(self, result):
        """
        Serializes a single-value result
        :param result:
        :return:
        """
        pass


class ProtobufSerializer(Serializer):
    """
    Handles serialization/deserializaton of protobuf messages

    Both inputs and results are expected to be a single protobuf object.  That is, if you're using a protobuf object
    as input, then it may appear in teh args or kwargs, but not both.  There must be exactly one argument.
    """

    __slots__ = ['_message_class']

    def __init__(self, message_class):
        test_instance = message_class()
        if not isinstance(test_instance, message.Message):
            raise Exception("The message class must be a protobuf message class, meaning that it must inherit from "
                            "google.protobuf.message.Message")
        self._message_class = message_class

    def deserialize_input(self, raw_inputs):
        result = (list(), dict())
        result[0].append(self.deserialize_result(raw_inputs))
        return result

    def deserialize_result(self, raw_result):
        deserialized_message = self._message_class()
        deserialized_message.ParseFromString(raw_result)
        return deserialized_message

    def serialize_input(self, args=None, kwargs=None):
        args_len = 0
        kwargs_len = 0
        if args:
            args_len = len(args)
        if kwargs:
            kwargs_len = len(kwargs)

        total_len = args_len + kwargs_len
        if total_len == 0:
            raise Exception('Serializer cannot be called with 0 arguments')
        if total_len > 1:
            raise Exception('Protobuf serializer cannot have more than one argument for an input')

        # Because we checked the total length of all of the args, these two things are mutually exclusive
        if args_len > 0:
            message = args[0]
        if kwargs_len > 0:
            message = kwargs.items()[0]

        return message.SerializeToString()

    def serialize_result(self, result):
        if result is None:
            raise Exception('Serializer cannot serialize a Nonetype result')
        return result.SerializeToString()


class JsonSerializer(Serializer):
    """
    Handles serialization/deserializaton of protobuf messages
    """

    __slots__ = []

    def __init__(self):
        pass  # Nothing to do here

    def deserialize_input(self, raw_inputs):
        return json.loads(raw_inputs)

    def deserialize_result(self, raw_result):
        return json.loads(raw_result)

    def serialize_input(self, args, kwargs):
        return json.dumps((args, kwargs))

    def serialize_result(self, result):
        return json.dumps(result)
