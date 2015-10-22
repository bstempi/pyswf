import json

from google.protobuf import message


class Serializer:
    """
    Defines how to serialize/unserialize messages used in SWF communications
    """
    # TODO This interface needs an update.  When storing results, they are a messsage.  When storing arguments to a
    # function, they're something else.  This interface needs to reflect that with additional methods and args.

    def deserialize(self, message_string):
        """
        Handles deserialization of the message
        :return: a message object
        """
        raise NotImplementedError()

    def serialize(self, message):
        """
        Handles serialization of the message
        :return: a string representing the message
        """
        raise NotImplementedError()


class ProtobufSerializer(Serializer):
    """
    Handles serialization/deserializaton of protobuf messages
    """

    def __init__(self, message_class):
        test_instance = message_class()
        if not isinstance(test_instance, message.Message):
            raise Exception("The message class must be a protobuf message class, meaning that it must inherit from "
                            "google.protobuf.message.Message")
        self._message_class = message_class

    def deserialize(self, message_string):
        if message_string is None:
            return None
        deserialized_message = self._message_class()
        deserialized_message.ParseFromString(message_string)
        result = (list(), dict())
        result[0].append(deserialized_message)
        return result

    def serialize(self, message):

        if message is None:
            return None
        # TODO THIS IS A TERRIBLE HACK
        if isinstance(message, tuple):
            if len(message[0]) + len(message[1]) > 1:
                raise Exception('Protobuf serializer can only serialize one argument.')
            if len(message[0]) > 0:
                serialized_message = message[0][0].SerializeToString()
            elif len(message[1]) > 0:
                serialized_message = message[1].items()[0].SerializeToString()
            else:
                raise Exception('No arguments were passed')
        else:
            serialized_message = message.SerializeToString()
        return serialized_message


class JsonSerializer(Serializer):
    """
    Handles serialization/deserializaton of protobuf messages
    """

    def __init__(self):
        pass  # Nothing to do here

    def deserialize(self, message_string):
        return json.loads(message_string)

    def serialize(self, message):
        return json.dumps(message)
