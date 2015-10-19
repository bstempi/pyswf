import json

from google.protobuf import message


class Serializer:
    """
    Defines how to serialize/unserialize messages used in SWF communications
    """

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
        if not isinstance(message_class, message.Message):
            raise Exception("The message class must be a protobuf message class, meaning that it must inherit from "
                            "google.protobuf.message.Message")
        self._message_class = message_class

    def deserialize(self, message_string):
        deserialized_message = self._message_class()
        deserialized_message.ParseFromString(message_string)
        return deserialized_message

    def serialize(self, message):
        serialized_message = self._message_class()
        serialized_message.SerializeToString()
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
