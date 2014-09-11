
import io
import json

import formencode


class MessageError(ValueError):
    pass


class MessageMeta(type):

    types = {}

    def __new__(meta, name, bases, attrs):
        if name == "Message":
            return type.__new__(meta, name, bases, attrs)
        if not attrs.get("type_"):
            raise TypeError("Message type_ must be set")
        if not attrs.get("schema"):
            raise TypeError("Message entity schema must be set")
        name = attrs["type_"]
        if name in meta.types:
            raise TypeError("Message type {!r} already "
                            "registered to {!r}".format(name, meta.types[name]))
        # Surely this is wrong? There's got to be something like
        # __subclasses__ right?
        cls = type.__new__(meta, name, bases, attrs)
        meta.types[cls.type_] = cls
        return cls


class Message(metaclass=MessageMeta):

    type_ = None
    schema = None

    def __init__(self, entity):
        self.entity = entity

    def encode(self):
        return json.dumps({
            "type": self.type_,
            "entity": self.schema.from_python(self.entity),
        }).encode("utf-8") + "\x00"

    @classmethod
    def decode(cls, raw):
        """Decode a raw message

        This will decode a UTF-8 encoded bytestring and then attempt to decode
        a JSON message from it. Both Unicode decode and JSON decode exceptions
        are reraised as :class:`MessageError`.

        The top-level JSON is expected to be an object with two fields:
        ``type`` and ``entity``. ``type`` should be a string that identifies a
        :class:`Message` subclass (see :attr:`Messsage.type_`). It is an
        instance of this subclass that is returned by this method.

        The ``entity`` field can be any JSON object that is valid according to
        the message type's schema.

        Formencode validation errors are propagated.

        :param raw: a byte string of the raw message, not including the null
            terminator.
        :return: a class:`Message` subclass instance which corresponds to the
            type of message encoded in the the raw message.
        """
        try:
            raw_string = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MessageError(exc)
        print(raw_string)
        try:
            json_obj = json.loads(raw_string)
        except ValueError as exc:
            raise MessageError(exc)
        # Could/should probably use formencode for this validation
        if not isinstance(json_obj, dict):
            raise MessageError("Top-level of JSON must be an object")
        message_type = json_obj.get("type")
        # Again, surely there's a better way
        try:
            message_cls = MessageMeta.types[message_type]
        except KeyError:
            raise MessageError("No message with type {!r}".format(message_type))
        return message_cls(message_cls.schema.to_python(json_obj.get("entity")))

    @classmethod
    def decode_from_buffer(cls, buffer_):
        """Attempt to decode messages from a stream of bytes

        This is a generator that yields :class:`Message` subclass instances
        for each message in the given ``buffer_``. Each message in the buffer
        is null-terminated.

        :param buffer_: a :class:`io.BytesIO`.
        """
        buffered = io.BufferedReader(buffer_)
        while True:
            peek = buffered.peek()
            terminator = peek.find(b"\x00")
            if terminator == -1:
                # Nothing left to read
                return
            yield cls.decode(buffered.read(terminator + 1)[:-1])


class ErrorMessage(Message):

    type_ = "error"
    schema = formencode.validators.NotEmpty()


if __name__ == "__main__":
    buffer_ = io.BytesIO(b'{"type": "error", "entity": "k"}\x00okay')
    for message in Message.decode_from_buffer(buffer_):
        print(message)
