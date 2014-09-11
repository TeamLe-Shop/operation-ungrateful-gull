
import json
import logging
import socketserver


class ZordzmanHandler(socketserver.BaseRequestHandler):

    MAGIC_NUMBER = b"\xCA\xC3\x55"
    PROTOCOL_VERSION = 0

    def send(self, type_, message):
        """Send a message

        Converts the given `message` to JSON and encodes it as UTF-8 with a
        trailing null-byte which is then send to the client.

        :param message: a JSONable object.
        """
        message = {"type": type_, "message": message}
        self.request.sendall(json.dumps(message).encode("utf-8") + b"\x00")

    def _process_message(self, message):
        """Dispatch JSON messages to handlers

        This expects the `message` to be a dictionary a `type` and `entity`
        field. The value for `type` must be a string which is mapped to a
        handler. The handler for the message type is called with this handler
        and the `entity` value as the sole arguments.

        If a handler for the type of message hasn't been registered on the
        server then a warning is logged.

        :param message: the decoded JSON object.
        """
        if not isinstance(message, dict):
            self.log.error("Message is not a dictionary: {!r}".format(message))
            return
        if "type" not in message and not isinstance(message["type"], str):
            self.log.error("Message missing type field or "
                           "type field is the wrong type: {!r}".format(message))
            return
        if "entity" not in message:
            self.log.error("Message doesn't contain "
                           "an entity field: {!r}".format(message))
            return
        if message["type"] not in self.server.type_handlers:
            self.log.warning("No handler registered for "
                             "'{0.type}': {0.entity!r}".format(message))
            return
        # I don't know how to deal with errors in type handlers? Just smother
        # them and log the exception?
        self.server.type_handlers[message["type"]](self, message["entity"])

    def _read_json(self):
        """Read and process JSON messages from the stream

        Each 'JSON message' is delimited by a null-byte. Each message is
        decoded as UTF-8. If the message cannot be decoded then the connection
        is terminated.
        """
        self.buffer = b""
        while True:
            recv = self.request.recv(8192)
            if not recv:
                self.log.debug("Connection terminated by client")
                return
            self.buffer += recv
            message_end = self.buffer.find(b"\x00")
            if message_end == -1:
                continue
            message = self.buffer[:message_end]
            self.buffer = self.buffer[message_end:]
            try:
                message = message.decode("utf-8")
            except UnicodeDecodeError:
                self.log.exception("Couldn't decode message as UTF-8")
                return
            try:
                json_object = json.loads(message)
            except ValueError:
                self.log.exception("Malformed JSON message")
                return
            self._process_message(json_object)

    def handler(self):
        """Connection handler entry point

        Checks for the existence of the magic number at the beginning of the
        stream and that the correct protocol version is in use. If either of
        these checks fail the connection will be terminated.
        """
        self.log = logging.getLogger("client." + self.client_address[0])
        self.log.info("Connection established")
        # Read magic number and protocol version
        self.buffer = self.request.recv(4)
        if len(self.buffer) < 4:
            self.log.error("Incomplete protocol identifier")
            return
        if not self.buffer.startswith(self.MAGIC_NUMBER):
            self.log.error("Bad magic number")
            return
        if self.buffer[3] != self.PROTOCOL_VERSION:
            self.log.error("Wrong protocol version")
            return
        self._read_json()

    def finish(self):
        self.log.info("Disconnecting")


class ZordzmanServer(socketserver.TCPServer):

    def __init__(self, address):
        super().__init__(address, ZordzmanHandler)
        self.type_handlers = {}

    def register_handler(self, type_, handler):
        """Registers a handler for a message type"""
        # Perhaps support multiple handlers for a single message type?
        if type_ in self.type_handlers:
            raise ValueError("Handler already exists "
                             "for message type '{}'".format(type_))
        # I won't pedantically check for __call__ on `handler` or that it
        # has the right signature because that is lame as balls
        self.type_handlers[type_] = handler
