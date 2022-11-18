import pickle
import socket

from PIL import Image


class Message:
    """Message Type."""

    def __init__(self, command: str):
        self._command = command

    @property
    def command(self):
        return self._command


class JoinMessage(Message):
    """Message to join the network."""

    def __init__(self, addr: list):
        super().__init__("join")
        self._addr = addr

    @property
    def addr(self):
        return self._addr

    def __bytes__(self) -> str:
        return pickle.dumps({"command": self._command, "addr": self._addr})


class ConfigMessage(Message):
    """Message to configure a peer after joining the network.

    Parameters:
        from_id: identification of the sender.
        new_id: identification that new peer should use.
        net_info: information about all peers - identification (id), address (addr), and hashcodes (hash).
            { id: { addr: (host, port), hash: {h1,..,hN} } }
    """

    def __init__(self, from_id: int, new_id: int, net_info: dict):
        super().__init__("config")
        self._from_id = from_id
        self._new_id = new_id
        self._net_info = net_info

    @property
    def from_id(self):
        return self._from_id

    @property
    def new_id(self):
        return self._new_id

    @property
    def net_info(self):
        return self._net_info

    def __bytes__(self) -> str:
        net_info = self._net_info
        return pickle.dumps({"command": self._command, "from_id": self._from_id, "new_id": self._new_id, "net_info": net_info})


class UpdateMessage(Message):
    """Message to update the network info of a peer.

    Parameters:
        from_id: identification of the sender.
        add: peer addr and/or hashcodes. All keys are optional.
            { id: { addr: (host, port), hash: {h1,..,hN}, size: value } }
        rem: peer addr and/or hashcodes. All keys are optional.
            { id: { addr: (host, port), hash: {h1,..,hN}, size: value } }
    """

    def __init__(self, from_id: int, add: dict, rem: dict):
        super().__init__("update")
        self._from_id = from_id
        self._add = add
        self._rem = rem

    @property
    def from_id(self):
        return self._from_id

    @property
    def add(self):
        return self._add

    @property
    def rem(self):
        return self._rem

    def __bytes__(self) -> str:
        return pickle.dumps({"command": self._command, "from_id": self._from_id, "add": self._add, "rem": self._rem})


class RequestImageMessage(Message):
    """Message to request an image from a peer."""

    def __init__(self, from_id: int, hash: bytes):
        super().__init__("request_image")
        self._from_id = from_id
        self._hash = hash

    @property
    def from_id(self):
        return self._from_id

    @property
    def hash(self):
        return self._hash

    def __bytes__(self) -> str:
        return pickle.dumps({"command": self._command, "from_id": self._from_id, "hash": self._hash})


class ImageMessage(Message):
    """Message to send an image to a peer."""

    def __init__(self, from_id: int, hash: bytes, image: Image, fname: str, store: bool = False):
        super().__init__("image")
        self._from_id = from_id
        self._hash = hash
        self._image = image
        self._fname = fname
        self._store = store

    @property
    def from_id(self):
        return self._from_id

    @property
    def hash(self):
        return self._hash

    @property
    def image(self):
        return self._image

    @property
    def fname(self):
        return self._fname

    @property
    def store(self):
        return self._store

    def __bytes__(self) -> str:
        return pickle.dumps({"command": self._command, "from_id": self._from_id, "hash": self._hash, "image": self._image, "fname": self._fname, "store": self._store})


class RequestListMessage(Message):
    """Message to request list of all hashes in distributed system."""

    def __init__(self, from_id: int):
        super().__init__("request_list")
        self._from_id = from_id

    @property
    def from_id(self):
        return self._from_id

    def __bytes__(self) -> str:
        return pickle.dumps({"command": self._command, "from_id": self._from_id})


class ListMessage(Message):
    """Message to send a list of all hashes to client."""

    def __init__(self, hashes: set):
        super().__init__("list")
        self._hashes = list(hashes)

    @property
    def hashes(self):
        return self._hashes

    def __bytes__(self) -> str:
        return pickle.dumps({"command": self._command, "hashes": self._hashes})


class PeerProto:
    """Peer protocol for communication."""

    @classmethod
    def join(cls, addr: tuple):
        """Creates a JoinMessage."""
        return JoinMessage(addr)

    @classmethod
    def config(cls, from_id: int, your_id: int, net_info: dict):
        """Creates a ConfigMessage."""
        return ConfigMessage(from_id, your_id, net_info)

    @classmethod
    def update(cls, from_id: int, add: dict = {}, rem: dict = {}):
        """Creates an UpdateMessage."""
        return UpdateMessage(from_id, add, rem)

    @classmethod
    def request_image(cls, from_id: int, hash: bytes):
        """Creates a RequestImageMessage."""
        return RequestImageMessage(from_id, hash)

    @classmethod
    def image(cls, from_id: int, hash: bytes, image: Image, fname: str, store: bool = False):
        """Creates a ImageMessage."""
        return ImageMessage(from_id, hash, image, fname, store)

    @classmethod
    def request_list(cls, from_id: int):
        """Creates a RequestListMessage."""
        return RequestListMessage(from_id)

    @classmethod
    def list(cls, hashes: set):
        """Creates ListMessage."""
        return ListMessage(hashes)

    @classmethod
    def send_msg(cls, conn: socket.socket, msg: Message):
        """Sends through a connection a Message object."""
        if conn.fileno() == -1:
            return
        m: bytes = bytes(msg)
        mlen: bytes = len(m).to_bytes(4, "big")
        conn.sendall(mlen + m)

    @classmethod
    def recv_msg(cls, conn: socket.socket) -> Message:
        """Receives through a connection a Message object."""
        if conn.fileno() == -1:
            return None
        mlen: int = int.from_bytes(conn.recv(4), "big")
        if mlen == 0:  # Connection was closed
            return None

        m: bytes = PeerProto.__recvall(conn, mlen)
        msg = pickle.loads(m)

        if "command" not in msg:
            raise PeerProtoBadFormat(msg)

        if msg["command"] == "join" and "addr" in msg:
            return PeerProto.join(msg["addr"])
        elif msg["command"] == "config" and "from_id" in msg and "new_id" in msg and "net_info" in msg:
            return PeerProto.config(msg["from_id"], msg["new_id"], msg["net_info"])
        elif msg["command"] == "update" and "from_id" in msg and "add" in msg and "rem" in msg:
            return PeerProto.update(msg["from_id"], msg["add"], msg["rem"])
        elif msg["command"] == "request_image" and "from_id" in msg and "hash" in msg:
            return PeerProto.request_image(msg["from_id"], msg["hash"])
        elif msg["command"] == "image" and "from_id" in msg and "hash" in msg and "image" in msg and "fname" in msg and "store" in msg:
            return PeerProto.image(msg["from_id"], msg["hash"], msg["image"], msg["fname"], msg["store"])
        elif msg["command"] == "request_list" and "from_id" in msg:
            return PeerProto.request_list(msg["from_id"])
        elif msg["command"] == "list" and "hashes" in msg:
            return PeerProto.list(msg["hashes"])
        else:
            raise PeerProtoBadFormat(msg)

    @classmethod
    def __recvall(cls, conn: socket.socket, n: int) -> bytes:
        """Receives n bytes through a connection."""
        data = bytearray()
        while len(data) < n:
            pack = conn.recv(n - len(data))
            data.extend(pack)
        return bytes(data)  # Immutable


class PeerProtoBadFormat(Exception):
    """Exception when source message is not PeerProto."""

    def __init__(self, original_msg: bytes = None):
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
