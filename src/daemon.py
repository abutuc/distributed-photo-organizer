"""Daemon implementation."""
import imagehash
import os
import selectors
import socket

from PIL import Image
from threading import Thread, Lock

from .peer_protocol import PeerProto


class Daemon(Thread):
    def __init__(self, folder_path, addr, join_addr=None):
        Thread.__init__(self)

        self._send_mutex = Lock() # Only one thread must send data at a time
        self._folder_mutex = Lock() # Only one thread must access the folder at a time

        self._addr = addr
        self._done = False
        self._folder_path = folder_path
        self._image = {}  # { hash: path } - Indexes all images in folder
        self._net_info = {}  # { id: { addr: (host, port), hash: {h1,..,hN}, size: value } }
        self._send_conn = {}  # { id: conn } - Connections that self uses to send data
        self._recv_conn = {}  # { id: conn } - Connections that self uses to receive data

        self._own_request = set()  # { h1,..,hN }
        self._client_request = {}  # { conn: hash }

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Listener socket
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Reuse socket
        self._sock.bind(self._addr)

        self._sel = selectors.DefaultSelector()  # Selector
        self._sel.register(self._sock, selectors.EVENT_READ, self.accept)  # Wait for connections

        self._sock.listen(20)  # Number of concurrent peers
        print("\nListening on port " + str(addr[1]) + "...\n")

        if not join_addr:
            self._id = 1
            self.setAddr(self._id, self._addr)
            self.parseFolder()
        else:
            self._join_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._join_sock.connect(join_addr)

            join_msg = PeerProto.join(self._addr)
            PeerProto.send_msg(self._join_sock, join_msg)

    def accept(self, sock: socket.socket):
        """Accept a connection."""
        conn, addr = sock.accept()
        conn.setblocking(True)
        self._sel.register(conn, selectors.EVENT_READ, self.read)  # Reads data from connection

    def read(self, conn: socket.socket):
        """Reads data from connection."""
        msg = PeerProto.recv_msg(conn)

        # Connection ended (with peer)
        if msg == None and self.isDaemon(conn):
            id = self.getIdByRecvConn(conn)
            print("[DEBUG PORT=" + str(self._addr[1]) + "] Peer crashed! Id " + str(id))
            self.crashHandler(id)
            self._recv_conn.pop(id, None)
            self._send_conn.pop(id, None)
            self._sel.unregister(conn)
            conn.close()
            return
        # Connection ended (with client)
        elif msg == None and not self.isDaemon(conn):
            print("[DEBUG PORT=" + str(self._addr[1]) + "] Client disconnected!")
            self._client_request.pop(conn, None)
            self._sel.unregister(conn)
            conn.close()
            return

        # Index received connection by id
        if msg.command != "join" and msg.from_id != 0:
            self.setRecvConn(msg.from_id, conn)

        print("[DEBUG PORT=" + str(self._addr[1]) + "] Received " + msg.__class__.__name__ + (" from id " + str(msg.from_id) if msg.command != "join" else ""))

        # Create thread to deal with message
        t = Thread(target=self.action, args=[conn, msg])
        t.start()

    def action(self, conn: socket.socket, msg):
        """Takes apropriate action based on received message."""
            
        if msg.command == "join":
            # Determine new peer id
            new_id = max(self.getIds()) + 1  # Must be the highest id on the network

            # Update self
            self.setAddr(new_id, msg.addr)
            self.setRecvConn(new_id, conn)  # Index received connection by id

            # Configure new peer
            config_msg = PeerProto.config(self._id, new_id, self.getNetInfo())
            self._send_mutex.acquire()
            PeerProto.send_msg(self.getSendConn(new_id), config_msg)
            self._send_mutex.release()

            # Update every peer, except new and self
            add = {new_id: {"addr": self.getAddr(new_id)}}
            update_msg = PeerProto.update(self._id, add=add)
            for id in self.getIds():
                if id != new_id and id != self._id:
                    self._send_mutex.acquire()
                    PeerProto.send_msg(self.getSendConn(id), update_msg)
                    self._send_mutex.release()

            # Replicate self images to new peer - only if there are just 2 peers in the network (self and new)
            if len(self.getIds()) == 2:
                for hash in self.getHashes(self._id):
                    image_msg = PeerProto.image(self._id, hash, self.getImage(hash), self.getFname(hash), store=True)
                    self._send_mutex.acquire()
                    PeerProto.send_msg(self.getSendConn(new_id), image_msg)
                    self._send_mutex.release()

        elif msg.command == "config":
            # Configure self
            self._id = msg.new_id
            self._net_info = msg.net_info
            self.setSendConn(msg.from_id, self._join_sock)  # Now join socket can be indexed, we know its peer id

            # Index images of self, as entire network is known
            self._folder_mutex.acquire()
            self.parseFolder()

            # Update every peer hashes, except self
            add = {self._id: {"hash": self.getHashes(self._id), "size": self.getSize(self._id)}}
            update_msg = PeerProto.update(self._id, add=add)
            for id in self.getIds():
                if id != self._id:
                    self._send_mutex.acquire()
                    PeerProto.send_msg(self.getSendConn(id), update_msg)
                    self._send_mutex.release()

            # Replicate self images across the network
            hashes = self.getHashes(self._id)
            ids = list(self.getIds())  # Convert to list so it can be iterated
            i = 0  # Current index on ids
            for hash in hashes:
                while True:
                    i = (i + 1) % len(ids)  # Update index on circular list
                    if ids[i] != self._id:
                        image_msg = PeerProto.image(self._id, hash, self.getImage(hash), self.getFname(hash), store=True)
                        self._send_mutex.acquire()
                        PeerProto.send_msg(self.getSendConn(ids[i]), image_msg)
                        self._send_mutex.release()
                        break
            self._folder_mutex.release()

        elif msg.command == "update":
            # Add information
            for id, info in msg.add.items():
                if "addr" in info:
                    self.setAddr(id, info["addr"])
                if "hash" in info:
                    for hash in info["hash"]:
                        self.addHash(id, hash)
                if "size" in info:
                    self.setSize(id, info["size"])
            # Remove information
            for id, info in msg.rem.items():
                if "addr" in info:
                    self.setAddr(id, tuple())
                if "hash" in info:
                    for hash in info["hash"]:
                        self.removeHash(id, hash)
                if "size" in info:
                    self.setSize(id, int())

            # Sender peer doesn't have a connection with self in its "_recv_conn" - needed if self crashes
            if msg.from_id not in self._send_conn.keys():
                # Update peer by sending an empty update message
                update_msg = PeerProto.update(self._id)
                self._send_mutex.acquire()
                PeerProto.send_msg(self.getSendConn(msg.from_id), update_msg)
                self._send_mutex.release()

        elif msg.command == "request_image":
            # Received from client
            if msg.from_id == 0:
                # Image is in not in self
                if msg.hash not in self.getHashes(self._id):
                    self._client_request[conn] = msg.hash
                    id = self.getIdByHash(msg.hash)  # Id of a peer with hash
                    request_image_msg = PeerProto.request_image(self._id, msg.hash)
                    self._send_mutex.acquire()
                    PeerProto.send_msg(self.getSendConn(id), request_image_msg)
                    self._send_mutex.release()
                    return  # Self needs to wait for ImageMessage to send to client
                else:
                    # Image is in self
                    image_msg = PeerProto.image(self._id, msg.hash, self.getImage(msg.hash), self.getFname(msg.hash))
                    self._send_mutex.acquire()
                    PeerProto.send_msg(conn, image_msg)
                    self._send_mutex.release()
            else:
                # Image is in self
                image_msg = PeerProto.image(self._id, msg.hash, self.getImage(msg.hash), self.getFname(msg.hash))
                self._send_mutex.acquire()
                PeerProto.send_msg(self.getSendConn(msg.from_id), image_msg)
                self._send_mutex.release()

        elif msg.command == "image":
            # Requested by client
            for conn, hash in self._client_request.items():
                if hash == msg.hash:
                    image_msg = PeerProto.image(self._id, msg.hash, msg.image, msg.fname)
                    self._send_mutex.acquire()
                    PeerProto.send_msg(conn, image_msg)
                    self._send_mutex.release()

            # Requested by self or to be stored
            if msg.hash in self._own_request or msg.store == True:
                self._own_request.discard(msg.hash)
                # Configure self
                self._folder_mutex.acquire()
                self.addImage(msg.hash, msg.image, msg.fname)
                self.addHash(self._id, msg.hash)
                self._folder_mutex.release()

                # Update every peer hashes, except self
                add = {self._id: {"hash": {msg.hash}, "size": self.getSize(self._id)}}
                update_msg = PeerProto.update(self._id, add=add)
                for id in self.getIds():
                    if id != self._id:
                        self._send_mutex.acquire()
                        PeerProto.send_msg(self.getSendConn(id), update_msg)
                        self._send_mutex.release()

        elif msg.command == "request_list":
            list_msg = PeerProto.list(self.getNetHashes())
            self._send_mutex.acquire()
            PeerProto.send_msg(conn, list_msg)
            self._send_mutex.release()

    def setSendConn(self, id, conn: socket.socket):
        self._send_conn[id] = conn

    def getSendConn(self, id) -> socket.socket:
        if id not in self._send_conn.keys():
            id_addr = self.getAddr(id)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(id_addr)
            self._send_conn[id] = sock
        return self._send_conn[id]

    def setRecvConn(self, id, conn: socket.socket):
        self._recv_conn[id] = conn

    def getRecvConn(self, id) -> socket.socket:
        if id not in self._recv_conn.keys():
            return None
        return self._recv_conn[id]

    def getIds(self) -> set:
        return set(self._net_info.keys())

    def setAddr(self, id, addr):
        if id not in self.getIds():
            self._net_info[id] = {"addr": tuple(), "hash": set(), "size": int()}
        self._net_info[id]["addr"] = addr

    def getAddr(self, id) -> tuple:
        return tuple(self._net_info[id]["addr"])

    def addHash(self, id, hash):
        if id not in self.getIds():
            self._net_info[id] = {"addr": tuple(), "hash": set(), "size": int()}
        self._net_info[id]["hash"].add(hash)

    def removeHash(self, id, hash):
        self._net_info[id]["hash"].discard(hash)

    def getHashes(self, id) -> set:
        return set(self._net_info[id]["hash"])

    def getNetHashes(self) -> set:
        hashes = set()
        for id in self.getIds():
            hashes = hashes.union(self.getHashes(id))
        return hashes

    def setSize(self, id, size):
        if id not in self.getIds():
            self._net_info[id] = {"addr": tuple(), "hash": set(), "size": int()}
        self._net_info[id]["size"] = size

    def getSize(self, id) -> int:
        return int(self._net_info[id]["size"])

    def getNetInfo(self) -> dict:
        return dict(self._net_info)

    def getIdByAddr(self, addr) -> int:
        for id in self.getIds():
            if self.getAddr(id) == addr:
                return id

    def getIdByHash(self, hash) -> int:
        for id in self.getIds():
            for id_hash in self.getHashes(id):
                if id_hash == hash:
                    return id

    def getIdByRecvConn(self, conn) -> int:
        for id in self.getIds():
            if self.getRecvConn(id) == conn:
                return id

    def addImage(self, hash: bytes, image: Image, fname: str):
        image_path = os.path.join(self._folder_path, fname)
        format = "jpeg" if fname == "jpg" else None
        image.save(image_path, format=format)
        # Update self
        self._image[hash] = image_path
        self.setSize(self._id, self.getFolderSize())

    def removeImage(self, hash: bytes):
        image_path = self._image.pop(hash)
        os.remove(image_path)
        # Update self
        self.setSize(self._id, self.getFolderSize())

    def getImage(self, hash) -> Image:
        image_path = self._image[hash]
        image = Image.open(image_path, mode="r")
        return image

    def getFname(self, hash) -> str:
        return str(self._image[hash].split("/")[-1])

    def isDaemon(self, conn: socket.socket) -> bool:
        return conn in self._recv_conn.values()

    def compareImages(self, image_path1, image_path2):
        image1 = Image.open(image_path1)
        image2 = Image.open(image_path2)
        # Comparison criteria (1 then 2):
        # 1 - bigger image always wins
        # 2 - color always wins
        # The function returns tuple (image_path_to_keep, image_path_to_remove)
        image1_dim = image1.size  # returns a tuple (width, height)
        image2_dim = image2.size

        image1_size = image1_dim[0] * image1_dim[1]  # total pixels
        image2_size = image2_dim[0] * image2_dim[1]

        n_image1_colours = len(image1.getcolors(image1_size))
        n_image2_colours = len(image2.getcolors(image2_size))

        if image1_size > image2_size:
            return (image_path1, image_path2)
        elif image1_size < image2_size:
            return (image_path2, image_path1)
        else:
            if n_image1_colours > n_image2_colours:
                return (image_path1, image_path2)
            elif n_image1_colours < n_image2_colours:
                return (image_path2, image_path1)
            else:
                return (image_path1, image_path2)  # if images have same dimensions and same number of colours, keep the initial photo

    def getFolderSize(self):
        size = 0
        for image_file in os.listdir(self._folder_path):
            if image_file.startswith("."):
                continue
            image_path = os.path.join(self._folder_path, image_file)
            size += os.path.getsize(image_path)
        return size

    def parseFolder(self):
        image_tmp = {}  # { hash: path } - Assign to self._image at the end
        hashes_tmp = set()  # { h1,..,hN } - Assign to self._net_info at the end with self.addHash(...)

        # Index images by hash
        for image_file in os.listdir(self._folder_path):
            
            image_path = os.path.join(self._folder_path, image_file)
            
            if not image_file.endswith(".jpeg") and not image_file.endswith(".jpg") and not image_file.endswith(".png"):
                os.remove(image_path)
                print("[DEBUG PORT=" + str(self._addr[1]) + "] Removed invalid image \"" + image_file + "\"")
                continue # Delete trash

            image = Image.open(image_path, mode="r")
            hash = str(imagehash.average_hash(image)).encode("utf-8")

            if hash in image_tmp.keys():
                similar_image_path = image_tmp[hash]
                comparison_result = self.compareImages(similar_image_path, image_path)
                # Delete duplicate image from folder
                os.remove(comparison_result[1])
                image_tmp[hash] = comparison_result[0]
                print("[DEBUG PORT=" + str(self._addr[1]) + "] Removed duplicate image \"" + comparison_result[1] + "\"")
            elif hash in self.getNetHashes():
                os.remove(image_path)
                print("[DEBUG PORT=" + str(self._addr[1]) + "] Removed duplicate image \"" + image_file + "\"")
            else:
                # Index image
                image_tmp[hash] = image_path
                hashes_tmp.add(hash)

        # Update self
        for hash, image_path in image_tmp.items():
            self._image[hash] = image_path
        for hash in hashes_tmp:
            self.addHash(self._id, hash)
        self.setSize(self._id, self.getFolderSize())

    def crashHandler(self, id):
        """Handles crash of id."""
        # Backup and remove crashed id hash entries on net_info
        hashes_crashed = self.getHashes(id)
        self._net_info.pop(id)
        # Order peer ids by least size, and by smallest id in case of tie
        ids = self.getIds()
        sizes = [self.getSize(id) for id in ids]
        ids_sorted = [id for _, id in sorted(zip(sizes, ids))]
        # Self is the only peer - stop algorithm
        if len(ids_sorted) == 1:
            return
        # Get designated peer and backup peer
        [id_designated, id_backup] = ids_sorted[0:2]
        # Self is not designated peer - stop algorithm
        if self._id != id_designated:
            return
        # Self is designated_peer
        for hash in hashes_crashed:
            # Id of peer storing the image
            id_hash = self.getIdByHash(hash)
            # Image is not in self
            if id_hash != None and id_hash != self._id:
                # Request image from the peer storing it
                self._own_request.add(hash)  # So self know it needs to store the image when he receives it
                request_image_msg = PeerProto.request_image(self._id, hash)
                self._send_mutex.acquire()
                PeerProto.send_msg(self.getSendConn(id_hash), request_image_msg)
                self._send_mutex.release()
            # Image is in self
            elif id_hash != None and id_hash == self._id:
                # Send image to backup peer
                image_msg = PeerProto.image(self._id, hash, self.getImage(hash), self.getFname(hash), store=True)
                self._send_mutex.acquire()
                PeerProto.send_msg(self.getSendConn(id_backup), image_msg)
                self._send_mutex.release()
            else:
                print("[FAIL PORT=" + str(self._addr[1]) + "] Image with hash \"" + str(hash) + "\" was lost!")

    def run(self):
        """Run until done."""

        while True:
            events = self._sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj)
