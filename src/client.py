"""Client implementation."""
import socket

from PIL import Image

from .peer_protocol import PeerProto


class Client:
    def __init__(self, daemon_addr):

        self._id = id
        self._daemon_addr = daemon_addr
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect(daemon_addr)

    def request_list(self) -> list:
        # Request hashes to daemon
        request_list_msg = PeerProto.request_list(0)
        PeerProto.send_msg(self._sock, request_list_msg)
        # Receive hashes from daemon
        list_msg = PeerProto.recv_msg(self._sock)  # Blocks client
        return list_msg.hashes

    def request_image(self, hash: bytes) -> Image:
        # Request image to daemon
        request_image_msg = PeerProto.request_image(0, hash)
        PeerProto.send_msg(self._sock, request_image_msg)
        # Receive image from daemon
        image_msg = PeerProto.recv_msg(self._sock)  # Blocks client
        return image_msg.image

    def run(self):
        """Run until done."""
        photo_menu = dict()
        print("\nWelcome to the distributed photo organizer!\n")

        cmd = ""
        while cmd != "q":
            print("(1) List all images in the system")
            print("(2) Show an image by hashcode")
            print("(3) Show all images in the system")
            print("(q) Quit")
            cmd = input(">> ")

            if cmd == "1":
                # List all images in the system
                hashes = self.request_list()
                for i,hash in enumerate(hashes):
                    photo_menu[i] = hash
                    print("{}: {}".format(i,str(hash)))
            elif cmd == "2":
                if len(photo_menu) == 0:
                    print("Must list all images first!")
                    continue
                # Show an image by hashcode
                hash_indice = int(input("Hashcode index >> "))
                if  len(photo_menu.keys()) > hash_indice >= 0:
                    image = self.request_image(photo_menu[hash_indice])
                    print("Showing image...")
                    image.show()
                else:
                    print("Invalid index!")
            elif cmd == "3":
                if len(photo_menu) == 0:
                    print("Must list all images first!")
                    continue
                # Show all images in the system - works like a stress test
                for hash in photo_menu.values():
                    image = self.request_image(hash)
                    image.show()
