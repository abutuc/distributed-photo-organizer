import argparse

from src.client import Client

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("peertoconnect_port", type=int)
    args = parser.parse_args()

    c = Client(("localhost", args.peertoconnect_port))

    c.run()
