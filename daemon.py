import argparse

from src.daemon import Daemon

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("images_folder", type=str)
    parser.add_argument("own_port", type=int)
    parser.add_argument("peertoconnect_port", type=int, nargs="?", default=None)
    args = parser.parse_args()

    if args.peertoconnect_port == None:
        d = Daemon(args.images_folder, ("localhost", args.own_port))
    else:
        d = Daemon(args.images_folder, ("localhost", args.own_port), ("localhost", args.peertoconnect_port))

    d.run()
