import time
import argparse

from src.daemon import Daemon

def main(n_peers):
    """ Script to launch several peers. """

    # List with all the peers
    peers = []
    # Initial peer on network
    peer = Daemon("node0", ("localhost", 5000), None)
    peer.start()
    peers.append(peer)
    
    for i in range(n_peers - 1):
        time.sleep(10)
        # Create other peers that connect to initial peer
        peer = Daemon("node" + str(i + 1), ("localhost", 5001 + i), ("localhost", 5000))
        peer.start()
        peers.append(peer)
        
    # Wait for peers to finish
    for peer in peers:
        peer.join()


if __name__ == "__main__":
    # Launch network with 3 peers (default)

    parser = argparse.ArgumentParser()
    parser.add_argument("n_peers", type=int, default=3)
    args = parser.parse_args()

    main(args.n_peers)
