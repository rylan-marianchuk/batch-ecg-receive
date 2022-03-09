import argparse
from src.receiveECGs import ReceiveECGs
import time

parser = argparse.ArgumentParser(description="Recieve A Batch of ECGs given an iterable entry")
parser.add_argument('--url', help="")
parser.add_argument('--puid_map', help="")
parser.add_argument('--xml_dir', help="")
args = parser.parse_args()
receive = ReceiveECGs(URL=args.url, puid_map=args.puid_map, iter=args.xml_dir)
start = time.time()
receive.receive_batch()
print("Time: " + str(time.time() - start))

