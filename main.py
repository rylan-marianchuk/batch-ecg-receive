import argparse
from src.receiveECGs import ReceiveECGs
import time

parser = argparse.ArgumentParser(description="Recieve A Batch of ECGs given an iterable entry, and an output to send the waveforms")
parser.add_argument('--url', help="")
parser.add_argument('--puid_map', help="")
parser.add_argument('--xml_dir', help="")
parser.add_argument('--h5_dir', help="")
args = parser.parse_args()
receive = ReceiveECGs(URL=args.url, puid_map=args.puid_map, xml_dir=args.xml_dir, h5_dir=args.h5_dir)
start = time.time()
receive.receive_batch()
print("Time: " + str(time.time() - start))

