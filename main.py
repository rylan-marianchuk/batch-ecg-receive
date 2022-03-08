import argparse
from receiveECGs import ReceiveECGs
import os

parser = argparse.ArgumentParser(description="Recieve A Batch of ECGs given an iterable entry")
parser.add_argument('--url', help="")
parser.add_argument('--puid_map', help="")
parser.add_argument('--xml_dir', help="")
args = parser.parse_args()
receive = ReceiveECGs(URL=args.url, puid_map=args.puid_map)

receive.receive_batch(os.listdir(args.xml_dir))

