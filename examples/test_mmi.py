import zmq
import time
from typing import List
import binascii
import sys 
import argparse

def _setup_connection() -> zmq.Socket:
    addr = "tcp://127.0.0.1:5000"
    ctx = zmq.Context()
    sock: zmq.Socket = ctx.socket(zmq.DEALER)
    sock.connect(addr)
    time.sleep(0.2)
    return sock

def status_callback(args: argparse.Namespace):
    print(f"Status of service '{args.name}'")
    sock = _setup_connection()

    sock.send_multipart(
        [
            b"MDPC02",
            bytes(
                [
                    0x01,
                ]
            ),
            b"mmi.service",
            args.name.encode(),
        ]
    )
    fullcontent = sock.recv_multipart()
    print(formatted_frames(fullcontent))
    print(f"Service {str(fullcontent[2])} on service '{args.name}' :  {str(fullcontent[3])}")

def discovery_callback(args: argparse.Namespace):
    print(f"Service discovery requested")
    sock = _setup_connection()

    sock.send_multipart(
        [
            b"MDPC02",
            bytes(
                [
                    0x01,
                ]
            ),
            b"mmi.discovery"
        ]
    )
    fullcontent = sock.recv_multipart()

def prepareCLI() -> argparse.ArgumentParser :
    parser = argparse.ArgumentParser()

    subparser = parser.add_subparsers(help="Services operations")
    status_parser = subparser.add_parser("status")
    status_parser.add_argument("name")
    status_parser.set_defaults(func=status_callback)

    discovery_parser = subparser.add_parser("discovery")
    discovery_parser.set_defaults(func=discovery_callback)

    return parser



def formatted_frames(frames: List[bytes]) -> str:
    return b"-".join([binascii.hexlify(i) for i in frames]).decode()



def main():
    parser = prepareCLI()
    args = parser.parse_args()
    args.func(args)
    # TODO ; setup connection and operations in appropriate callbacks
    raise NotImplementedError()

    # service = "UBER_SERVICE"

    sock.disconnect(addr)
    sock.close()


if __name__ == "__main__":
    main()
