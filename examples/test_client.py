import zmq
import time
from typing import List
import binascii


def formatted_frames(frames: List[bytes]) -> str:
    return b"-".join([binascii.hexlify(i) for i in frames]).decode()

def main():
    addr = "tcp://127.0.0.1:5000"
    ctx = zmq.Context()
    sock: zmq.Socket = ctx.socket(zmq.DEALER)
    sock.connect(addr)
    time.sleep(0.2)

    sock.send_multipart(
        [
            b"MDPC02",
            bytes(
                [
                    0x01,
                ]
            ),
            b"UBER_SERVICE",
            b"UBER_PARAM",
        ]
    )
    fullcontent = sock.recv_multipart()
    print("Received : ", formatted_frames(fullcontent))

    sock.disconnect(addr)
    sock.close()


if __name__ == "__main__":
    main()
