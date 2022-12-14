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

    service = "UBER_SERVICE"
    sock.send_multipart(
        [
            b"MDPC02",
            bytes(
                [
                    0x01,
                ]
            ),
            b"mmi.service",
            service.encode(),
        ]
    )
    fullcontent = sock.recv_multipart()
    print(formatted_frames(fullcontent))
    print(f"Service {str(fullcontent[2])} on service '{service}' :  {str(fullcontent[3])}")

    sock.disconnect(addr)
    sock.close()


if __name__ == "__main__":
    main()
