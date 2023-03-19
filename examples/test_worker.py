import zmq.utils.monitor
import time
import binascii
import signal
import threading
from typing import List, Optional, Dict, Any
import logging
import time

termination_signal = False


def term_signal(signal, _stackframe):
    global termination_signal
    print("Termination requested")
    termination_signal = True


class COMMAND_TYPES:
    READY = 0x01
    REQUEST = 0x02
    PARTIAL = 0x03
    FINAL = 0x04
    HEARTBEAT = 0x05
    DISCONNECT = 0x06


class WorkerContext:
    def __init__(self, name: str, sock: zmq.Socket, mon_sock: zmq.Socket):
        self.name = name
        self.socket = sock
        self.monitor_socket = mon_sock
        self.ready = False
        self.response_header = []
        self.last_heartbeat_received: Optional[float] = None
        self.last_heartbeat_sent: Optional[float] = None

    def gen_frames(
        self, command_type: int, payload: Optional[List[bytes]] = None
    ) -> List[bytes]:
        result_to_come: List[bytes] = []
        result_to_come.append(b"MDPW02")
        result_to_come.append(
            bytes(
                [
                    command_type,
                ]
            )
        )
        result_to_come.extend(self.response_header)
        if payload is not None:
            result_to_come.extend(payload)
        return result_to_come


def formatted_frames(frames: List[bytes]) -> str:
    return b"-".join([binascii.hexlify(i) if len(i) > 0 else b"[]" for i in frames]).decode()


def handle_heartbeat(ctx: WorkerContext, _frames: List[bytes]):
    # logging.debug("Broker heartbeat received")
    if ctx.last_heartbeat_received is None:
        # first init
        logging.debug("First heartbeat set")

    ctx.last_heartbeat_received = time.monotonic()


def handle_request(ctx: WorkerContext, frames: List[bytes]):
    logging.debug("REQUEST received")
    logging.info("Do something really useful here...")
    # or not :) We just answer FINAL with no specific data
    frames_to_send = ctx.gen_frames(COMMAND_TYPES.FINAL, payload=[b"answer"])
    logging.debug("sending : {}".format(formatted_frames(frames_to_send)))
    ctx.socket.send_multipart(frames_to_send)


def default_callback(_: WorkerContext, frames: List[bytes]):
    logging.debug(
        f"Callback not defined for command type value {formatted_frames(frames)}")


def handle_message(ctx: WorkerContext):
    ctx.response_header = []
    frames = ctx.socket.recv_multipart()
    if frames[0] != b"MDPW02":
        logging.error("Invalid command header. Ignoring command")
        logging.debug(formatted_frames(frames))
        raise Exception("Invalid header received")

    callbacks = {
        COMMAND_TYPES.HEARTBEAT: handle_heartbeat,
        COMMAND_TYPES.REQUEST: handle_request,
    }

    idx = 2
    # retrieve envelope stack and save it in response header
    while idx < len(frames):
        ctx.response_header.append(frames[idx])
        if len(frames[idx]) == 0:
            break
        # go to the next frame
        idx += 1

    callbacks.get(frames[1][0], default_callback)(ctx, frames[idx:])


PERIOD = 1  # 1 second period


def check_broker_expiration(ctx: WorkerContext):
    reftime = time.monotonic()
    if ctx.last_heartbeat_received:
        if (ctx.last_heartbeat_received + (4 * PERIOD)) < reftime:
            ctx.ready = False
            logging.error("Broker connection lost")


def mark_ready(ctx: WorkerContext):
    logging.info("Signalling worker as READY")
    ctx.socket.send_multipart(
        [
            b"MDPW02",
            bytes(
                [
                    COMMAND_TYPES.READY,
                ]
            ),
            ctx.name.encode(),
        ]
    )


def send_hearbeat(ctx: WorkerContext):
    reftime = time.monotonic()
    if ctx.last_heartbeat_sent is None or (reftime - ctx.last_heartbeat_sent) > (0.8 * PERIOD):
        ctx.socket.send_multipart(
            [
                b"MDPW02",
                bytes(
                    [
                        COMMAND_TYPES.HEARTBEAT,
                    ]
                ),
            ]
        )
        ctx.last_heartbeat_sent = reftime


def send_disconnect_command(ctx: WorkerContext):
    logging.info("Sending DISCONNECT to broker")
    ctx.socket.send_multipart(
        [
            b"MDPW02",
            bytes(
                [
                    COMMAND_TYPES.DISCONNECT,
                ]
            ),
        ]
    )


def handle_monitor_events(ctx: WorkerContext):
    monitor_msgs: Dict[str, Any] = zmq.utils.monitor.recv_monitor_message(
        ctx.monitor_socket)
    # expected Dict with keys "event", "value", "endpoint"
    if monitor_msgs["event"] == zmq.EVENT_HANDSHAKE_SUCCEEDED:
        logging.info(f"Connected to the broker on {monitor_msgs['endpoint']}")
        mark_ready(ctx)
    elif monitor_msgs["event"] == zmq.EVENT_DISCONNECTED:
        logging.debug(
            f"Network connection with broker lost on {monitor_msgs['endpoint']}")


def main():
    logging.basicConfig(level=logging.DEBUG)
    signal.signal(signal.SIGTERM, term_signal)
    signal.signal(signal.SIGINT, term_signal)
    global termination_signal
    addr = "tcp://127.0.0.1:6000"
    ctx = zmq.Context()
    sock: zmq.Socket = ctx.socket(zmq.DEALER)
    mon_sock: zmq.Socket = sock.get_monitor_socket(
        zmq.EVENT_HANDSHAKE_SUCCEEDED | zmq.EVENT_DISCONNECTED)
    sock.connect(addr)
    time.sleep(0.2)

    # always add empty frame at the beginning
    poller: zmq.Poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    poller.register(mon_sock, zmq.POLLIN)

    ctx = WorkerContext("UBER_SERVICE", sock, mon_sock)

    while not termination_signal:
        socks = dict(poller.poll(200))
        if sock in socks and socks[sock] == zmq.POLLIN:
            handle_message(ctx)
        if mon_sock in socks:
            handle_monitor_events(ctx)
        # send periodic heartbeat
        # print("HEARTBEAT...")
        send_hearbeat(ctx)
        check_broker_expiration(ctx)

    send_disconnect_command(ctx)
    print("Disconnecting socket")
    # proper close by flushing the queue of remaining messages
    sock.setsockopt(zmq.LINGER, 0)
    sock.disconnect(addr)
    print("done")


if __name__ == "__main__":
    main()
