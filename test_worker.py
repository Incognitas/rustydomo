import zmq
import time 
import binascii
import signal

termination_signal = False

def term_signal(signal, _stackframe):
    global termination_signal
    print("Termination requested")
    termination_signal = True


def main():
    signal.signal(signal.SIGTERM, term_signal)
    signal.signal(signal.SIGINT, term_signal)
    global termination_signal
    addr = "tcp://127.0.0.1:6000"
    ctx = zmq.Context()
    sock: zmq.Socket = ctx.socket(zmq.DEALER)
    sock.connect(addr)
    time.sleep(0.2)

    # always send empty frame at the beginning
    sock.send_multipart([b"", b"MDPW02", bytes([0x01,]), b"UBER_SERVICE"]) 

    poller:zmq.Poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    ended = False
    while not termination_signal:
        socks = dict(poller.poll(900))
        if sock in socks and socks[sock] == zmq.POLLIN :
            message = sock.recv_multipart()
            for entry in message:
                print("received : ", "-".join([binascii.hexlify(i) for i in message]))
        # send periodic heartbeat
        print("HEARTBEAT...")
        sock.send_multipart([b"", b"MDPW02", bytes([0x05,])])

    sock.disconnect(addr)
    sock.close()



if __name__ == "__main__":
    main()
