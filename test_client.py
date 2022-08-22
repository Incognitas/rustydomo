import zmq
import time 

def main():
    addr = "tcp://127.0.0.1:5000"
    ctx = zmq.Context()
    sock =ctx.socket(zmq.DEALER)
    sock.connect(addr)
    time.sleep(0.2)

    time.sleep(5)
    sock.disconnect(addr)
    sock.close()


if __name__ == "__main__":
    main()
