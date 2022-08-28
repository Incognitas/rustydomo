import zmq
import time 

def main():
    addr = "tcp://127.0.0.1:6000"
    ctx = zmq.Context()
    sock: zmq.Socket =ctx.socket(zmq.DEALER)
    sock.connect(addr)
    time.sleep(0.2)

    # always send empty frame at the beginning
    sock.send_multipart([b"", b"MDPW02", bytes([0x01,]), b"UBER_SERVICE"]) 
    input("Type something to end this program")
    sock.disconnect(addr)
    sock.close()



if __name__ == "__main__":
    main()
