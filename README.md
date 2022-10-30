# MAJORDOMO broker implementation  in Rust

Simple Rust implementation based on the ZeroMQ RFC for the Majordomo protocol in its version 2.
The broker itself is in Rust.

Sample workers and clients are provided in Python 3 to interact with this broker.

## Specifications used:

- [Draft](https://rfc.zeromq.org/spec/18/) of MajorDomo protocol itself
- [MMI](https://rfc.zeromq.org/spec/8/) protocol also put in place to indicate whether or not a 
  service is registered on the system

## Build 

```console
git clone https://github.com/Incognitas/rustydomo.git 
cd rustydomo
cargo build 
```

## Run broker

```console
RUST_LOG=broker cargo run 
```

## Test worker and client on the broker 

Actions below shall be executed in separate consoles (or panes/windows if you are on tmux or 
equivalent)

### Register sample worker

This worker simply declares itself to handle a service that can be used by clients afterwards

```console 
cd examples 
python3 test_worker.py 
```

### Test existence of the service once registered 

This sample program just uses mmi.service as defined in speficication to check if the requested 
service is present or not

```console
cd examples 
python3 test_mmi.py
```

### Start client to execute a request

This client does not do much except call the requested service with parameters

```console
cd examples
python3 test_client.py 
```

## TODO LIST:

-  [ ] Integrate basic CI pipelines 
-  Provide also libs to easily create and register clients and workers:
   - [ ] In Rust 
   - [ ] In C++
   - [ ] Update examples to use those libs
- [ ] Enrich functionnalities to provide services discovery
- [ ] Add support for authenticity/confidentiality requirements (based on zmq Curve protocol)
- [ ] and probably other things...


