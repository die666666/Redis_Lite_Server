# Redis_Lite_Server
A lightweight Redis-compatible in-memory data store built from scratch in Python.



## Features
- RESP protocol support
- Core Redis commands (PING, SET, GET, EXISTS, DEL)
- Atomic counters (INCR, DECR)
- List operations (LPUSH, RPUSH)
- Key expiry (EX, PX, EXAT, PXAT)
- Concurrent client handling
- Disk persistence (SAVE)
- Redis CLI & redis-benchmark compatible

## How to Run 
1) Connect using Redis CLI (via Docker)

docker run -it --rm redis redis-cli -h host.docker.internal -p 6379

2) Benchmark

docker run -it --rm redis redis-benchmark \
  -h host.docker.internal -p 6379 -t SET,GET -n 1000
