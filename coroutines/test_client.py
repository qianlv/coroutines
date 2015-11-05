# encooding=utf-8

import socket
import sys
from coroutines import Connection
from coroutines import Scheduler
from coroutines import read



def client(host, name):
    rawsock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    rawsock.connect((host, name))
    sock = Connection(rawsock, (host, name))
    while True:
        data = yield read(sys.stdin)
        print '1', data
        if not data:
            break
        yield sock.sendall(data)
        data = yield sock.recv(1024)
        print '2', data

if __name__ == '__main__': 
    sched = Scheduler()
    sched.new(client('ip6-localhost', 8000))
    sched.mainloop()
