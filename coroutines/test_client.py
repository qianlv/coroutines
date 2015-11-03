# encooding=utf-8

import socket
from coroutines import Socket
from coroutines import Scheduler


def client(host, name):
    rawsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rawsock.connect((host, name))
    sock = Socket(rawsock)
    for i in range(10):
        yield sock.sendall('number{}'.format(i))
        data = yield sock.recv(1024)

if __name__ == '__main__': 
    sched = Scheduler()
    sched.new(client('127.0.0.1', 8000))
    sched.mainloop()
