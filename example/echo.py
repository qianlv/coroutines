# encoding=utf-8

import sys
sys.path.append('../')

import coroutines


def handle_client(client):
    print 'Connection from {}'.format(client.addr)
    try:
        while True:
            data = yield client.recv(1024)
            if not data:
                break
            print 'Read from {}: {}'.format(client.addr, data)
            yield client.sendall(data)
    finally:
        print 'Disconnected: {}'.format(client.addr)
        client.close()


def echo_server():
    listener = coroutines.Listener('', 8080)
    try:
        while True:
            client = yield listener.accept()
            yield coroutines.NewTask(handle_client(client))
    except KeyboardInterrupt:
        print
    finally:
        print 'Exiting...'
        listener.close()


if __name__ == '__main__':
    sched = coroutines.Scheduler()
    sche.new(echo_server())
    sche.mainloop()
