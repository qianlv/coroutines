# encoding=utf-8

import select
import types
import sys
import socket
from Queue import Queue


class Task(object):
    taskid = 0

    def __init__(self, target):
        Task.taskid += 1
        self.tid = Task.taskid
        self.target = target
        self.sendval = None
        self.exc_info = None
        self.stack = []

    def run(self):
        while True:
            try:
                if self.exc_info:
                    self.target.throw(*self.exc_info)
                else:
                    result = self.target.send(self.sendval)
            except StopIteration:
                if not self.stack:
                    raise
                self.sendval = None
                self.target = self.stack.pop()
            except:
                if not self.stack:
                    raise
                self.exc_info = sys.exc_info()
                self.target = self.stack.pop()
            else:
                if isinstance(result, SystemCall):
                    return result
                if isinstance(result, types.GeneratorType):
                    self.stack.append(self.target)
                    self.sendval = None
                    self.target = result
                else:
                    if not self.stack:
                        return
                    self.sendval = result
                    self.target = self.stack.pop()


    def __str__(self):
        return 'Task {} {}'.format(self.tid, str(self.target))

    __repr__ = __str__


class Scheduler(object):
    def __init__(self):
        self.ready = Queue()
        self.taskmap = {}
        self.exit_waiting = {}  # tid: [task0, task1], tid为被等待退出Task的id, 列表为等待的Task
        self.read_waiting = {}
        self.write_waiting = {}

    # I/O wait
    def waitforread(self, task, fd):
        self.read_waiting[fd] = task

    def waitforwrite(self, task, fd):
        self.write_waiting[fd] = task

    def iopoll(self, timeout):
        if self.read_waiting or self.write_waiting:
            rlist, wlist, elist = select.select(self.read_waiting,
                                                self.write_waiting,
                                                [], timeout)
            for fd in rlist:
                self.schedule(self.read_waiting.pop(fd))
            for fd in wlist:
                self.schedule(self.write_waiting.pop(fd))

    def iotask(self):
        while True:
            if self.ready.empty():
                self.iopoll(None)
            else:
                self.iopoll(0)
            yield

    def new(self, target):
        newtask = Task(target)
        self.taskmap[newtask.tid] = newtask
        self.schedule(newtask)
        return newtask.tid

    def schedule(self, task):
        self.ready.put(task)

    def mainloop(self):
        self.new(self.iotask())
        while self.taskmap:
            task = self.ready.get()
            try:
                result = task.run()
                # print task, result
                if isinstance(result, SystemCall):
                    result.task = task
                    result.sched = self
                    result.handle()
                    continue
            except StopIteration:
                print 'exit task {}'.format(str(task))
                self.exit(task)
                continue
            self.schedule(task)

    def exit(self, task):
        print "Task {} terminated".format(task.tid)
        del self.taskmap[task.tid]
        for task in self.exit_waiting.pop(task.tid, []):
            self.schedule(task)

    def waitforexit(self, task, waittid):
        if waittid in self.taskmap:
            self.exit_waiting.setdefault(waittid, []).append(task)
            return True
        else:
            return False


class SystemCall(object):
    def handle(self):
        pass


class GetTid(SystemCall):
    def handle(self):
        self.task.sendval = self.task.tid
        self.sched.schedule(self.task)


class NewTask(SystemCall):
    def __init__(self, target):
        self.target = target

    def handle(self):
        tid = self.sched.new(self.target)
        # print 'New Task', tid, self.task
        self.task.sendval = tid
        self.sched.schedule(self.task)


class KillTask(SystemCall):
    def __init__(self, tid):
        self.tid = tid

    def handle(self):
        task = self.sched.taskmap.get(self.tid, None)
        if task:
            task.target.close()
            self.task.sendall = True
        else:
            self.task.sendall = False
        self.sched.schedule(self.task)


class WaitTask(SystemCall):
    def __init__(self, tid):
        self.tid = tid

    def handle(self):
        result = self.sched.waitforexit(self.task, self.tid)
        self.task.sendval = result
        # if waiting for non-existent task.
        # return immediately without waiting.
        if not result:
            self.sched.schedule(self.task)


class ReadWait(SystemCall):
    def __init__(self, f):
        self.f = f

    def handle(self):
        fd = self.f.fileno()
        self.sched.waitforread(self.task, fd)


class WriteWait(SystemCall):
    def __init__(self, f):
        self.f = f

    def handle(self):
        fd = self.f.fileno()
        self.sched.waitforwrite(self.task, fd)


class SocketCloseError(Exception):
    pass


class Socket(object):
    def __init__(self, sock):
        self.sock = sock
        self._buf = b''
        self._closed = False

    def accept(self):
        """ Accept a new socket connection, Return a Socket Object. """
        if self._closed:
            raise SocketCloseError()
        yield ReadWait(self.sock)
        client, addr = self.sock.accept()
        yield Socket(client), addr

    def send(self, buffer):
        """ Sends data on socket, return the numbers of bytes successfully sent. """
        if self._closed:
            raise SocketCloseError()
        if buffer:
            yield WriteWait(self.sock)
            len = self.sock.send(buffer)
            yield len

    def sendall(self, buffer):
        """ Sends all data on socket."""
        if self._closed:
            raise SocketCloseError()

        if buffer:
            yield WriteWait(self.sock)
            self.sock.sendall(buffer)

    def readline(self, terminator="\n", bufsize=1024):
        if self._close:
            raise SocketCloseError()
        while True:
            if terminated in self._buf:
                line, self._buf = self._buf.split(terminator, 1)
                line += terminator
                yield line
                break
            data =  yield self.recv(bufsize)
            if data:
                self._buf += data
            else:
                line = self._buf
                self._buf = b''
                yield line
                break

    def recv(self, maxbytes):
        if self._closed:
            raise SocketCloseError()
         
        yield ReadWait(self.sock)
        yield self.sock.recv(maxbytes)

    def close(self):
        self._closed = True
        self.sock.close()


class MyQueue(object):


def handle_client(client, addr):
    print 'Connection from {}'.format(addr)
    while True:
        data = yield client.recv(1024)
        if not data:
            client.close()
            break
        yield client.sendall(data)


def server(port):
    print "Server Porting..."
    rawsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rawsock.bind(('', port))
    rawsock.listen(5)
    sock = Socket(rawsock)
    while True:
        client, addr = yield sock.accept()
        yield NewTask(handle_client(client, addr))
        
if __name__ == '__main__':
    # def divzero():
    #     t = 10 / 0
    #     yield t

    # def mid():
    #     yield divzero()

    # def printer():
    #     for i in range(4):
    #         print i
    #         yield
    #     yield mid()

    sched = Scheduler()
    sched.new(server(8000))
    sched.mainloop()
