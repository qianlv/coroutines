# encoding=utf-8

import select
import types
import sys
import os
import socket
import errno
from itertools import chain
from Queue import Queue


class Task(object):
    '''
    A Simple Task object.
    '''
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
                # print '----------------------'
                # print self.target, '+', self.sendval, '+', repr(result)
                # print '----------------------'
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
            try:
                rlist, wlist, elist = select.select(self.read_waiting,
                                                    self.write_waiting,
                                                    [], timeout)
            except (TypeError, ValueError):
                self._remove_bad_file_descriptors()
            except (select.error, IOError) as err:
                if err[0] == errno.EINTR:
                    pass
                elif err[0] == errno.EBADF:
                    self._remove_bad_file_descriptors()
                else:
                    raise
            else:
                for fd in rlist:
                    self.schedule(self.read_waiting.pop(fd))
                for fd in wlist:
                    self.schedule(self.write_waiting.pop(fd))

    def _remove_bad_file_descriptors(self):
        for fd in set(self.read_waiting):
            try:
                select.select([fd], [fd], [fd], 0)
            except:
                task = self.read_waiting.pop(fd)
                task.exc_info = sys.exc_info()
                self.schedule(task)

        for fd in set(self.write_waiting):
            try:
                select.select([fd], [fd], [fd], 0)
            except:
                task = self.write_waiting.pop(fd)
                task.exc_info = sys.exc_info()
                self.schedule(task)

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


def _is_file_descriptor(fd):
    return isinstance(fd, (int, long))


class ReadWait(SystemCall):
    def __init__(self, fd, timeout=None):
        self.fd = fd if _is_file_descriptor(fd) else fd.fileno()

    def handle(self):
        self.sched.waitforread(self.task, self.fd)


class WriteWait(SystemCall):
    def __init__(self, fd):
        self.fd = fd if _is_file_descriptor(fd) else fd.fileno()

    def handle(self):
        self.sched.waitforwrite(self.task, self.fd)


class SocketCloseError(Exception):
    pass


class SocketListenError(Exception):
    pass


class Listener(object):
    ''' A socket wrapper object for listening socket.'''
    def __init__(self, host, port, queuesize=5):
        self.host = host or None
        self.port = port
        self._closed = False
        self.sock = self._listen(queuesize)
        if self.sock is None:
            raise SocketListenError()
    
    def _listen(self, queuesize):
        ''' Get socket. '''
        addrinfo = socket.getaddrinfo(
            self.host,
            self.port,
            socket.AF_UNSPEC,
            socket.SOCK_STREAM
        )
        sock = None
        for family, socketype, proto, _, sockaddr in addrinfo:
            try:
                sock = socket.socket(family, socketype, proto)
                if hasattr(socket, 'AF_INET6') and family == socket.AF_INET6:
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            except socket.error:
                continue
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(sockaddr)
            except socket.error as err:
                print "Bind Error {} {}".format(err.args[0], err.args[1])
                sock.close()
                sock = None
            if sock:
                break
        if sock:
            sock.listen(queuesize)
        return sock
    
    def accept(self):
        """ Accept a new socket connection, Return a Connection Object. """
        if self._closed:
            raise SocketCloseError()
        yield ReadWait(self.sock)
        client, addr = self.sock.accept()
        yield Connection(client, addr)

    def close(self):
        """ Immediately close the listening socket. """
        self._closed = True
        self.sock.close()


class Connection(object):
    ''' A socket wrapper object for connected socket.'''

    def __init__(self, sock, addr=None):
        self.sock = sock
        self.addr = addr
        self._buf = b''
        self._closed = False

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
        ''' Read a line (delimited by terminator) from socket. '''
        if self._closed:
            raise SocketCloseError()
        while True:
            if terminator in self._buf:
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
        ''' Read data from socket. '''
        if self._closed:
            raise SocketCloseError()
         
        yield ReadWait(self.sock)
        yield self.sock.recv(maxbytes)

    def close(self):
        """ Immediately close the listening socket. """
        self._closed = True
        self.sock.close()

def read(fd, bufsize=None, timeout=None):
    ''' Read data from fd. If bufsize is None, read all data until eof.'''
    if bufsize is None:
        buf = []
        while True:
            data = yield read(fd, 1024, timeout)
            print repr(data)
            if not data:
                break
            buf.append(data)
        yield ''.join(buf)
    else:
        yield ReadWait(fd, timeout=timeout)
        yield fd.read(bufsize)


def main():
    f = open('test.txt')
    data = yield read(102323, 10)
    print repr(data)


class MyQueue(object):
    pass

def handle_client(client):
    print 'Connection from {}'.format(client.addr)
    while True:
        data = yield client.readline()
        print data,
        if not data:
            client.close()
            break
        yield client.sendall(data)


def server(port):
    print "Server Porting..."
    listener = Listener('', port, 5)
    while True:
        client = yield listener.accept()
        yield NewTask(handle_client(client))


if __name__ == '__main__':
    def divzero():
        t = 10 / 0
        print '++++++'
        yield t

    def mid():
        yield divzero()

    def printer():
        for i in range(4):
            print i
            yield
        yield mid()
    sched = Scheduler()
    sched.new(server(8000))
    # sched.new(main())
    # sched.new(printer())
    sched.mainloop()
