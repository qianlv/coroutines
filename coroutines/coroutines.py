# encoding=utf-8

"""

多任务协调和异步I/O通过使用生成器(generators).

Scheduler调度多个任务, 当任务(Task)执行阻塞操作时，如I/O操作, 
从某个队列中获取数据, Scheduler将暂时挂起这个Task,并且当阻塞
操作完成时，Task任务将被重新启动.
使用select或多线程也能实现并行任务.

无关任务并行的例子:
    
    >>> def printer(msg):
    ...     while True:
    ...         print msg
    ...         yiled
    >>> sched = Scheduler()
    >>> sched.new(printer('first'))
    >>> sched.new(printer('second'))
    >>> sched.mainloop()
    first
    second
    first
    second
    ... 

一个简单处理多个客户端连接的服务器.

    >>> def handle_client(client):
    ...     print 'Connection from {}'.format(client.addr)
    ...     while True:
    ...         data = yield client.readline()
    ...         print data,
    ...         if not data:
    ...             client.close()
    ...             break
    ...         yield client.sendall(data)

    >>> def server(port):
    ...     print "Server Porting..."
    ...     listener = Listener('', port, 5)
    ...     while True:
    ...         client = yield listener.accept()
    ...         yield NewTask(handle_client(client))

    >>> sched = Scheduler()
    >>> sched.new(server(8000))
    >>> sched.mainloop()

Task中yield表达式的子函数中也可以使用yield表达式.
在子函数运行到完成或异常抛出. 子函数结果(output)将
传递到父函数, 异常将层层往上传递.

    >>> def one():
    ...    yield
    ...
    >>> def two():
    ...     yield
    ...     raise StopIteration(2)
    ...
    >>> def three():
    ...     yield
    ...     raise StopIteration((1, 2))
    ...
    >>> def raise_exception():
    ...     yield
    ...     raise RuntimeError('run error')
    ...
    >>> def all():
    ...     print (yield one())
    ...     print (yield two())
    ...     print (yield three())
    ...     try:
    ...         yield raise_exception()
    ...     except Exception as err:
    ...         print err
    ...
    >>> sched = Scheduler()
    >>> sched.new(all())
    >>> sched.mainloop()
    None
    2
    (1, 2)
    run error

"""

import select
import types
import sys
import socket
import time
import errno
import heapq
from collections import defaultdict
from functools import partial
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
                    exc_info, self.exc_info = self.exc_info, None
                    result = self.target.throw(*exc_info)
                else:
                    result = self.target.send(self.sendval)
            except StopIteration as err:
                # 正常结束
                if not self.stack:
                    raise
                # 这里是否需要返回StopIteration的参数?
                if not err.args:
                    self.sendval = result
                elif len(err.args) == 1:
                    self.sendval = err.args[0]
                else:
                    self.sendval = err
                self.target = self.stack.pop()
            except:
                # 异常捕获，异常一层层向上抛
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

    def __str__(self):
        return 'Task {} {}'.format(self.tid, str(self.target))

    __repr__ = __str__


class Scheduler(object):
    '''
    多任务调度管理器.
    '''
    def __init__(self):
        self.ready = Queue()
        self.taskmap = {}
        self.exit_waiting = {}  # tid: [task0, task1], tid为被等待退出Task的id, 列表为等待的Task
        self._read_waiting = {}
        self._write_waiting = {}
        self._timeouts = [] 

    # I/O wait
    def waitforread(self, task, fd):
        self._read_waiting[fd] = task

    def waitforwrite(self, task, fd):
        self._write_waiting[fd] = task

    def has_runable(self):
        '''
        Return True if there are runable tasks in ready queue, else return False.
        '''
        return not self.ready.empty()
    
    def has_io_waits(self):
        '''
        Return True if there are tasks waiting for I/O, else return False.
        '''
        return bool(self._read_waiting or self._write_waiting)
    
    def has_timouts(self):
        '''
        Return True if there are tasks with pending timouts, else return False.
        '''
        return bool(self._timeouts)

    def iopoll(self, timeout):
        '''
        检查是否有fd可以进行I/O, 并且等待此fd的Task重新加入到任务队列(ready),
        如果此fd有timeout限制, 那么同时那fd移除timeout列表.
        '''
        if self.has_io_waits():
            try:
                rlist, wlist, elist = select.select(self._read_waiting,
                                                    self._write_waiting,
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
                map(self.remove_read_waiting, rlist)
                map(self._remove_timeout, [fd for fd in rlist if fd.expires()])
                map(self.remove_write_waiting, wlist)
                map(self._remove_timeout, [fd for fd in wlist if fd.expires()])
    
    def _remove_bad_file_descriptors(self):
        for fd in set(self._read_waiting):
            try:
                select.select([fd], [fd], [fd], 0)
            except:
                self.remove_read_waiting(fd, sys.exc_info())

        for fd in set(self._write_waiting):
            try:
                select.select([fd], [fd], [fd], 0)
            except:
                self.remove_write_waiting(fd, sys.exc_info())

    def remove_write_waiting(self, fd, exc_info=None):
        task = self._write_waiting.pop(fd)
        if exc_info:
            task.exc_info = exc_info
        elif isinstance(fd, FDAction):
            task.sendval = fd._eval()
        self.schedule(task)

    def remove_read_waiting(self, fd, exc_info=None):
        task = self._read_waiting.pop(fd)
        if exc_info:
            task.exc_info = exc_info
        elif isinstance(fd, FDAction):
            task.sendval = fd._eval()
        self.schedule(task)

    def io_and_timeout_task(self):
        '''
        I/O 任务处理, 任务超时处理。
        '''
        while True:
            self.iopoll(self._fix_timeout(None))
            self.handle_timeout(self._fix_timeout(None))
            yield

    def _fix_timeout(self, timeout):
        if not self.ready.empty():
            timout = 0.0
        elif self.has_timouts(): 
            expiration_timeout = max(0.0,  self._timeouts[0][0] - time.time())
            if timeout is None or timeout > expiration_timeout:
                timeout = expiration_timeout
        return timeout

    def _add_timeout(self, result):
        heapq.heappush(self._timeouts, (result.expiration, result))

    def _remove_timeout(self, result):
        self._timeouts.remove((result.expiration, result))
        heapq.heapify(self._timeouts)

    def handle_timeout(self, timeout): 
        '''
        处理超时的任务, 并抛出Timeout异常.
        '''
        if not self.has_runable() and timeout > 0.0:
            time.sleep(timeout)

        current_time = time.time()
        while self._timeouts and self._timeouts[0][0] <= current_time:
            result = heapq.heappop(self._timeouts)[1]
            if isinstance(result, Sleep): 
                self.schedule(result.task)        
            else:
                result.handle_expiration()

    def new(self, target):
        newtask = Task(target)
        self.taskmap[newtask.tid] = newtask
        self.schedule(newtask)
        return newtask.tid

    def schedule(self, task):
        self.ready.put(task)

    def mainloop(self):
        '''
        主循环
        '''
        self.new(self.io_and_timeout_task())
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
    pass


class Condition(SystemCall):
    def __init__(self, timeout=None):
        '''
        如果timeout是None, task将被永远的挂起直达条件被满足.
        否则, 如果在timeout时间范围内条件未满足则Timeout异常
        将被抛出.
        '''
        self.expiration = None
        if timeout is not None:
            self.expiration = time.time() + float(timeout)
            print repr(self.expiration)
    
    def expires(self):
        return (self.expiration is not None)

    def handle(self):
        raise NotImplemented


class GetTid(SystemCall):
    '''
    获取Task Id 
    '''
    def handle(self):
        self.task.sendval = self.task.tid
        self.sched.schedule(self.task)


class NewTask(SystemCall):
    '''
    创建一个新Task.
    '''
    def __init__(self, target):
        self.target = target

    def handle(self):
        tid = self.sched.new(self.target)
        self.task.sendval = tid
        self.sched.schedule(self.task)


class KillTask(SystemCall):
    '''
    杀死指定tid的Task.
    '''
    def __init__(self, tid, timeout=None):
        self.tid = tid

    def handle(self):
        task = self.sched.taskmap.get(self.tid, None)
        if task:
            task.target.close()
            self.task.sendall = True
        else:
            self.task.sendall = False
        self.sched.schedule(self.task)


class WaitTask(Condition):
    '''
    如果Task任务yield这个类的实例将阻塞到指定的tid的Task完成.
    '''
    def __init__(self, tid, timeout=None):
        super(NewTask, self).__init__(timeout=timeout)
        self.tid = tid

    def handle(self):
        result = self.sched.waitforexit(self.task, self.tid)
        self.task.sendval = result
        # if waiting for non-existent task.
        # return immediately without waiting.
        if not result:
            self.sched.schedule(self.task)
        if result and self.expires():
            self.sched._add_timeout(self)


def _is_file_descriptor(fd):
    return isinstance(fd, (int, long))


class Timeout(Exception):
    pass


class FDReady(Condition):
    '''

    任务(Task) yield 这个类的实例将被挂起直到fd变为准备好进行I/O操作.

    '''

    def __init__(self, fd, read=False, write=False, exc=False, timeout=None):
        '''

        当fd可读或超时时，任务将重新可运行.
        fd可以是任意可以被select.select接受的对象.
        由于fd引起的异常，将被重新抛出在Task中.
        
        如果timeout不是None, 那么在timeout时间范围内,
        fd还是不可以进行I/O操作, Timeout异常将被抛出.

        '''

        super(FDReady, self).__init__(timeout)

        if not (read or write or exc):
            raise ValueError("'read', 'write' and 'exc' cannot all be False.")

        self.fd = fd if _is_file_descriptor(fd) else fd.fileno()
        self.read = read
        self.write = write
        self.exc = exc

    def fileno(self):
        return self.fd

    def _handle_read(self):
        self.sched.waitforread(self.task, self)
        if self.expires():
            self.sched._add_timeout(self)

    def _handle_write(self):
        self.sched.waitforwrite(self.task, self)
        if self.expires():
            self.sched._add_timeout(self)

    def handle(self):
        if self.read:
            self._handle_read()
        elif self.write:
            self._handle_write()

    def handle_expiration(self):
        if self.read:
            self.sched.remove_read_waiting(self, (Timeout, ))
        elif self.write:
            self.sched.remove_write_waiting(self, (Timeout, ))
            

def readwait(fd, timeout=None):
    '''

    如果Task任务yield这个类的实例将阻塞到指定的文件描述符(fd)可读.

        >>> try:
        ...     yield readwait(fd, timeout=5)
        ... except Timeout:
        ...     print 'Timeout'
        ... yield fd.read(100)

    '''

    return FDReady(fd, read=True, timeout=timeout)


def writewait(fd, timeout):
    '''
    如果Task任务yield这个类的实例将阻塞到指定的文件描述符(fd)可写.
        
        >>> try:
        ...     yield writewait(fd, timeout=5)
        ... except Timeout:
        ...     print 'Timeout'
        ... yield fd.write(data)
            
    '''

    return FDReady(fd, write=True, timeout=timeout)


class FDAction(FDReady):
    def __init__(self, fd, func, args=(), kwargs={}, read=False, write=False, exc=False):
        timeout = kwargs.pop('timeout', None)
        super(FDAction, self).__init__(fd, read, write, exc, timeout)
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def handle(self):
        super(FDAction, self).handle()

    def _eval(self):
        return self.func(*(self.args), **(self.kwargs))


def read(fd, *args, **kwargs):
    func = partial(os.read, fd) if _is_file_descriptor(fd) else fd.read
    return FDAction(fd, func, args, kwargs, read=True)


def readline(fd, *args, **kwargs):
    func = partial(os.readline, fd) if _is_file_descriptor(fd) else fd.readline
    return FDAction(fd, func, args, kwargs, read=True)


def write(fd, *args, **kwargs):
    func = partial(os.write, fd) if _is_file_descriptor(fd) else fd.write
    return FDAction(fd, func, args, kwargs, write=True)


class Sleep(Condition):
    '''

    Task等待指定时间后，重新运行.
      
      yield Sleep(10)
      do_something()

    '''
    def __init__(self, seconds):
        seconds = float(seconds)
        if seconds < 0.0:
            raise ValueError("'seconds' argument must be greater than 0")
        super(Sleep, self).__init__(timeout=seconds)
    
    def handle(self):
        self.sched._add_timeout(self)


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
        yield readwait(self.sock)
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
            yield writewait(self.sock)
            len = self.sock.send(buffer)
            yield len

    def sendall(self, buffer):
        """ Sends all data on socket."""
        if self._closed:
            raise SocketCloseError()

        if buffer:
            yield writewait(self.sock)
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
            data = yield self.recv(bufsize)
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

        yield readwait(self.sock)
        yield self.sock.recv(maxbytes)

    def close(self):
        """ Immediately close the listening socket. """
        self._closed = True
        self.sock.close()


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


def main():
    current = time.time()
    yield Sleep(10)
    print 'main sleep', time.time() - current
    try:
        data = yield read(sys.stdin, 10, timeout=1)
    except:
        print 'pass'


def main2():
    current = time.time()
    yield Sleep(9.2)
    print 'main sleep', time.time() - current
    try:
        data = yield read(sys.stdin, 100, timeout=2)
    except:
        print 'pass2'

if __name__ == '__main__':
    def divzero():
        t = 10 / 0
        yield t

    def mid():
        try:
            yield divzero()
        except:
            pass

    def printer():
        for i in range(4):
            print i
            yield
        yield mid()
        print 'what'
    sched = Scheduler()
    sched.new(server(8000))
    # sched.new(main())
    # sched.new(main2())
    # sched.new(printer())
    sched.mainloop()
