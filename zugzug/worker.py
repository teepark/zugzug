# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import fcntl
import os
import struct
import sys
import time

import greenhouse
import mummy

from . import errors, futures

green_os = greenhouse.patched('os')


CMD_JOB = 1
CMD_CLOSE = 2
CMD_STREAM_START = 3
CMD_STREAM_ELEM = 4
CMD_STREAM_END = 5

RC_GOOD = 1
RC_BAD = 2


class Worker(object):
    def __init__(self, func):
        self.func = func
        self.pid = None
        self.readpipe = None
        self.writepipe = None
        self._inflight = None
        self._childpipes = None
        self._pusher = None

    def start(self):
        read_req, write_req = os.pipe()
        read_resp, write_resp = os.pipe()

        pid = os.fork()
        if pid: # in master proc
            self.pid = pid

            _ungreenify(read_resp)
            _ungreenify(write_req)
            self.readpipe = read_resp
            self.writepipe = write_req
            self._childpipes = (read_req, write_resp)

            assert green_os.read(read_resp, 1) == '\x00'

            #TODO: multiplex responses in a single collector in the master
            self._collector = greenhouse.greenlet(self._collect)
            greenhouse.schedule(self._collector)

        else: # in worker proc

            _ungreenify(read_req)
            _ungreenify(write_resp)
            self.readpipe = read_req
            self.writepipe = write_resp

            greenhouse.unpatch()

            while not os.write(write_resp, '\x00'): pass

            while 1:
                cmd, args, kwargs = recv_req(read_req)
                if cmd == CMD_JOB:
                    try:
                        result = self.func(*args, **kwargs)
                    except Exception:
                        trace = traceback.format_exception(*sys.exc_info())
                        send_resp(RC_BAD, ''.join(trace).strip())
                    else:
                        send_resp(write_resp, RC_GOOD, result)
                elif cmd == CMD_CLOSE:
                    sys.exit(0)

    def close(self):
        send_req(self.writepipe, CMD_CLOSE, None, None)
        greenhouse.end(self._collector)

    def join(self):
        green_os.waitpid(self.pid, 0)
        self._close_pipes()

    def submit(self, args, kwargs):
        self._inflight = futures.Future()
        if args and (
                hasattr(args[0], '__iter__') and
                not hasattr(args[0], '__len__')):
            send_req(self.writepipe, CMD_STREAM_START, args[1:], kwargs)
            self._pusher = greenhouse.greenlet(self._push_down(args[0]))
            greenhouse.schedule(self._pusher)
        else:
            send_req(self.writepipe, CMD_JOB, args, kwargs)
        return self._inflight

    def recover(self):
        if self._inflight is not None:
            self._inflight.abort(errors.WorkerDied, errors.WorkerDied())
            self._inflight = None

        greenhouse.end(self._collector)
        self._close_pipes()
        self.start()

    def _collect(self):
        while 1:
            rc, result = recv_resp(self.readpipe)
            if rc == RC_GOOD:
                self._inflight.finish(result)
            elif rc == RC_BAD:
                self._inflight.abort(errors.WorkerException,
                        errors.WorkerException(result))
            self._inflight = None

    def _close_pipes(self):
        os.close(self.readpipe)
        os.close(self.writepipe)
        os.close(self._childpipes[0])
        os.close(self._childpipes[1])

    def _push_down(self, gen):
        failed = False
        try:
            for obj in gen:
                send_req(self.writepipe, CMD_STREAM_ELEM, (obj,), {})
        except Exception:
            failed = True
        finally:
            send_req(self.writepipe, CMD_STREAM_END, (failed,), {})
            self._pusher = None


def _greenify(pipefd):
    flags = fcntl.fcntl(pipefd, fcntl.F_GETFL)
    if not flags & os.O_NONBLOCK:
        fcntl.fcntl(pipefd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

def _ungreenify(pipefd):
    flags = fcntl.fcntl(pipefd, fcntl.F_GETFL)
    if flags & os.O_NONBLOCK:
        fcntl.fcntl(pipefd, fcntl.F_SETFL, flags & ~os.O_NONBLOCK)


def _recv(pipefd, os):
    length = struct.unpack('>I', os.read(pipefd, 4))[0]
    return mummy.loads(os.read(pipefd, length))

def recv_req(pipefd):
    return _recv(pipefd, os)

def recv_resp(pipefd):
    return _recv(pipefd, green_os)


def _send(pipefd, msg, os):
    msg = mummy.dumps(msg)
    msg = struct.pack('>I', len(msg)) + msg
    total = len(msg)
    written = 0
    while written < total:
        written += os.write(pipefd, msg[written:])

def send_req(pipefd, cmd, args, kwargs):
    _send(pipefd, (cmd, args, kwargs), green_os)

def send_resp(pipefd, rc, result):
    _send(pipefd, (rc, result), os)
