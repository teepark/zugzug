# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import collections
import functools

import greenhouse

os = greenhouse.patched("os")
signal = greenhouse.patched("signal")

from . import errors, futures, worker


class Master(object):
    def __init__(self, func, count):
        self._func = func
        self._count = count
        self._workers = [None] * count
        self._available = set()
        self._backlog = collections.deque()
        self._worker_closed = greenhouse.Event()
        self._closed_count = 0
        self._closing = False

    def start(self):
        signal.signal(signal.SIGCHLD, self._sigchld)

        for i in xrange(self._count):
            w = worker.Worker(self._func)
            w.start()
            self._workers[i] = w
            self._available.add(i)

    def close(self):
        self._closing = True
        while self._closed_count < self._count:
            if self._available:
                i = self._available.pop()
                self._workers[i].close()
            else:
                self._worker_closed.wait()
            self._closed_count += 1

        for w in self._workers:
            w.join()

    def submit(self, args, kwargs):
        if self._available:
            return self._submit(self._available.pop(), args, kwargs)
        else:
            fut = futures.Future()
            self._backlog.append((fut, args, kwargs))
            return fut

    def perform(self, args, kwargs, timeout=None):
        return self.submit(args, kwargs).get(timeout)

    def _completion(self, i, *result):
        if self._closing:
            for outfut, _, _ in self._backlog:
                outfut.abort(errors.Closing, errors.Closing())
            self._backlog.clear()

            self._workers[i].close()
            self._worker_closed.set()
            self._worker_closed.clear()

        elif self._backlog:
            outfut, args, kwargs = self._backlog.popleft()
            infut = self._submit(i, args, kwargs)
            infut.on_finish(outfut.finish)
            infut.on_abort(outfut.abort)

        else:
            self._available.add(i)

    def _submit(self, i, args, kwargs):
        fut = self._workers[i].submit(args, kwargs)
        p = functools.partial(self._completion, i)
        fut.on_finish(p)
        fut.on_abort(p)
        return fut

    def _sigchld(self, signum, frame):
        if self._closing:
            return

        pid, status = os.wait()
        if not pid:
            return

        for i, worker in enumerate(self._workers):
            if worker.pid == pid:
                worker.recover()
                break
