# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import time
import weakref

import greenhouse

from . import errors


class Future(object):
    'A stand-in object for some value that may not have yet arrived'

    def __init__(self):
        self._done = greenhouse.Event()
        self._waits = set()
        self._children = []
        self._value = None
        self._failure = None
        self._cbacks = []
        self._errbacks = []

    @property
    def complete(self):
        'Whether or not this future has completed'
        return self._done.is_set()

    @property
    def value(self):
        '''The final value, if it has arrived

        :raises: AttributeError, if not yet complete
        :raises: an exception of the Future was :meth:`abort`\ed
        '''
        if not self._done.is_set():
            raise AttributeError("value")
        if self._failure:
            raise self._failure[0], self._failure[1], self._failure[2]
        return self._value

    def finish(self, value):
        '''Give the future it's value and trigger any success callbacks

        :param value: the new value for the future
        :raises:
            :class:`AlreadyComplete <zugzug.errors.AlreadyComplete>` if
            already complete
        '''
        if self._done.is_set():
            raise errors.AlreadyComplete()

        self._value = value

        for cb in self._cbacks:
            greenhouse.schedule(cb, args=(value,))
        self._cbacks = None

        for wait in list(self._waits):
            wait.finish(self)
        self._waits = None

        for child in self._children:
            child = child()
            if child is None:
                continue
            child._incoming(self)
        self._children = None

        self._done.set()

    def abort(self, klass, exc, tb=None):
        '''Finish this future in an error state

        Takes a standard exception triple as arguments (like those returned by
        ``sys.exc_info``).

        Any :class:`Dependent` that are children of this one will also be
        aborted.

        :param class klass: the class of the exception
        :param Exception exc: the exception instance itself
        :param traceback tb: the traceback associated with the exception

        :raises:
            :class:`AlreadyComplete <zugzug.errors.AlreadyComplete>` if
            already complete
        '''
        if self._done.is_set():
            raise errors.AlreadyComplete()

        self._failure = (klass, exc, tb)

        for eb in self._errbacks:
            greenhouse.schedule(eb, args=(klass, exc, tb))
        self._errbacks = None

        for wait in list(self._waits):
            wait.finish(self)
        self._waits = None

        for child in self._children:
            child = child()
            if child is None:
                continue
            child.abort(klass, exc, tb)
        self._children = None

        self._done.set()

    def on_finish(self, func):
        '''Assign a callback function to be run when successfully complete

        :param function func:
            A callback to run when complete. It will be given one argument (the
            value that has arrived), and its return value is ignored.
        '''
        if self._done.is_set():
            if self._failure is None:
                backend.schedule(func, args=(self._value,))
        else:
            self._cbacks.append(func)

    def on_abort(self, func):
        '''Assign a callback function to be run when :meth:`abort`\ed

        :param function func:
            A callback to run if/when aborted. It will be given three args:

                - ``klass``: the exception class
                - ``exc``: the exception instance
                - ``tb``: the traceback object associated with the exception
        '''
        if self._done.is_set():
            if self._failure is not None:
                backend.schedule(func, args=self._failure)
        else:
            self._errbacks.append(func)

    def wait(self, timeout=None):
        '''Block the current coroutine until complete

        :param timeout: maximum time to block in seconds
        :type timeout: int, float or None

        :raises:
            :class:`WaitTimeout <zugzug.errors.WaitTimeout>` if ``timeout``
            expires before completion
        '''
        if self._done.wait(timeout):
            raise errors.WaitTimeout()

    def after(self, func=None):
        '''Create a new Future whose completion depends on this one

        The new future will have a function that it calls once all its parents
        have completed, the return value of which will be its final value.
        There is a special case in which the dependent future's callback
        returns a future or list of futures. In those cases, waiting on the
        dependent will also wait for all those futures, and the result (or list
        of results) of those future(s) will be this future's final value.

        :param function func:
            The function to determine the value of the dependent future. It
            will be given a single argument, the result of this future.

        :returns:
            a :class:`Dependent`, which is a :class:`Future` subclass and has
            all its capabilities.
        '''
        return after([self], func)

    def get(self, timeout=None):
        '''Get and return the Future's value, blocking if necessary

        :param timeout: maximum time to block in seconds
        :type timeout: int, float or None

        :returns: the Future's :attr:`value <zugzug.futures.Future.value>`

        :raises:
            :class:`WaitTimeout <zugzug.errors.WaitTimeout>` if ``timeout``
            expires before completion
        '''
        self.wait(timeout)
        return self.value


class Dependent(Future):
    '''A future with a function to run after termination of its parent futures

    Instances of this class needn't be created directly; they are returned by
    :meth:`Future.after` and :func:`after`.
    '''

    def __init__(self, parents, func):
        super(Dependent, self).__init__()
        self._parents = parents
        self._func = func
        self._parent_results = [None] * len(parents)
        self._parent_indexes = {v: i for i, v in enumerate(parents)}
        self._transfer = None

    def _incoming(self, parent):
        index = self._parent_indexes.pop(parent)
        self._parents[index] = None
        self._parent_results[index] = parent._value

        if all(p is None for p in self._parents):
            greenhouse.schedule(self._run_func)

    def _run_func(self):
        try:
            value = self._func(*self._parent_results)
        except Exception:
            self.abort(*sys.exc_info())
        else:
            if (isinstance(value, list)
                    and all(isinstance(v, Future) for v in value)
                    and value):
                value = after(value, lambda *l: l)

            if isinstance(value, Future):
                value.on_finish(self.finish)
                value.on_abort(self.abort)
                self._transfer = value
            else:
                self.finish(value)

    def finish(self, value):
        super(Dependent, self).finish(value)
        self._transfer = None

    def abort(self, klass, exc, tb=None):
        super(Dependent, self).abort(klass, exc, tb)
        self._transfer = None


def after(parents, func=None):
    '''Create a new Future whose completion depends on a list of futures

    The new future will have a function that it calls once all its parents have
    completed, the return value of which will be its final value. There is a
    special case in which the dependent future's callback returns a future or
    list of futures. In those cases, waiting on the dependent will also wait
    for all those futures, and the results of those future(s) will then be the
    final value (or list of final values).

    :param parents:
        A list of futures, all of which must be complete before the dependent's
        function runs.
    :type parents: list

    :param function func:
        The function to determine the value of the dependent future. It will
        take as many arguments as it has parents, and they will be the results
        of those futures.

    :returns:
        a :class:`Dependent`, which is a subclass of :class:`Future` and has
        all its capabilities.
    '''
    if func is None:
        return lambda f: after(parents, f)

    dep = Dependent(parents, func)
    for parent in parents:
        if parent.complete:
            dep._incoming(parent)
        else:
            parent._children.append(weakref.ref(dep))
    return dep


def wait_any(futures, timeout=None):
    '''Wait for the completion of any (the first) one of multiple futures

    :param list futures: A list of :class:`Future`\s
    :param timeout:
        The maximum time to wait. With ``None``, will block indefinitely.
    :type timeout: int, float or None

    :returns:
        One of the futures from the provided list -- the first one to become
        complete (or any of the ones that were already complete).

    :raises WaitTimeout: if a timeout is provided and expires
    '''
    for fut in futures:
        if fut.complete:
            return fut

    wait = _Wait(futures)

    for fut in futures:
        fut._waits.add(wait)

    if wait.done.wait(timeout):
        raise errors.WaitTimeout()

    return wait.completed_future


def wait_all(futures, timeout=None):
    '''Wait for the copmletion of all futures in a list

    :param list future: a list of :class:`Future`\s
    :param timeout:
        the maximum time to wait. With ``None``, can block indefinitely.
    :type timeout: int, float or None

    :raises WaitTimeout: if a timeout is provided and expires
    '''
    if timeout is None:
        for fut in futures:
            fut.wait()
    else:
        deadline = time.time() + timeout
        for fut in futures:
            fut.wait(deadline - time.time())


class _Wait(object):
    def __init__(self, futures):
        self.futures = set(futures)
        self.done = greenhouse.Event()
        self.completed_future = None
        self.finished = False

    def finish(self, fut):
        if self.finished:
            return
        self.finished = True

        self.completed_future = fut

        for future in self.futures:
            future._waits.remove(self)

        self.done.set()
