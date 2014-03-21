# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

class AlreadyComplete(Exception):
    pass

class WaitTimeout(Exception):
    pass

class Closing(Exception):
    pass

class WorkerException(Exception):
    pass

class WorkerDied(Exception):
    pass

class ProducerException(Exception):
    pass
