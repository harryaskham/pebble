"""Process-local commit wakeups; durable cursors remain in SQLite, not here."""
import threading


class ChangeSignal:
    def __init__(self):
        self._condition = threading.Condition()
        self._version = 0

    def version(self):
        with self._condition:
            return self._version

    def committed(self):
        with self._condition:
            self._version += 1
            self._condition.notify_all()

    def wake(self):
        """Wake stop predicates without pretending that a DB commit happened."""
        with self._condition:
            self._condition.notify_all()

    def wait(self, version, timeout, stop):
        with self._condition:
            self._condition.wait_for(lambda: self._version != version or stop.is_set(), timeout)
            return self._version


def for_archive(archive):
    return getattr(archive, 'legacy', archive).changes
