"""Wakeups are transient notifications, never authority or a durable cursor."""
import threading
import time
import unittest
import test_receiver as fixtures
from change_signal import ChangeSignal
from receiver import StopEvent


class ChangeSignalTest(unittest.TestCase):
    def test_commit_between_snapshot_and_wait_is_not_lost_for_any_reader(self):
        signal=ChangeSignal();stop=threading.Event();version=signal.version()
        signal.committed()
        start=time.monotonic()
        self.assertEqual(signal.wait(version,1,stop),version+1)
        self.assertLess(time.monotonic()-start,.1)
        reached=threading.Barrier(5);values=[]
        def observe():
            before=signal.version();reached.wait();values.append(signal.wait(before,1,stop))
        readers=[threading.Thread(target=observe) for _ in range(4)]
        for t in readers:t.start()
        reached.wait();signal.committed()
        for t in readers:t.join(2);self.assertFalse(t.is_alive())
        self.assertEqual(values,[2]*4)

    def test_stop_wakes_without_advancing_database_version(self):
        signal=ChangeSignal();stop=StopEvent(signal.wake);values=[]
        reader=threading.Thread(target=lambda:values.append(signal.wait(0,5,stop)))
        reader.start();stop.set();reader.join(1)
        self.assertFalse(reader.is_alive());self.assertEqual(values,[0])

    def test_only_successful_write_transactions_notify_after_close(self):
        case=fixtures.ReceiverTest('runTest');case.setUp();self.addCleanup(case.tearDown)
        archive=case.archive;version=archive.changes.version()
        with archive.db() as db:db.execute('SELECT count(*) FROM audit').fetchone()
        self.assertEqual(archive.changes.version(),version)
        with self.assertRaisesRegex(RuntimeError,'rollback'):
            with archive.db() as db:
                db.execute("INSERT INTO audit(at,action) VALUES(1,'fixture')")
                raise RuntimeError('rollback')
        self.assertEqual(archive.changes.version(),version)
        with archive.db() as db:db.execute("INSERT INTO audit(at,action) VALUES(1,'fixture')")
        self.assertEqual(archive.changes.version(),version+1)
        with archive.db() as db:self.assertEqual(db.execute('SELECT count(*) FROM audit').fetchone()[0],1)
        self.assertEqual(archive.changes.version(),version+1)
