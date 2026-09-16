import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from mqtt_ack_receiver import Inbox, Conflict, receive


class InboxTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "inbox.db"
        self.inbox = Inbox(self.path)
        self.prefix = "cardpool/v1/nodes/test-node"
        self.acks = []

    def tearDown(self):
        self.inbox.close()
        self.tmp.cleanup()

    def task(self, seq):
        return json.dumps({"id": "task-1", "seq": seq, "time": "2026-09-16 10:00:00",
                           "state": "succeeded" if seq == 3 else "running",
                           "result": None, "error": None}).encode()

    def event(self):
        return b'{"id":"event-1","event":"sms.received","time":"2026-09-16 10:00:00","data":{"text":"example"}}'

    def receive(self, kind, payload, publish=None):
        return receive(self.inbox, self.prefix, self.prefix + "/" + kind, payload,
                       publish or (lambda topic, ack: self.acks.append((topic, json.loads(ack)))))

    def test_ack_happens_after_commit_and_duplicate_is_acked_again(self):
        def published(topic, ack):
            with sqlite3.connect(self.path) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0], 1)
            self.acks.append(json.loads(ack))
        self.receive("event", self.event(), published)
        self.receive("event", self.event(), published)
        self.assertEqual(len(self.acks), 2)
        self.assertEqual(self.acks[0]["items"], [{"kind": "event", "id": "event-1"}])

    def test_lost_ack_and_receiver_restart_do_not_duplicate_business_record(self):
        def failed(topic, ack):
            raise OSError("ACK connection lost")
        with self.assertRaises(OSError):
            self.receive("event", self.event(), failed)
        self.inbox.close()
        self.inbox = Inbox(self.path)
        self.receive("event", self.event())
        self.assertEqual(self.inbox.db.execute("SELECT COUNT(*) FROM inbox").fetchone()[0], 1)
        self.assertEqual(len(self.acks), 1)

    def test_task_sequences_are_independent_and_late_state_does_not_roll_back(self):
        self.receive("task", self.task(3))
        self.receive("task", self.task(2))
        self.assertEqual([a[1]["items"][0]["seq"] for a in self.acks], [3, 2])
        self.assertEqual(self.inbox.db.execute("SELECT seq FROM task_latest").fetchone()[0], 3)
        self.assertEqual(self.inbox.db.execute("SELECT COUNT(*) FROM inbox").fetchone()[0], 2)

    def test_same_identity_different_payload_is_quarantined_without_ack(self):
        self.receive("event", self.event())
        with self.assertRaises(Conflict):
            self.receive("event", self.event().replace(b"example", b"changed"))
        self.assertEqual(len(self.acks), 1)
        self.assertEqual(self.inbox.db.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0], 1)
        self.assertIn("example", self.inbox.db.execute("SELECT payload FROM inbox").fetchone()[0])

    def test_transaction_failure_never_acknowledges(self):
        self.inbox.db.execute("CREATE TRIGGER fail_write BEFORE INSERT ON inbox BEGIN SELECT RAISE(ABORT, 'disk write failed'); END")
        with self.assertRaises(sqlite3.Error):
            self.receive("event", self.event())
        self.assertEqual(self.acks, [])

    def test_other_topics_and_malformed_messages_never_acknowledge(self):
        for suffix in ("presence", "heartbeat", "ack", "command"):
            self.assertFalse(self.receive(suffix, self.event()))
        self.assertFalse(receive(self.inbox, self.prefix, "cardpool/v1/nodes/other/event", self.event(), lambda *x: self.fail("wrong node ACK")))
        for payload in (b'{}', b'{"id":"x","seq":true}', b'{"id":"x","seq":0}', b'not json'):
            with self.assertRaises(ValueError):
                self.receive("task", payload)
        self.assertEqual(self.acks, [])


if __name__ == "__main__":
    unittest.main()
