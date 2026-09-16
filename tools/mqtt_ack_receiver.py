#!/usr/bin/env python3
"""Reference business receiver: durable SQLite inbox, exact dedupe, then ACK.

This inbox is the durable handoff point, not an SMS business application.
Integrations must commit their own business writes in the same transaction.
See docs/operations/mqtt-business-ack.md before enabling node ACKs.
"""

import argparse
from datetime import datetime, timezone, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3


class Conflict(ValueError):
    pass


def beijing_now():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


class Inbox:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS inbox (
                node TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL,
                seq INTEGER NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY (node,kind,id,seq));
            CREATE TABLE IF NOT EXISTS task_latest (
                node TEXT NOT NULL, id TEXT NOT NULL, seq INTEGER NOT NULL,
                payload TEXT NOT NULL, PRIMARY KEY (node,id));
            CREATE TABLE IF NOT EXISTS conflicts (
                node TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL,
                seq INTEGER NOT NULL, incoming_sha256 TEXT NOT NULL,
                PRIMARY KEY (node,kind,id,seq,incoming_sha256));
        """)

    def close(self):
        self.db.close()

    def persist(self, node, kind, payload):
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("message must be an object")
        identity = data.get("id")
        if not isinstance(identity, str) or not 1 <= len(identity) <= 128 or any(ord(c) < 32 for c in identity):
            raise ValueError("invalid message id")
        datetime.strptime(data.get("time", ""), "%Y-%m-%d %H:%M:%S")
        seq = data.get("seq") if kind == "task" else 0
        if kind == "task":
            if type(seq) is not int or seq < 1 or data.get("state") not in (
                    "accepted", "running", "succeeded", "rejected", "failed", "expired", "uncertain"):
                raise ValueError("invalid task sequence or state")
            if "result" not in data or "error" not in data:
                raise ValueError("task result and error are required")
        elif kind != "event" or not isinstance(data.get("event"), str) or not data["event"] or not isinstance(data.get("data"), dict):
            raise ValueError("invalid event")
        canonical = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        key = (node, kind, identity, seq)
        # BEGIN IMMEDIATE serializes duplicate checks with inserts, including
        # when multiple receiver processes share this same SQLite database.
        self.db.execute("BEGIN IMMEDIATE")
        conflict = False
        try:
            row = self.db.execute("SELECT payload FROM inbox WHERE node=? AND kind=? AND id=? AND seq=?", key).fetchone()
            if row and row[0] != canonical:
                self.db.execute("INSERT OR IGNORE INTO conflicts VALUES(?,?,?,?,?)", key + (hashlib.sha256(canonical.encode()).hexdigest(),))
                conflict = True
            elif not row:
                self.db.execute("INSERT INTO inbox VALUES(?,?,?,?,?)", key + (canonical,))
                if kind == "task":
                    self.db.execute("""INSERT INTO task_latest VALUES(?,?,?,?)
                        ON CONFLICT(node,id) DO UPDATE SET seq=excluded.seq,payload=excluded.payload
                        WHERE excluded.seq>task_latest.seq""", (node, identity, seq, canonical))
                # Put application-specific SMS/task writes here, before commit.
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        if conflict:
            raise Conflict("same message identity has different content; no ACK")
        item = {"kind": kind, "id": identity}
        if kind == "task":
            item["seq"] = seq
        return item


def receive(inbox, prefix, topic, payload, publish):
    if topic not in (prefix + "/task", prefix + "/event"):
        return False
    item = inbox.persist(prefix.rsplit("/", 1)[1], topic.rsplit("/", 1)[1], payload)
    # A failed/lost publish must not roll back the committed inbox. The node
    # retries the original message, which is deduplicated and acknowledged again.
    publish(prefix + "/ack", json.dumps({"time": beijing_now(), "items": [item]}))
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--node", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password-file", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--ca", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.node):
        parser.error("invalid node")
    if args.ca and not args.tls:
        parser.error("--ca requires --tls")
    os.umask(0o077)
    args.database.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import paho.mqtt.client as mqtt
    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes
    inbox = Inbox(args.database)
    prefix = "cardpool/v1/nodes/" + args.node
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id, protocol=mqtt.MQTTv5)
    client.username_pw_set(args.username, args.password_file.read_text().rstrip("\r\n"))
    if args.tls:
        client.tls_set(ca_certs=str(args.ca) if args.ca else None)

    def connected(c, userdata, flags, reason, properties):
        if reason.is_failure:
            logging.error("broker rejected connection: %s", reason)
            return
        c.subscribe([(prefix + "/task", 1), (prefix + "/event", 1)])

    def subscribed(c, userdata, mid, reasons, properties):
        if any(r.is_failure for r in reasons):
            logging.error("broker rejected business subscriptions: %s", reasons)
        else:
            logging.info("subscribed to task/event; ACK follows durable commit")

    def publish(topic, payload):
        result = client.publish(topic, payload, qos=1, retain=False)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise OSError("ACK could not be queued")
        # Never wait_for_publish inside the network callback: it deadlocks the
        # same loop that must receive PUBACK. Original-message retry is the fallback.

    def message(c, userdata, msg):
        try:
            receive(inbox, prefix, msg.topic, msg.payload, publish)
        except Exception as exc:
            # Do not log payloads, SMS bodies, credentials, or activation codes.
            logging.error("message not acknowledged (%s)", type(exc).__name__)

    client.on_connect, client.on_subscribe, client.on_message = connected, subscribed, message
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = 86400
    client.connect(args.host, args.port, keepalive=30, clean_start=False, properties=props)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
        inbox.close()


if __name__ == "__main__":
    main()
