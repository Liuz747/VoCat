# MQTT business acknowledgement

Business ACK is already part of the native node protocol. Enable
`business_ack_enabled` on the node only when the business receiver has durable
storage and permission to publish to that node's `/ack` topic. MQTT PUBACK alone
means transport delivery, not a committed business record.

## Receiver contract

For prefix `cardpool/v1/nodes/{node}`:

- Subscribe to `/task` and `/event` at QoS 1. Publish `/ack` at QoS 1, retain false.
- Use an independent, stable client ID and authorized business credentials.
  Never share the node's client ID. Restrict ACLs to the required node prefix.
- In one database transaction, deduplicate and save the message plus its business
  records. The unique key is `(node,kind,id,seq)`; events have no protocol `seq`.
- Only after committing, ACK the exact message. A task ACK is not cumulative:
  acknowledging seq 3 does not acknowledge seq 1 or 2. Save late task updates but
  do not let them replace a newer displayed state.
- If the same key and content arrives again, do not repeat business effects;
  ACK it again. If the key is reused with different content, record a conflict
  and do not ACK or overwrite the original record.
- A receiver crash after commit but before ACK is safe: the node resends the
  same payload, the receiver deduplicates it, and sends another ACK.
- Do not ACK presence, heartbeat, or ACK messages. Do not acknowledge a message
  merely because it was received by a callback or enqueued in memory.

After saving a task update and an event, publish this shape to `/ack`. Generate
the current Beijing time (`UTC+08:00`, without `T` or `Z`):

```json
{
  "time": "2026-09-16 18:00:00",
  "items": [
    {"kind": "task", "id": "example-command", "seq": 3},
    {"kind": "event", "id": "example-event"}
  ]
}
```

One ACK can include 1–100 items. IDs and sequence numbers must come from actual
committed messages. An unknown identity does not clear another pending record.

## Reference receiver

`tools/mqtt_ack_receiver.py` is an executable example using Python 3 and
`paho-mqtt==2.1.0`. SQLite `inbox` is its durable handoff point; `task_latest`
tracks the highest committed task sequence, and `conflicts` records content
conflicts. It does not implement a business platform, SMS forwarding, retention,
or monitoring. When integrating, add the application's own business writes
inside `Inbox.persist`'s transaction before the commit, or use an equivalent
transactional outbox to deliver downstream work.

```bash
python3 -m venv .venv-ack
.venv-ack/bin/pip install paho-mqtt==2.1.0
.venv-ack/bin/python tools/mqtt_ack_receiver.py \
  --host TEST_BROKER --port 1883 --node TEST_NODE \
  --client-id test-business-inbox --username TEST_BUSINESS_USER \
  --password-file /private/path/mqtt-password \
  --database /private/path/mqtt-inbox.db
```

For a TLS listener, add `--tls --port 8883`, and `--ca /path/ca.pem` if the CA is
not trusted by the system. Keep the password file and inbox directory private;
the database contains full message payloads, including SMS text. The CLI creates
new files with umask 077 and does not log message contents. Back up the inbox with
SQLite's backup API. Keep one stable client ID per concurrently running receiver.

The network callback does not wait synchronously for MQTT PUBACK: doing so blocks
the same loop that receives it. If the ACK publish is lost, the original-message
retry supplies the recovery path. Never deploy a dummy ACK subscriber alongside
the actual business backend: it could confirm messages before the backend saves
them. Use the example on an isolated test broker until integrated.

## Node retention and retries

With business ACK enabled, PUBACK does not clear `outbox_pending`. Unconfirmed
messages survive node restarts and are not expired by the cleanup job. Retries
use 10/20/40/80/160/300 second backoff plus jitter; reconnect wakes pending
messages immediately. Payload ID, sequence, timestamp and content stay unchanged.

Acknowledged events and older task updates can be cleaned after seven days.
The latest task response must remain for the lifetime of the task dedupe record
(normally at least 30 days; unresolved `uncertain` tasks remain). Otherwise a
duplicate command would be recognized but have no response left to replay.
This retention fix does not require a database schema migration. It cannot
reconstruct replies already deleted by an older version.

## Enablement and acceptance

Use the isolated test host first. Confirm no running commands before changing
MQTT settings. Disable MQTT, save the desired ACK setting, then re-enable to
avoid overlapping old/new runtime clients; do not change modem/tunnel settings.

Test actual broker traffic and the receiver's committed database:

1. Withhold ACK: task/event payloads must repeat unchanged and remain pending.
2. ACK only the highest task seq: lower unacknowledged seq values must remain.
3. Send invalid/unknown ACK identities: unrelated pending records must remain.
4. Drop an ACK after receiver commit and restart the receiver: one durable row,
   repeated ACK, eventual pending count zero.
5. Restart broker and node while records are pending: recover exact payloads
   without rerunning the underlying command. Confirm correctly ACKed records
   stop retrying. A new node.hello is a new event and requires its own ACK.

Application tests (no broker required):

```bash
go test ./internal/nodemqtt ./internal/store
python3 -m unittest discover -s tools -p test_mqtt_ack_receiver.py
```

For production, deploy the validated node build, integrate the receiver, verify
both broker ACL directions, and then enable ACK. If the receiver is not ready,
leave the production ACK switch off and state that limitation. A passing isolated
test does not prove production credentials, ACLs or business database integration.
