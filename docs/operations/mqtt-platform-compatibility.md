# MQTT node compatibility with PingCode

This change only modifies the VoCat node. PingCode is an independently owned
business receiver. Its source at `79024d0` was inspected, not modified.

## Node and profile identity

`inventory.get` enumerates current physical EC20 modules once per IMEI, including
blank modules. `target.slot` in phone records is that same module IMEI. Twenty-one
profiles in one module are twenty-one phone records sharing one slot.

As explicitly requested by the operator on 2026-09-17, `phones.list` includes
blank-card rows by default, exposing their real module IMEI in `target.slot`.
Set `params.include_empty_slots=false` on every page to return only actual
profiles with nonempty ICCIDs. Pagination uses one frozen snapshot and rejects
changes to this option or `page_size`; omitting the option means true on every page.
Blank-card rows still fail the inspected PingCode backend's required-ICCID
validation. This default change does not fix that backend validation or create
platform slot records; the operator requested deployment with that limitation known.

Count physical slots with `inventory.get`, or deduplicate slot identities across
the entire relevant snapshot. Adding per-page distinct counts can count a module
twice when its profiles cross a page boundary. The node cannot remove historical
records from the platform's database or correct a platform-side page counter.

## Version compatibility

PingCode's `software_version` database column and input validation allow only
40 characters. Node `node.hello` and `node.describe` now share a compact version
bounded to 40 characters; short versions remain unchanged. Long versions keep
a prefix plus a digest to distinguish builds. `software_build` retains the full
original build identifier. Historical events keep their original immutable data.

## Acknowledged task replay

A duplicate command replays the stored latest result. An earlier business ACK
continues to prove that result was durably accepted: replay does not erase it.
The replay is retried until MQTT transport confirmation, then stops even when a
receiver does not repeat its business ACK. A result never business-ACKed still
requires the exact `(node,kind,id,seq)` ACK and keeps retrying.

## Explicitly pause historical delivery

These are authenticated HTTP administration endpoints with normal CSRF checks:

- `GET /api/settings/node-mqtt/outbox`: at most 200 unresolved message metadata
  records, no payload bodies. While `complete=false`, pass `next_after` as the
  next request's `after` query parameter. Counts distinguish actively pending
  and manually paused delivery.
- `POST /api/settings/node-mqtt/outbox/pause`: exact identity list plus reason.
- `POST /api/settings/node-mqtt/outbox/resume`: same identity list.

Example pause body:

```json
{"reason":"operator paused historical integration replies","items":[{"kind":"task","id":"example-task-id","seq":3}]}
```

Each batch contains 1–100 identities in the currently configured node. Event
sequence is 0; task sequences start at 1. Unknown or cross-node identities reject
the whole batch. Pause preserves payload, dedupe history, and actual ACK state.
It survives broker reconnects, duplicate commands and service restarts. An
already publishing message may complete before the pause takes effect. Resume
makes the original unacknowledged message eligible again. Nothing auto-pauses or
discards SMS events. Heartbeat and local status expose `outbox_paused` separately;
`outbox_pending` counts only unresolved messages still scheduled for delivery.

This is not a business confirmation. Pause only messages the operator has chosen
to stop retrying. Do not disable business ACK globally or publish synthetic ACKs
to clear the counter.

## Deployment and rollback

SQLite migration 29 adds replay and pause metadata. Before upgrading, back up the
database with SQLite backup, the matching MQTT key, and the old binary. Downgrade
requires restoring the matching schema-28 database with its binary/key, not
opening a schema-29 database with the old binary. Test on the independent gq
node before promoting a verified build to the stable host.
