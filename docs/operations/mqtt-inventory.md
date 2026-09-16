# Querying module IMEIs through MQTT

Use `inventory.get` to enumerate attached Quectel EC20 modules. `phones.list`
enumerates configured phone/profile rows: several phones can share an IMEI,
and a module without configured profiles need not appear there. Deduplicating
`phones.list` cannot produce a complete hardware inventory.

Send the usual command envelope (`id`, `action`, `time`, `expires_at`, `params`)
with `action: "inventory.get"`, no `target`, and `params: {"page_size": 100}`.
Use current UTC+8 timestamps in `YYYY-MM-DD HH:mm:ss` format. Subscribe to the
node's `/task` topic and durably save each task update before acknowledging its
exact `id` and `seq`; see [business ACK](mqtt-business-ack.md).

The successful result contains `items`; each item's `slot` is its module IMEI
and `device` is its selected device record ID. Modules without cards or saved
profiles remain in the result. Historical records for absent modules and
virtual PC/SC readers are excluded. Multiple saved records for one IMEI yield
one item, preferring the active group, then the IMEI-named record.

An attached but unregistered module is included with `state: "pending"` and
its discovery ID. Register it in VoCat before using that ID for device actions;
inventory does not create records or start tunnels. A discovered EC20 with an
unresolved or ambiguous IMEI returns `IDENTITY_PENDING`, rather than silently
claiming a complete inventory with that module omitted. Retry after discovery
settles and investigate persistent identity errors.

Follow `cursor` with a new command ID and the same `page_size` until `complete`
is true. Pages share one immutable snapshot and `generated_at`, even if devices
change meanwhile. Cursors expire after five minutes, after process restart, or
when displaced by the 32-snapshot bound. On `CURSOR_EXPIRED`, discard the partial
collection and restart without a cursor. Old numeric cursors are not supported.

`complete` means all pages of this module snapshot were returned. It does not
mean every card was freshly read. Profile metadata still comes from saved
configuration: `source: "registry"`, `profiles_complete: false`, unknown profile
states, and no claimed current ICCID or profile observation timestamp. Use
`esim.profiles.list` or `slot.refresh` for a targeted card read and check its
completeness. Full hardware profile enumeration required by the design contract
remains a separate implementation gap.

The node can only report modules discovered by the host. Compare results with
USB discovery; a physically expected module absent from USB needs hardware or
discovery diagnosis. Do not reset a live hub merely to change an inventory count.
