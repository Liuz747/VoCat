# Multi-profile and native MQTT release baseline

The application baseline is commit `d72bf47b0dd8e142acc45ea8078f236ae5f3300d`.
The release preparation adds documentation and a portable systemd template; it
does not change modem, tunnel, or MQTT application behavior. Record the exact
release-preparation commit and binary SHA-256 with each deployment.

## Included changes

- Multi-profile runtime: incremental profile reconciliation, delayed startup when
  a reader is absent, bounded AKA retries, and reader rebinding by modem/card identity.
- Device/UI handling: explicit VoWiFi disable remains effective; placeholder
  profiles do not start tunnels; detached modules are hidden by default, with
  separate attached-device counts; a call panel is available.
- eUICC operations can borrow the running group's reader transaction instead of
  racing profile switching and AKA authentication.
- Native MQTT 5 node integration: configuration UI/API, command/task processing,
  persistent outbox, inventory, profile/tunnel actions and SMS events. A fresh
  database starts with MQTT disabled.
- `251f5b9` confirms downloads by reading the returned ICCID in the same reader
  transaction. A successful download does not implicitly enable or start it.
- `d72bf47` resolves explicit profile operations through the eUICC ISD-R address
  and rolls back a newly saved group entry if runtime application is rejected.

## Build from a clean checkout

Use a Go toolchain satisfying `go.mod` and a Node/npm version supported by the
locked frontend dependencies. Resolve dependencies with the lockfile. Build the
frontend **before** Go tests or compilation: `web/embed.go` embeds `web/dist`.

```bash
npm --prefix web ci
npm --prefix web run build
npm --prefix web test
go test ./...
go test -race ./internal/nodemqtt ./internal/store ./cmd/vocat
```

Build on Linux for a Linux amd64 host, from a committed clean tree:

```bash
test -z "$(git status --porcelain)" || exit 1
release_commit=$(git rev-parse HEAD)
release_version="0.1.0-multitunnel.$(git rev-parse --short=12 HEAD)"
release_time=$(git show -s --format=%cI HEAD)
mkdir -p build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
  -ldflags "-s -w -X vocat/internal/buildinfo.Version=$release_version -X vocat/internal/buildinfo.BuildTime=$release_time" \
  -o build/vocat ./cmd/vocat
go version -m build/vocat > build/build-info.txt
sha256sum build/vocat > build/SHA256SUMS
printf '%s\n' "$release_commit" > build/COMMIT
```

Archive the binary, checksum, commit, build metadata and dependency lockfile with
the deployment. Build output, database files and credentials must not be committed.
Do not use the upstream installer/self-updater to silently replace a pinned fork
build with the upstream release.

## Independent test host

1. Give the test host its own empty database, administrator credential and MQTT
   identity. Never copy a live host's database to a second active host: it contains
   desired tunnel state, device bindings, credentials, tasks and event cursors.
2. Inventory USB topology and IMEI. Check existing device/card ownership before
   moving a module: disable the old record/group before starting the new owner.
   If the old host must remain untouched, do not start a profile whose old binding
   has not been confirmed disabled. Power off the hub before changing modules.
3. Ensure only the intended modem controller owns the serial ports. Check
   ModemManager, pcscd, MDD and other modem services before enabling VoCat. Merely
   seeing an EC20 in `lsusb` does not establish card or tunnel readiness.
4. Install the pinned binary at `/opt/vocat/bin/vocat`, create
   `/opt/vocat/data` with root-only access, and run `vocat bootstrap-admin`
   with the new password on stdin before starting the service. Initialize the
   fresh DB, not a copy of another host's DB.
5. Install `deploy/vocat-simhub.service` as `vocat.service`. It runs `serve`
   explicitly, restarts after crashes, allows 120 seconds for orderly shutdown,
   and keeps `LimitNOFILE=65536`. Optional non-secret host overrides go in
   `/etc/vocat/vocat.env`; create the file root-owned with mode 0600. Application
   secrets are initialized through stdin or authenticated APIs.
6. Confirm installed SHA-256, `vocat version`, systemd active/enabled state and
   `/api/health`. Log in through the host itself for initial setup. For Tailscale
   IPv4 clients, keep access mode `internal` and explicitly allow
   `100.64.0.0/10` through the security settings API. Confirm the client bypasses
   any unrelated HTTP proxy.
7. Add only locally discovered modules, identify them by IMEI, read the card and
   installed profiles, and enable only the intended test lines. The add-device
   API enables VoWiFi by default on supported hardware; confirm card ownership
   **before** adding a real profile-bearing module.
8. Use an isolated MQTT broker unless separate test credentials/ACLs have been
   provisioned on a shared broker. Set a unique node and client ID. Verify actual
   `node.describe`/inventory task replies, not just the connected flag.

The stable host retains its pinned binary, database and active node identity.
All new builds, hub experiments and protocol tests go to the test host first.
Promotion back to the stable host is a separate deliberate deployment.

## Acceptance and known limits

Record each layer independently: USB/IMEI discovery, real EID/profile reads,
registration/tunnel state, MQTT task replies, actual SMS reception and events,
then restart recovery. A health endpoint, SIP acceptance or MQTT publish alone
does not prove end-to-end message delivery. An empty card cannot validate a
carrier tunnel; document that boundary instead of reporting it as a pass.

This baseline has short live-test evidence for adding a profile while existing
tunnels run, explicit incremental tunnel startup, ensure idempotency and
bidirectional SMS. It does not establish long-term stability, natural rekey
recovery or another broker's business-account ACL behavior.

Known limitations to preserve in test planning:

- MQTT profile deletion has not passed a real-card success-path acceptance.
  Its handler still calls the device manager directly; do not use it to delete
  profiles from a running multi-profile group.
- Phone lists can be cached even when their source field says `hardware`.
- Leave MQTT connection settings unchanged while tasks are running. Configuration
  replacement and recovery of in-flight tasks need separate validation.
- A task or event transport acknowledgement is distinct from business persistence;
  business ACK is optional and must be deliberately configured on both ends.

## Rollback

Before replacing an existing test deployment, stop VoCat gracefully and back up
the binary, effective unit/environment, SQLite database and its sibling
`<database-path>.node-mqtt.key` together. The MQTT key encrypts sensitive pending
task parameters: restoring the database without its matching key cannot recover
those tasks. Keep the key root-owned, mode 0600, and out of source control. Include
SQLite WAL/SHM state by using SQLite's backup API or copying only after shutdown.
Record the schema version. Restore the compatible binary/database/key set while
the service is stopped, then repeat the acceptance checks above. Do not point a
rollback instance at a database still owned by another running process.
