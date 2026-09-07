# Multi-tunnel reception implementation plan

> **For agentic workers:** Use the subagent-driven-development or executing-plans workflow for the owned tasks. Do not edit another worker's files.

**Goal:** Deploy opt-in multi-profile reception without periodic profile-rotation reception.

**Architecture:** Separate reader ownership and on-demand AKA from per-subscription IKE/IMS sessions. Preserve the existing inbox by projecting each received SMS to the physical device with its immutable subscription identity.

**Tech Stack:** Go, existing VoCat providers/runtime, SQLite migration 26, React device UI, EC20 AT/eUICC.

**Spec:** `docs/superpowers/specs/2026-09-07-multi-tunnel-design.md`

## Global constraints

Based on `91fa49f`; no new service port, no credentials in artifacts, no new fake physical devices, no MO test traffic. Only the specified public device opts in. Full in-place rekey is not claimed. Recovery must clear readiness immediately. Preserve unrelated work and the original PoC.

## 1. Reader and group lifecycle — protocol_review

Files: new `internal/vowifi/multisim/*.go` and behavioral tests.

- [x] Define validated Config/Profile/GroupState contracts and stable logical IDs.
- [x] Add failing tests for queued cancellation, wrong identity, per-reader transaction exclusion, independent line recovery, stop during setup, and ownership during failed restore.
- [x] Implement AuthBroker and group manager around existing runtime/orchestrator contracts.
- [x] Run `go test -race ./internal/vowifi/multisim`.

## 2. Persistence, API, ownership guards and UI — vocat_sessions

Files: `internal/store`, `internal/server`, `web/src` scoped to multi-SIM configuration, device actions, automatic tasks and modem SMS synchronization.

- [x] Add migration 26 and configuration round-trip/delete tests.
- [x] Add authenticated GET/PUT group endpoint and per-line reconnect endpoint; verify invalid profiles, busy ownership and conflicting tasks are rejected.
- [x] Guard physical mutations and pending task execution; skip modem SMS scanning for owned readers.
- [x] Render profile selection and per-line readiness in the eSIM device tab.
- [x] Run store/server tests and build the web assets.

## 3. Reliable failure detection — mdd_limits

Files: `internal/vowifi/ike`; only immediate readiness invalidation in `internal/vowifi/orchestrator.go` outside that package.

- [x] Add failing tests for control/data failure aggregation, authenticated DPD timeout and forged traffic rejection.
- [x] Implement bounded DPD and aggregate terminal notifications for both userspace and XFRM paths.
- [x] Reject conflicting tunnel inner addresses before route installation.
- [x] Run `go test -race ./internal/vowifi/ike ./internal/vowifi`.

## 4. IMS authentication/ACK concurrency — collaborating session

Files: `internal/vowifi/ims/provider.go`, `sms_runtime.go`, and focused tests.

- [x] Reproduce delayed authentication blocking MT/RP-ACK and establish exact shared fields.
- [x] Narrow state locking without losing CSeq/authentication/Close consistency.
- [x] Verify concurrent MT/renewal and cancellation with race tests.

## 5. Product wiring and deployment — root

Files: `cmd/vocat/main.go`, new `cmd/vocat/multisim.go`, integration tests and rollout evidence.

- [x] Implement physical backend, per-line adapter and shared SMS persistence callbacks.
- [x] Load desired configurations at startup; suppress single-line startup/reconciliation while owned; close groups before device teardown.
- [x] Test policy handoff, profile restore, immutable SMS ownership and factory composition.
- [x] Build UI and static binary; run required repository checks once after integration.
- [x] Back up live binary/SQLite/service invocation, deploy and enable only the designated reader.
- [x] Test inactive-profile MT, observed renewal, individual-line reconnect and service restart; use few externally triggered messages at these decision points.
- [x] Record the actual behavior, remaining limitations, enabled configuration and rollback command; do not leave a failed trial owning the reader.

## Deployment evidence

Implemented in `874b9eb`; deployed `0.1.0-multitunnel.20260907.3` on the designated public reader. Five external messages were received. Three-line coexistence, ordinary in-session refresh, individual reconnect, original-profile restoration and service restart were verified. One A-line interruption recovered automatically; continuous availability and full in-place rekey are not claimed. Evidence: `Note/多隧道-上线与三号实测-2026-09-07.md` in the parent workspace.
