# Single-reader multi-tunnel reception

## Goal and authorization

The user requests more decisive tests and deployment of multi-tunnel reception to replace profile-rotation reception. A prior independent PoC proved actual A and B MT on one single-active eUICC while B remained enabled. This change makes that path an explicit product mode. Start with the user-designated EC20 on the public machine, keeping other configured devices unchanged.

## Design

- A physical reader owns card switching, RF-off state, and one cancelable authentication transaction lock. Each subscription owns a stable logical session ID, immutable identity, independent IKE/IMS lifecycle, and independent readiness.
- Persist an opt-in device multi-SIM configuration separately from device configuration and automatic tasks. Never duplicate physical `devices` records to represent subscriptions. Proxy selection remains by ICCID.
- Reuse `runtime.Manager` and `vowifi.Orchestrator` for independent session retries where practical. The group owns the physical device before stopping its old single-line runtime. The single-line reconciler, automatic tasks, SMS modem sync and external mutating device APIs must honor group ownership. Ownership is released only after sessions are closed and the original profile and ordinary policy are restored.
- Both EAP and IMS use the same per-reader AuthBroker. The entire select/verify/authenticate operation is serialized; queue time counts toward the caller's deadline. The broker activates a profile only for a real identity/authentication request. It never periodically rotates profiles or caches AKA results for future challenges.
- Persist MT through the existing SMS storage path, remapping logical session ID to the physical device ID while retaining the session's immutable ICCID/IMSI. Existing per-device inbox and notifications continue working.
- Expose persistent settings plus live per-line states via authenticated API and the device eSIM page. A requested configuration or established IKE tunnel is not SMS readiness. A reconnecting line must visibly lose readiness immediately.
- Detect silent tunnel loss with authenticated bounded DPD, aggregate data/control-plane failures, and independently rebuild failed lines. Full in-place IKE/CHILD rekey is outside this first revision; recovery may create a brief outage on the affected line, which must be represented honestly. Do not claim uninterrupted or production-scale service from the short PoC.

## Isolation and rollout

Work in `feature/multi-tunnel` at `/tmp/esim-multisession-vtsoc2l9/VoCat-product`, based on `91fa49f`. The original checkout has unrelated documentation changes; do not alter them. Keep the original PoC and evidence archive intact. The designated public device is `usb-2c7c-0123456789abcdef-1-1-3-4-2`. Public credentials and SMS contents must not enter source or reports. No SIM-to-SIM MO tests; use a small number of external MT triggers at milestones.

Before replacing a running binary, build assets and binary, preserve the live executable and a consistent SQLite backup, record the service invocation and current device/task policies, and prepare rollback. Enable the new mode only for the designated device. Test the deployed UI/API and actual MT, not only unit tests. Reconcile and confirm other devices after restart.

## Decisive acceptance evidence

1. Deterministic tests show queued cancellation, wrong-identity rejection, no card switch for ordinary MT, and serialized new A/B AKA without cross-routing vectors.
2. A delayed new IMS authentication does not prevent independent MT persistence and RP-ACK; Close/cancellation does not deadlock.
3. Bad or unauthenticated IKE traffic cannot trigger card reauthentication. Authenticated terminal failure or bounded DPD timeout revokes readiness and drives one-line recovery.
4. API configuration persists, settings survive restart, and automatic/manual card mutation cannot race group ownership.
5. The designated device actually receives external MT on inactive and active profiles, after at least an observed IMS refresh and after an intentional individual-line recovery. Record actual events and limitations, not an arbitrary soak duration alone.
6. Disable/rollback restores ordinary service; unrelated devices retain their configuration and recover actual runtime readiness. UI truthfully reflects any remaining degraded line.
