import type { DeviceListItem } from "../../types";

/** One selectable sender: a single-line device, or one line of a multi-tunnel device. */
export interface SenderOption {
  value: string;
  deviceId: string;
  sessionId?: string;
  label: string;
  disabled: boolean;
}

function deviceTail(id: string): string {
  const tail = id.split("-").slice(-3).join("-");
  return tail && tail !== id ? `…${tail}` : id;
}

/** Flatten devices into senders so the user picks a number, not a device then a line. */
export function senderOptions(devices: DeviceListItem[], notReady: string): SenderOption[] {
  const out: SenderOption[] = [];
  for (const d of devices) {
    const summary = d.multisim;
    if (summary?.enabled && summary.lines?.length) {
      for (const line of summary.lines) {
        const who = line.phoneNumber || line.name || line.iccidSuffix;
        out.push({
          value: `${d.id}|${line.sessionId}`,
          deviceId: d.id,
          sessionId: line.sessionId,
          label: `${who} · ${deviceTail(d.id)}${line.smsReady ? "" : ` · ${notReady}`}`,
          disabled: !line.smsReady,
        });
      }
      continue;
    }
    const runtime = d.vowifiRuntime;
    const who = runtime?.localPhone || d.name || d.id;
    out.push({ value: d.id, deviceId: d.id, label: `${who} · ${deviceTail(d.id)}`, disabled: false });
  }
  return out;
}

