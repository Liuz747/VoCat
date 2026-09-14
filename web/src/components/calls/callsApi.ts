import { api } from "../../api";
import type { DeviceListItem, DevicesResponse } from "../../types";

/** One call as reported by the device's call API, single-line or multi-tunnel. */
export interface CallItem {
  id: string;
  number: string;
  direction: "incoming" | "outgoing" | string;
  state: string;
  startedAt?: string;
  answeredAt?: string;
  endedAt?: string;
  sipCode?: number;
  reason?: string;
  mediaReady?: boolean;
  codec?: string;
  /** Multi-tunnel only: the line that carries the call. */
  sessionId?: string;
  iccid?: string;
  phoneNumber?: string;
}

export interface CallsSnapshot {
  transport: string;
  multisim: boolean;
  calls: CallItem[];
}

/** One selectable line: a single-line device, or one line of a multi-tunnel device. */
export interface CallLineOption {
  value: string;
  deviceId: string;
  sessionId?: string;
  label: string;
  phone?: string;
  imsReady: boolean;
  disabled: boolean;
}

function deviceTail(id: string): string {
  const tail = id.split("-").slice(-3).join("-");
  return tail && tail !== id ? `…${tail}` : id;
}

export async function listCallDevices(): Promise<DeviceListItem[]> {
  const res = await api<DevicesResponse>("/devices");
  return (res.devices || []).filter((d) => d.running || d.multisim?.enabled);
}

/** Flatten devices into lines so the user picks a number, not a device then a line. */
export function callLineOptions(devices: DeviceListItem[], notReady: string, cellular: string): CallLineOption[] {
  const out: CallLineOption[] = [];
  for (const d of devices) {
    const summary = d.multisim;
    if (summary?.enabled && summary.lines?.length) {
      for (const line of summary.lines) {
        const who = line.phoneNumber || line.name || line.iccidSuffix;
        out.push({
          value: `${d.id}|${line.sessionId}`,
          deviceId: d.id,
          sessionId: line.sessionId,
          phone: line.phoneNumber,
          imsReady: line.imsReady,
          label: `${who} · ${deviceTail(d.id)}${line.imsReady ? "" : ` · ${notReady}`}`,
          disabled: !line.imsReady,
        });
      }
      continue;
    }
    const runtime = d.vowifiRuntime;
    const imsReady = Boolean(runtime?.imsReady);
    const who = runtime?.localPhone || d.name || d.id;
    out.push({
      value: d.id,
      deviceId: d.id,
      phone: runtime?.localPhone,
      imsReady,
      label: `${who} · ${deviceTail(d.id)}${imsReady ? "" : ` · ${cellular}`}`,
      disabled: false,
    });
  }
  return out;
}

const CLCC_STATE: Record<number, string> = {
  0: "active",
  1: "held",
  2: "dialing",
  3: "ringing",
  4: "ringing",
  5: "waiting",
};

interface RawCallsResponse {
  transport?: string;
  multisim?: boolean;
  calls?: Array<Record<string, unknown>>;
}

export async function getCalls(deviceId: string): Promise<CallsSnapshot> {
  const res = await api<RawCallsResponse>(`/devices/${encodeURIComponent(deviceId)}/calls`);
  const transport = res.transport || "vowifi";
  const raw = res.calls || [];
  if (transport === "vowifi") {
    return { transport, multisim: Boolean(res.multisim), calls: raw as unknown as CallItem[] };
  }
  // Circuit-switched listing (AT+CLCC) uses numeric fields; normalise it so the
  // panel can render both shapes the same way.
  const calls = raw.map((item) => {
    const index = Number(item.index ?? 0);
    const direction = Number(item.direction ?? 0) === 1 ? "incoming" : "outgoing";
    const state = CLCC_STATE[Number(item.state ?? -1)] || "unknown";
    return { id: String(index), number: String(item.number ?? ""), direction, state } satisfies CallItem;
  });
  return { transport, multisim: false, calls };
}

export interface CallActionResult {
  accepted: boolean;
  action: string;
  callId?: string;
  transport: string;
  call?: CallItem;
}

function lineBody(sessionId?: string) {
  return sessionId ? { sessionId } : {};
}

export function dialCall(deviceId: string, number: string, sessionId?: string): Promise<CallActionResult> {
  return api<CallActionResult>(`/devices/${encodeURIComponent(deviceId)}/calls/dial`, {
    method: "POST",
    body: { number, durationSeconds: 0, ...lineBody(sessionId) },
  });
}

export function answerCall(deviceId: string, callId: string, sessionId?: string): Promise<CallActionResult> {
  return api<CallActionResult>(`/devices/${encodeURIComponent(deviceId)}/calls/answer`, {
    method: "POST",
    body: { callId, ...lineBody(sessionId) },
  });
}

export function hangupCall(deviceId: string, callId: string, sessionId?: string): Promise<CallActionResult> {
  return api<CallActionResult>(`/devices/${encodeURIComponent(deviceId)}/calls/hangup`, {
    method: "POST",
    body: { callId, ...lineBody(sessionId) },
  });
}

/** Same-origin WebSocket URL of the PCM bridge for one call. */
export function callMediaSocketURL(deviceId: string, callId: string, sessionId?: string): string {
  const params = new URLSearchParams({ call_id: callId });
  if (sessionId) params.set("session_id", sessionId);
  const scheme = window.location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${window.location.host}/api/devices/${encodeURIComponent(deviceId)}/calls/media?${params.toString()}`;
}

/** Terminal states never come back; everything else is a live call. */
export function isLiveCall(call: CallItem): boolean {
  return call.state !== "ended" && call.state !== "failed";
}
