import type { SMSContact, SMSMessage } from "../../types";
import { tl, useI18n } from "../../lib/i18n";

// Aggregated conversation row built from an SMSContact (mirrors the VoHive `Os` mapper).
export interface SmsThread {
  key: string;
  modemImei: string;
  iccid: string;
  imsi: string;
  peer: string;
  deviceId: string;
  lastTs: number;
  lastSmsId: number;
  lastMessage: string;
  lastDeviceName?: string;
  localPhone: string;
  peerLower: string;
  lastMessageLower: string;
  // Authoritative read state from the database (unreadCount from /sms/contacts).
  unread: boolean;
}

// Long-press action-sheet target (mobile).
export type SmsActionTarget =
  | { type: "thread"; thread: SmsThread }
  | { type: "message"; message: SMSMessage };

export interface SmsEncodingInfo {
  encoding: "GSM7" | "UCS2";
  parts: number;
  units: number;
  unitName: "septets" | "chars";
}

// GSM 03.38 basic character set (LF + CR included) and extension table (counts as 2 septets).
const GSM7_BASIC = new Set(
  Array.from(`@£$¥èéùìòÇ
Øø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ !"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà`),
);
const GSM7_EXT = new Set(Array.from("^{}\\[~]|€"));

// Segment/encoding estimator, faithful to the reference `Ge`.
export function analyzeSmsEncoding(text: string): SmsEncodingInfo {
  const s = String(text || "");
  let units = 0;
  let gsm = true;
  for (const ch of Array.from(s)) {
    if (GSM7_BASIC.has(ch)) {
      units += 1;
      continue;
    }
    if (GSM7_EXT.has(ch)) {
      units += 2;
      continue;
    }
    gsm = false;
    break;
  }
  if (gsm) {
    return { encoding: "GSM7", parts: units <= 160 ? 1 : Math.ceil(units / 153), units, unitName: "septets" };
  }
  const chars = Array.from(s).length;
  return { encoding: "UCS2", parts: chars <= 70 ? 1 : Math.ceil(chars / 67), units: chars, unitName: "chars" };
}

export function toTs(value?: string | number | null): number {
  const t = new Date(value as string).getTime();
  return Number.isFinite(t) ? t : 0;
}

export function timeLabel(ts: number): string {
  return ts ? new Date(ts).toLocaleTimeString() : "";
}

export function dayLabel(ts: number): string {
  const d = new Date(ts);
  if (!Number.isFinite(d.getTime())) return tl("未知日期");
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

export function deriveThread(c: SMSContact): SmsThread {
  const lastMessage = String(c.lastContent ?? c.lastMessage ?? "").slice(0, 80);
  const modemImei = String(c.modemImei || "").trim();
	const iccid = String(c.iccid || "").trim();
	const subscriptionKey = iccid || (c.imsi ? `imsi:${c.imsi}` : "unknown");
  return {
		key: `${modemImei || `device:${c.deviceId}`}|${subscriptionKey}|${c.peer}`,
    modemImei,
		iccid,
    imsi: c.imsi,
    peer: c.peer,
    deviceId: c.deviceId,
    lastTs: toTs(c.lastTimestamp),
    lastSmsId: c.lastSmsId || 0,
    lastMessage,
    lastDeviceName: c.deviceName,
    localPhone: c.localPhone || "",
    peerLower: String(c.peer || "").toLowerCase(),
    lastMessageLower: lastMessage.toLowerCase(),
    unread: Number(c.unreadCount) > 0,
  };
}

export function sortThreads(list: SmsThread[]): SmsThread[] {
  return [...list].sort((a, b) => b.lastTs - a.lastTs);
}

export function sortMessages(list: SMSMessage[]): SMSMessage[] {
  return [...list].sort((a, b) => toTs(a.timestamp) - toTs(b.timestamp) || a.id - b.id);
}

export function filterThreads(list: SmsThread[], query: string): SmsThread[] {
  const q = String(query || "").trim().toLowerCase();
  if (!q) return list;
  return list.filter((t) => t.peerLower.includes(q) || t.lastMessageLower.includes(q));
}

// Backend encodes direction numerically (1 = received, 2 = sent); fall back to direction text.
export function messageType(m: SMSMessage): number {
  const t = Number(m.type);
  if (t === 1 || t === 2) return t;
  const d = String(m.direction || "").toLowerCase();
  return d === "inbound" || d === "received" || d === "in" ? 1 : 2;
}

export type SmsDeliveryIndicator = "delivered" | "accepted_unconfirmed" | "failed" | "unknown";

// +CMGS and SIP 2xx prove next-hop acceptance, not recipient delivery.
export function messageDeliveryIndicator(m: SMSMessage): SmsDeliveryIndicator {
  const delivery = String(m.deliveryState || "").toLowerCase();
  const submission = String(m.status || "").toLowerCase();
  if (delivery === "delivered" || delivery === "delivery_confirmed") return "delivered";
  if (
    delivery.includes("failed") || delivery.includes("rejected") ||
    submission.includes("failed") || submission.includes("rejected") || submission.includes("partial")
  ) return "failed";
  if (
    delivery.includes("accepted") || submission.includes("accepted_by_modem") ||
    submission.includes("accepted_by_ims")
  ) return "accepted_unconfirmed";
  return "unknown";
}

export function messageBody(m: SMSMessage): string {
  return String(m.content ?? m.body ?? "");
}

export interface MessageGroup {
  date: string;
  items: SMSMessage[];
}

export function groupMessagesByDay(list: SMSMessage[]): MessageGroup[] {
  const groups: MessageGroup[] = [];
  let current = "";
  for (const m of list) {
    const day = dayLabel(toTs(m.timestamp));
    if (!groups.length || day !== current) {
      groups.push({ date: day, items: [m] });
      current = day;
    } else {
      groups[groups.length - 1].items.push(m);
    }
  }
  return groups;
}
