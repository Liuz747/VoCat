export interface AutomaticTaskProfileGroup {
  aidHex?: string;
  profiles?: Array<{
    iccid: string;
    name?: string;
    serviceProviderName?: string;
  }>;
}

export interface AutomaticTaskProfileOption {
  iccid: string;
  aidHex: string;
  label: string;
}

export interface AutomaticTaskProfileRequestGuard {
  begin: () => number;
  invalidate: () => void;
  isCurrent: (requestID: number) => boolean;
}

export function createAutomaticTaskProfileRequestGuard(): AutomaticTaskProfileRequestGuard {
  let latestRequestID = 0;
  return {
    begin: () => ++latestRequestID,
    invalidate: () => { latestRequestID += 1; },
    isCurrent: (requestID) => requestID === latestRequestID,
  };
}

export function buildAutomaticTaskProfileOptions(
  groups: AutomaticTaskProfileGroup[],
  currentICCID: string,
  currentSIMLabel: string,
): AutomaticTaskProfileOption[] {
  const options = groups.flatMap((group, groupIndex) =>
    (group.profiles || []).map((profile) => ({
      iccid: profile.iccid,
      aidHex: group.aidHex || "",
      label: `${profile.name || profile.serviceProviderName || `Profile ${groupIndex + 1}`} · ${profile.iccid}`,
    })),
  );
  const iccid = currentICCID.trim();
  if (iccid && !options.some((option) => option.iccid.trim() === iccid)) {
    options.push({ iccid, aidHex: "", label: `${currentSIMLabel} · ${iccid}` });
  }
  return options;
}

export function selectAutomaticTaskProfileOption(
  options: AutomaticTaskProfileOption[],
  requestedICCID: string,
): AutomaticTaskProfileOption | undefined {
  const iccid = requestedICCID.trim();
  if (iccid) return options.find((option) => option.iccid.trim() === iccid);
  return options[0];
}

// Profile rotation: an ordered list of profiles the task cycles through.
// Click order is rotation order; toggling an already-selected profile removes it.
export function toggleRotationProfile(
  selected: AutomaticTaskProfileOption[],
  option: AutomaticTaskProfileOption,
): AutomaticTaskProfileOption[] {
  const iccid = option.iccid.trim();
  if (selected.some((entry) => entry.iccid.trim() === iccid)) {
    return selected.filter((entry) => entry.iccid.trim() !== iccid);
  }
  return [...selected, option];
}

export function moveRotationProfile(
  selected: AutomaticTaskProfileOption[],
  iccid: string,
  direction: -1 | 1,
): AutomaticTaskProfileOption[] {
  const index = selected.findIndex((entry) => entry.iccid.trim() === iccid.trim());
  const target = index + direction;
  if (index < 0 || target < 0 || target >= selected.length) return selected;
  const next = [...selected];
  [next[index], next[target]] = [next[target], next[index]];
  return next;
}

// Measured on EC20 + T-Mobile eSIM (2026-09-05, rotation10): EnableProfile +
// verification ≈ 2.3 s and VoWiFi tunnel + IMS registration ≈ 4–6 s, so a
// switch costs about ten seconds of offline time.
export const ROTATION_SWITCH_SECONDS = 10;

export function estimateRotationCycleSeconds(profileCount: number, dwellSeconds: number): number {
  if (profileCount <= 0) return 0;
  return profileCount * (ROTATION_SWITCH_SECONDS + Math.max(0, dwellSeconds));
}

// Splits a server instant into the values of <input type="date"> and
// <input type="time"> in the browser's local zone. The Go zero time (year 1)
// and unparsable input mean "not set".
export function splitLocalDateTime(iso: string): { date: string; time: string } {
  if (!iso || iso.startsWith("0001-")) return { date: "", time: "" };
  const value = new Date(iso);
  if (Number.isNaN(value.getTime())) return { date: "", time: "" };
  const pad = (n: number) => String(n).padStart(2, "0");
  return {
    date: `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`,
    time: `${pad(value.getHours())}:${pad(value.getMinutes())}`,
  };
}
