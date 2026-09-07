import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, apiMessage } from "../../api";
import { Button, StatusDot, message } from "../ui";
import { cx, formatDateTime } from "../../lib/utils";
import { tf, useI18n } from "../../lib/i18n";
import type { EsimProfileGroup } from "./types";
import { ageText, multiSIMGroupPhaseLabel, multiSIMLinePhaseLabel, multiSIMReasonLabel } from "./multisimLabels";

interface Profile { iccid: string; aid: string; name?: string }
interface LineSecurity {
  responderAuth?: string; compatibilityOverride?: boolean; highRisk?: boolean;
  ikeEncryption?: string; ikeIntegrity?: string; ikeDhGroup?: string; espEncryption?: string; espIntegrity?: string;
}
interface LineRuntime {
  deviceId: string; iccid?: string; imsi?: string; phase: string; enabled?: boolean; active?: boolean;
  simReady: boolean; accessReady: boolean; tunnelReady: boolean; imsReady: boolean; smsReady: boolean;
  homeMcc?: string; homeMnc?: string; carrierProfile?: string; carrierProfileFrom?: string; epdg?: string;
  proxyMode?: string; proxyId?: string; tunnelName?: string; dataplaneMode?: string; imsRegistration?: string;
  phoneNumber?: string; phoneNumberSource?: string; lastErrorClass?: string; lastError?: string; lastReason?: string;
  warnings?: string[]; cleanupErrors?: string[]; security?: LineSecurity; attempt: number; sequence: number;
  startedAt?: string; updatedAt?: string;
}
interface Line { iccid: string; name?: string; sessionId: string; state: LineRuntime }
interface MultiSIMStatus {
  available: boolean; owned: boolean; activeProfileIccid: string;
  config: { deviceId: string; enabled: boolean; profiles: Profile[]; createdAt?: string; updatedAt?: string };
  state: { deviceId: string; enabled: boolean; busy: boolean; phase: string; lastError?: string; lines: Line[]; updatedAt?: string };
}
interface Props {
  deviceId: string; isActive: boolean; deviceOnline: boolean; supported: boolean;
  groups: EsimProfileGroup[]; onOwnedChange: (owned: boolean) => void;
}

const PIPELINE: { key: keyof LineRuntime; label: string }[] = [
  { key: "simReady", label: "SIM" }, { key: "accessReady", label: "Access" }, { key: "tunnelReady", label: "Tunnel" },
  { key: "imsReady", label: "IMS" }, { key: "smsReady", label: "SMS" },
];

function Detail({ label, value, mono }: { label: string; value?: string | number | boolean | null; mono?: boolean }) {
  const text = value === undefined || value === null || value === "" ? "--" : String(value);
  return <div className="flex justify-between gap-3 py-0.5"><span className="shrink-0 text-gray-500">{label}</span><span className={cx("min-w-0 truncate text-right text-gray-800 dark:text-gray-100", mono && "font-mono text-xs")} title={text}>{text}</span></div>;
}

export function DeviceMultiSIMPanel({ deviceId, isActive, deviceOnline, supported, groups, onOwnedChange }: Props) {
  const { t } = useI18n();
  const [status, setStatus] = useState<MultiSIMStatus | null>(null);
  const [selected, setSelected] = useState<Profile[]>([]);
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  const [pending, setPending] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [, setTick] = useState(0);
  const dirty = useRef(false);
  const callback = useRef(onOwnedChange); callback.current = onOwnedChange;
  const currentDevice = useRef(deviceId); currentDevice.current = deviceId;

  const applyStatus = useCallback((value: MultiSIMStatus) => {
    setStatus(value);
    if (!dirty.current || value.config.enabled || value.owned) setSelected(value.config.profiles || []);
    callback.current(value.config.enabled || value.owned);
    setError("");
  }, []);

  useEffect(() => {
    dirty.current = false; setStatus(null); setSelected([]); setError(""); callback.current(false);
    if (!isActive) return;
    const controller = new AbortController(); let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const value = await api<MultiSIMStatus>(`/devices/${encodeURIComponent(deviceId)}/multisim`, { signal: controller.signal });
        if (!controller.signal.aborted) applyStatus(value);
      } catch (err) {
        if (!controller.signal.aborted) setError(apiMessage(err) || t("多隧道状态读取失败"));
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(poll, 2000);
      }
    };
    void poll();
    const ticker = setInterval(() => setTick((v) => v + 1), 15000);
    return () => { controller.abort(); clearTimeout(timer); clearInterval(ticker); };
  }, [deviceId, isActive, applyStatus]);

  const options = useMemo(() => {
    const map = new Map<string, Profile>();
    for (const p of status?.config.profiles || []) map.set(p.iccid, p);
    for (const group of groups) for (const p of group.profiles) map.set(p.iccid, {
      iccid: p.iccid, aid: group.aidHex || "", name: p.name || p.serviceProviderName,
    });
    return [...map.values()];
  }, [groups, status?.config.profiles]);
  const owned = Boolean(status?.owned || status?.config.enabled);
  const locked = owned || saving || Boolean(status?.state.busy);

  const save = async (enabled: boolean) => {
    if (enabled && (selected.length < 2 || selected.length > 8)) { message.warning(t("请选择 2–8 个 eSIM Profile")); return; }
    const id = deviceId; setSaving(true);
    try {
      const value = await api<MultiSIMStatus>(`/devices/${encodeURIComponent(id)}/multisim`, {
        method: "PUT", body: { enabled, profiles: owned ? status?.config.profiles || [] : selected },
      });
      if (currentDevice.current !== id) return;
      dirty.current = false; applyStatus(value);
      message.success(enabled ? t("已提交启动，请等待各线路就绪") : t("已提交停止，请等待设备恢复"));
    } catch (err) { if (currentDevice.current === id) message.error(apiMessage(err) || t("保存多隧道配置失败")); }
    finally { if (currentDevice.current === id) setSaving(false); }
  };
  const lineAction = async (iccid: string, action: "reconnect" | "refresh") => {
    const id = deviceId; setPending(`${action}:${iccid}`);
    try {
      const value = await api<MultiSIMStatus>(`/devices/${encodeURIComponent(id)}/multisim/lines/${encodeURIComponent(iccid)}/${action}`, { method: "POST" });
      if (currentDevice.current === id) { applyStatus(value); message.success(action === "reconnect" ? t("已提交线路重连") : t("已提交 IMS 续约")); }
    } catch (err) { if (currentDevice.current === id) message.error(apiMessage(err) || (action === "reconnect" ? t("线路重连失败") : t("IMS 续约失败"))); }
    finally { if (currentDevice.current === id) setPending(null); }
  };

  if (!supported && !status?.config.enabled && !status?.owned && !(status?.config.profiles.length)) return null;
  const lines = status?.state.lines || [];
  const readyCount = lines.filter((line) => line.state.smsReady).length;
  const total = status?.config.profiles.length || 0;
  const groupPhase = status?.state.phase || "pending";
  const headline = error ? t("状态更新失败")
    : groupPhase === "running" && total > 0 ? (readyCount === total ? t("全部线路可接收短信") : readyCount > 0 ? tf("部分线路可用（{ready}/{total}）", { ready: readyCount, total }) : t("运行中，线路尚未就绪"))
    : multiSIMGroupPhaseLabel(groupPhase, t);
  const headTone = error ? "warning" : groupPhase === "running" ? (readyCount === total ? "success" : readyCount > 0 ? "warning" : "danger") : ["failed", "cleanup_failed", "restore_failed"].includes(groupPhase) ? "danger" : "neutral";

  return <section className="rounded-xl border border-sky-200 bg-white p-5 dark:border-sky-800 dark:bg-slate-900">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div><h3 className="font-semibold">{t("多号码常驻接收")}</h3>
        <p className="mt-1 text-sm text-gray-500">{t("每个号码保留独立的 VoWiFi 线路，收到短信后直接进入收件箱。")}</p>
      </div>
      <div className="flex items-center gap-2 text-sm">
        <StatusDot tone={headTone} size="sm" animated={headTone === "success"} />
        <span className={cx(headTone === "success" ? "text-green-600" : headTone === "danger" ? "text-red-600" : "text-gray-500")}>{headline}</span>
      </div>
    </div>
    <p className="mt-3 text-xs text-gray-500">{t("首版仅支持 EC20/EC25 AT 设备，配置上限为 8 个号码；实际并发容量仍需验证。启用前请停止本设备的自动任务。")}</p>
    {error ? <p className="mt-3 text-sm text-red-600" role="alert">{error}</p> : null}
    {status?.state.lastError ? <p className="mt-3 break-words text-sm text-red-600" role="alert">{status.state.lastError}</p> : null}
    {status && !status.available ? <p className="mt-3 text-sm text-amber-600">{t("多隧道运行服务不可用")}</p> : null}
    {status && status.config.enabled && !status.owned && !status.state.busy ? <p className="mt-3 text-sm text-amber-600">{t("配置已启用但设备尚未被接管：若长时间停留，请停止后重新启用。")}</p> : null}
    <div className="mt-4 grid gap-2 sm:grid-cols-2">
      {options.map((p) => <label key={p.iccid} className={`flex items-center gap-3 rounded-lg border p-3 text-sm ${locked ? "opacity-70" : "cursor-pointer"}`}>
        <input type="checkbox" checked={selected.some((item) => item.iccid === p.iccid)} disabled={locked || !supported}
          onChange={() => { dirty.current = true; setSelected((old) => old.some((item) => item.iccid === p.iccid) ? old.filter((item) => item.iccid !== p.iccid) : [...old, p]); }} />
        <span>{p.name || t("未命名号码")}<span className="ml-2 font-mono text-xs text-gray-400">…{p.iccid.slice(-6)}</span></span>
      </label>)}
    </div>
    {!options.length ? <p className="mt-3 text-sm text-gray-500">{t("读取下方 eSIM 列表后，可在这里选择号码。")}</p> : null}
    <div className="mt-4 flex flex-wrap items-center gap-3">
      {owned ? <Button variant="danger" loading={saving} onClick={() => void save(false)}>{t("停止多隧道")}</Button>
        : <Button variant="primary" loading={saving} disabled={!supported || !deviceOnline || !status?.available || selected.length < 2 || selected.length > 8 || Boolean(status?.state.busy)} onClick={() => void save(true)}>{t("启用多隧道")}</Button>}
      {!error && status ? <span className="text-sm text-gray-500">{t("可接收短信")}：{readyCount} / {total}</span> : null}
      {status?.state.updatedAt ? <span className="text-xs text-gray-400" title={formatDateTime(status.state.updatedAt)}>{t("组状态更新于")} {ageText(status.state.updatedAt, t) || "--"}{t("前")}</span> : null}
    </div>
    {lines.length > 0 ? <div className="mt-4 divide-y rounded-lg border">
      {lines.map((line) => {
        const s = line.state;
        const open = !!expanded[line.iccid];
        const ready = s.smsReady && !error;
        const tone = error ? "warning" : s.smsReady ? "success" : s.phase === "failed" ? "danger" : "warning";
        const since = s.smsReady ? ageText(s.updatedAt, t) : ageText(s.updatedAt, t);
        const reason = multiSIMReasonLabel(s.lastReason, t);
        return <div key={line.iccid} className="p-3 text-sm">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <StatusDot tone={tone} size="sm" animated={ready} />
                <span className="font-medium">{s.phoneNumber || line.name || t("号码待识别")}</span>
                {line.name && s.phoneNumber ? <span className="text-xs text-gray-500">{line.name}</span> : null}
                <span className="font-mono text-xs text-gray-400">…{line.iccid.slice(-6)}</span>
                {status?.activeProfileIccid === line.iccid ? <span className="rounded bg-sky-100 px-1.5 py-0.5 text-[10px] font-bold text-sky-700 dark:bg-sky-500/20 dark:text-sky-300">{t("卡片当前激活")}</span> : null}
              </div>
              <div className={cx("mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-xs", ready ? "text-green-600" : "text-gray-500")}>
                <span>{error ? t("等待状态确认") : s.smsReady ? t("可接收短信") : multiSIMLinePhaseLabel(s.phase, t)}</span>
                {since ? <span className="text-gray-500" title={formatDateTime(s.updatedAt)}>{s.smsReady ? t("已就绪") : t("停留")} {since}</span> : null}
                {s.imsRegistration ? <span className="text-gray-500">IMS: {s.imsRegistration}</span> : null}
                {s.proxyId ? <span className="text-gray-500">{t("代理")}: {s.proxyId}{s.proxyMode ? ` (${s.proxyMode})` : ""}</span> : <span className="text-gray-500">{t("代理")}: {t("直连")}</span>}
                <span className="text-gray-500" title={t("本线路第几次建立；每次自动重连加一")}>{tf("第 {n} 次建立", { n: s.attempt || 0 })}</span>
                {!s.smsReady && reason ? <span className="text-gray-500">{reason}</span> : null}
              </div>
              <div className="mt-2 flex max-w-xs gap-1">
                {PIPELINE.map((step) => <div key={step.label} className="flex-1" title={step.label}>
                  <div className={cx("h-1 rounded-full", s[step.key] === true ? "bg-emerald-500" : s.phase === "failed" ? "bg-red-400" : "bg-gray-200 dark:bg-white/10")} />
                  <div className="mt-0.5 text-center text-[9px] text-gray-400">{step.label}</div>
                </div>)}
              </div>
              {s.lastError ? <p className="mt-1 break-words text-xs text-red-600">{s.lastErrorClass ? <span className="font-mono">{s.lastErrorClass}: </span> : null}{s.lastError}</p> : null}
              {s.warnings?.length ? <p className="mt-1 break-words text-xs text-amber-600">{s.warnings.join("；")}</p> : null}
              {s.cleanupErrors?.length ? <p className="mt-1 break-words text-xs text-amber-600">{t("清理告警")}: {s.cleanupErrors.join("；")}</p> : null}
            </div>
            <div className="flex shrink-0 flex-wrap items-center gap-2">
              <Button size="small" plain loading={pending === `refresh:${line.iccid}`} disabled={!status?.config.enabled || saving || Boolean(status?.state.busy) || !s.imsReady} title={t("在现有隧道上做一次普通 re-REGISTER，不强制新鉴权")} onClick={() => void lineAction(line.iccid, "refresh")}>{t("IMS 续约")}</Button>
              <Button size="small" plain loading={pending === `reconnect:${line.iccid}`} disabled={!status?.config.enabled || saving || Boolean(status?.state.busy)} onClick={() => void lineAction(line.iccid, "reconnect")}>{t("重连此号码")}</Button>
              <button type="button" className="text-xs text-gray-400 hover:underline" onClick={() => setExpanded((old) => ({ ...old, [line.iccid]: !open }))}>{open ? t("收起") : t("详情")}</button>
            </div>
          </div>
          {open ? <div className="mt-3 grid gap-x-6 gap-y-0 rounded-lg bg-gray-50 p-3 text-xs dark:bg-white/5 sm:grid-cols-2">
            <Detail label={t("会话 ID")} value={line.sessionId} mono />
            <Detail label={t("隧道接口")} value={s.tunnelName} mono />
            <Detail label={t("数据平面")} value={s.dataplaneMode} mono />
            <Detail label="ePDG" value={s.epdg} mono />
            <Detail label={t("运营商配置")} value={s.carrierProfile ? `${s.carrierProfile} (${s.carrierProfileFrom || ""})` : ""} mono />
            <Detail label="HPLMN" value={s.homeMcc ? `${s.homeMcc}-${s.homeMnc}` : ""} mono />
            <Detail label="IMSI" value={s.imsi ? `…${s.imsi.slice(-4)}` : ""} mono />
            <Detail label={t("号码来源")} value={s.phoneNumberSource} mono />
            <Detail label="IKE" value={s.security ? `${s.security.ikeEncryption} / ${s.security.ikeIntegrity} / ${s.security.ikeDhGroup}` : ""} mono />
            <Detail label="ESP" value={s.security ? `${s.security.espEncryption} / ${s.security.espIntegrity}` : ""} mono />
            <Detail label={t("对端认证")} value={s.security?.responderAuth} mono />
            <Detail label={t("最后原因")} value={s.lastReason ? `${reason} (${s.lastReason})` : ""} />
            <Detail label={t("本次建立开始")} value={s.startedAt ? formatDateTime(s.startedAt) : ""} />
            <Detail label={t("状态更新于")} value={s.updatedAt ? formatDateTime(s.updatedAt) : ""} />
            <Detail label={t("状态序号")} value={s.sequence} mono />
          </div> : null}
        </div>;
      })}
    </div> : null}
    {owned ? <p className="mt-3 text-xs text-gray-500">{t("当前卡片激活的 Profile")}：<span className="font-mono">{status?.activeProfileIccid ? `…${status.activeProfileIccid.slice(-6)}` : t("正在确认")}</span>。{t("卡片激活状态与上方各号码的线路状态分别显示。")}{t("激活的 Profile 只在某条线路需要读卡鉴权时才切换，其余线路靠各自已建立的隧道继续收信。")}</p> : null}
  </section>;
}
