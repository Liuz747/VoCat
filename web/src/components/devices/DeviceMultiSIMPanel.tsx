import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, apiMessage } from "../../api";
import { Button, message } from "../ui";
import { useI18n } from "../../lib/i18n";
import type { EsimProfileGroup } from "./types";

interface Profile { iccid: string; aid: string; name?: string }
interface Line {
  iccid: string; name?: string; sessionId: string;
  state: { phase: string; smsReady: boolean; tunnelReady: boolean; imsReady: boolean;
    phoneNumber?: string; lastError?: string; lastErrorClass?: string; proxyId?: string };
}
interface MultiSIMStatus {
  available: boolean; owned: boolean; activeProfileIccid: string;
  config: { deviceId: string; enabled: boolean; profiles: Profile[] };
  state: { deviceId: string; enabled: boolean; busy: boolean; phase: string; lastError?: string; lines: Line[]; updatedAt?: string };
}
interface Props {
  deviceId: string; isActive: boolean; deviceOnline: boolean; supported: boolean;
  groups: EsimProfileGroup[]; onOwnedChange: (owned: boolean) => void;
}

export function DeviceMultiSIMPanel({ deviceId, isActive, deviceOnline, supported, groups, onOwnedChange }: Props) {
  const { t } = useI18n();
  const [status, setStatus] = useState<MultiSIMStatus | null>(null);
  const [selected, setSelected] = useState<Profile[]>([]);
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  const [reconnecting, setReconnecting] = useState<string | null>(null);
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
    return () => { controller.abort(); clearTimeout(timer); };
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
  const phases: Record<string, string> = {
    disabled: t("已停止"), idle: t("未启动"), pending: t("等待启动"), starting: t("正在建立线路"),
    preparing: t("正在接管设备"), stopping: t("正在停止并恢复设备"), ready: t("运行中"),
    active: t("运行中"), running: t("运行中"), restoring: t("正在恢复设备"), partial: t("部分线路可用"), failed: t("运行失败"),
    cleanup_failed: t("线路清理失败"), restore_failed: t("设备恢复失败"),
    sim_ready: t("SIM 已就绪"), access_ready: t("正在建立隧道"), tunnel_ready: t("隧道已建立"),
    ims_ready: t("IMS 已注册"), sms_ready: t("可接收短信"),
  };
  const phaseLabel = (phase: string) => phases[phase] || phase || t("等待启动");

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
  const reconnect = async (iccid: string) => {
    const id = deviceId; setReconnecting(iccid);
    try {
      const value = await api<MultiSIMStatus>(`/devices/${encodeURIComponent(id)}/multisim/lines/${encodeURIComponent(iccid)}/reconnect`, { method: "POST" });
      if (currentDevice.current === id) { applyStatus(value); message.success(t("已提交线路重连")); }
    } catch (err) { if (currentDevice.current === id) message.error(apiMessage(err) || t("线路重连失败")); }
    finally { if (currentDevice.current === id) setReconnecting(null); }
  };

  if (!supported && !status?.config.enabled && !status?.owned && !(status?.config.profiles.length)) return null;
  const readyCount = status?.state.lines.filter((line) => line.state.smsReady).length || 0;
  return <section className="rounded-xl border border-sky-200 bg-white p-5 dark:border-sky-800 dark:bg-slate-900">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div><h3 className="font-semibold">{t("多号码常驻接收")}</h3>
        <p className="mt-1 text-sm text-gray-500">{t("每个号码保留独立的 VoWiFi 线路，收到短信后直接进入收件箱。")}</p>
      </div>
      <span className="text-sm text-gray-500">{error ? t("状态更新失败") : phaseLabel(status?.state.phase || "pending")}</span>
    </div>
    <p className="mt-3 text-xs text-gray-500">{t("首版仅支持 EC20/EC25 AT 设备，配置上限为 8 个号码；实际并发容量仍需验证。启用前请停止本设备的自动任务。")}</p>
    {error ? <p className="mt-3 text-sm text-red-600" role="alert">{error}</p> : null}
    {status?.state.lastError ? <p className="mt-3 break-words text-sm text-red-600" role="alert">{status.state.lastError}</p> : null}
    {status && !status.available ? <p className="mt-3 text-sm text-amber-600">{t("多隧道运行服务不可用")}</p> : null}
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
      {!error && status ? <span className="text-sm text-gray-500">{t("可接收短信")}：{readyCount} / {status.config.profiles.length}</span> : null}
    </div>
    {(status?.state.lines.length || 0) > 0 ? <div className="mt-4 divide-y rounded-lg border">
      {status!.state.lines.map((line) => <div key={line.iccid} className="flex flex-wrap items-center justify-between gap-3 p-3 text-sm">
        <div className="min-w-0"><div className="font-medium">{line.state.phoneNumber || line.name || t("号码待识别")} <span className="font-mono text-xs text-gray-400">…{line.iccid.slice(-6)}</span></div>
          <div className={`mt-1 ${line.state.smsReady && !error ? "text-green-600" : "text-gray-500"}`}>{error ? t("等待状态确认") : line.state.smsReady ? t("可接收短信") : phaseLabel(line.state.phase)}</div>
          {line.state.lastError ? <p className="mt-1 break-words text-xs text-red-600">{line.state.lastError}</p> : null}
        </div>
        <Button size="small" plain loading={reconnecting === line.iccid} disabled={!status?.config.enabled || saving || Boolean(status?.state.busy)} onClick={() => void reconnect(line.iccid)}>{t("重连此号码")}</Button>
      </div>)}
    </div> : null}
    {owned ? <p className="mt-3 text-xs text-gray-500">{t("当前卡片激活的 Profile")}：<span className="font-mono">{status?.activeProfileIccid ? `…${status.activeProfileIccid.slice(-6)}` : t("正在确认")}</span>。{t("卡片激活状态与上方各号码的线路状态分别显示。")}</p> : null}
  </section>;
}
