import { useCallback, useEffect, useRef, useState } from "react";
import { ArrowSyncRegular, PowerRegular, WarningRegular } from "@fluentui/react-icons";
import { api, apiMessage } from "../../api";
import { Button, confirmDialog, message } from "../ui";
import { readEventStream, useShowSensitive } from "./shared";
import { EsimLoadingHero } from "./EsimLoadingHero";
import { DeviceMultiSIMPanel } from "./DeviceMultiSIMPanel";
import { EsimChipHeader } from "./EsimChipHeader";
import { EsimEuiccGroup, type SpaceNotice } from "./EsimEuiccGroup";
import { EsimNotificationsModal } from "./EsimNotificationsModal";
import { EsimDownloadForm } from "./EsimDownloadForm";
import { DeleteProfileModal, type DeleteProfileTarget } from "./DeleteProfileModal";
import { EmptyState } from "../ui";
import type { EsimChipInfo, EsimDownloadForm as EsimDownloadFormData, EsimNotification, EsimProfileGroup } from "./types";
import { tf, tl, useI18n } from "../../lib/i18n";

export interface DeviceEsimTabProps {
  deviceId: string;
  deviceImei: string;
  isActive: boolean;
  deviceOnline: boolean;
  deviceType?: string;
  backendMode?: string;
  rebooting?: boolean;
  onRebootModem?: () => Promise<boolean>;
  onProfileChanged?: () => void;
}

interface EsimLoadFailure {
  code: string;
  message: string;
  channelStuck: boolean;
}

function isEUICCChannelStuck(code: string, detail: string): boolean {
  if (code === "euicc_channel_stuck") return true;
  const normalized = detail.toUpperCase();
  return normalized.includes("+CME ERROR: 0") &&
    (normalized.includes("0070000001") || normalized.includes("01A4040010A0000005591010"));
}

function normAid(aid?: string): string {
  return (aid || "").trim().toUpperCase();
}
function fmtSpace(bytes?: number): string {
  if (!bytes || !Number.isFinite(bytes) || bytes <= 0) return "";
  if (bytes >= 1024 * 1024) return `${Math.round((bytes / (1024 * 1024)) * 100) / 100} MB`;
  if (bytes >= 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${Math.round(bytes)} B`;
}
function spaceDeltaText(delta?: { bytes?: number; direction?: string }): string {
  if (!delta) return "";
  const s = fmtSpace(delta.bytes ?? 0);
  if (!s) return "";
  return delta.direction === "releasedt(" ? `刚刚释放约 ${s}` : delta.direction === ")consumedt(" ? `刚刚占用约 ${s}` : ")";
}
function selectDefaultAid(chip: EsimChipInfo | null, preferAid?: string): string {
  const eids = chip?.eids || [];
  if (eids.length === 0) return "";
  if (preferAid && eids.some((e) => e.aid === preferAid)) return preferAid;
  return eids[0]?.aid || "";
}
function applyDisableLocal(groups: EsimProfileGroup[], iccid: string, aidHex?: string): EsimProfileGroup[] {
  const clean = iccid.replace(/\s+/g, "");
  const aid = normAid(aidHex);
  return groups.map((g) => ({
    ...g,
    profiles: g.profiles.map((p) => {
      const inGroup = !aid || normAid(g.aidHex) === aid;
      const match = inGroup && p.iccid.replace(/\s+/g, "") === clean;
      return match ? { ...p, state: 0, stateText: tl("已禁用") } : p;
    }),
  }));
}

export function DeviceEsimTab({ deviceId, deviceImei, isActive, deviceOnline, deviceType, rebooting, onRebootModem, onProfileChanged }: DeviceEsimTabProps) {
  const { t } = useI18n();
  const [initialLoading, setInitialLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [chipInfo, setChipInfo] = useState<EsimChipInfo | null>(null);
  const [groups, setGroups] = useState<EsimProfileGroup[]>([]);
  const [multiSIMOwned, setMultiSIMOwned] = useState(false);
  const [switchingIccid, setSwitchingIccid] = useState<string | null>(null);
  const [deletingIccid, setDeletingIccid] = useState<string | null>(null);
  const [renamingIccid, setRenamingIccid] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [policyIccid, setPolicyIccid] = useState<string | null>(null);
  const [showSensitive, toggleSensitive] = useShowSensitive();
  const [notifications, setNotifications] = useState<EsimNotification[]>([]);
  const [notificationsLoading, setNotificationsLoading] = useState(false);
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [retryingSeq, setRetryingSeq] = useState<number | null>(null);
  const [form, setForm] = useState<EsimDownloadFormData>({ smdp: "", matchingId: "", confirmationCode: "", aidHex: "" });
  const [downloading, setDownloading] = useState(false);
  const [downloadPct, setDownloadPct] = useState(0);
  const [downloadMsg, setDownloadMsg] = useState("");
  const [downloadErr, setDownloadErr] = useState("");
  const [spaceNotice, setSpaceNotice] = useState<SpaceNotice | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DeleteProfileTarget | null>(null);
  const [deleteInput, setDeleteInput] = useState("");
  const [loadFailure, setLoadFailure] = useState<EsimLoadFailure | null>(null);
  const overviewAbort = useRef<AbortController | null>(null);
  const downloadAbort = useRef<AbortController | null>(null);
  const spaceTimer = useRef<number | null>(null);
  const recoveryTimers = useRef<number[]>([]);
  const loadSeq = useRef(0);
  const formRef = useRef(form);
  formRef.current = form;
  const chipRef = useRef(chipInfo);
  chipRef.current = chipInfo;
  const showSpaceNotice = useCallback((aidHex: string | undefined, delta?: { bytes?: number; direction?: string }) => {
    const aid = normAid(aidHex);
    const msg = spaceDeltaText(delta);
    if (!aid || !msg) return;
    if (spaceTimer.current !== null) {
      window.clearTimeout(spaceTimer.current);
      spaceTimer.current = null;
    }
    setSpaceNotice({ aidHex: aid, message: msg });
    spaceTimer.current = window.setTimeout(() => {
      setSpaceNotice(null);
      spaceTimer.current = null;
    }, 75000);
  }, []);

  const profileChangedRef = useRef(onProfileChanged);
  profileChangedRef.current = onProfileChanged;
  const notifyProfileChanged = useCallback(() => {
    profileChangedRef.current?.();
  }, []);

  const loadOverview = useCallback(
    async (refresh = false, quiet = false) => {
      loadSeq.current += 1;
      const seq = loadSeq.current;
      overviewAbort.current?.abort();
      const controller = new AbortController();
      overviewAbort.current = controller;
      if (refresh) setRefreshing(true);
      else setInitialLoading(true);
      try {
        const data = await api<{ chipInfo?: EsimChipInfo; profiles?: EsimProfileGroup[] }>(
          `/devices/${deviceId}/esim${refresh ? "?refresh=true" : ""}`,
          { signal: controller.signal },
        );
        if (seq !== loadSeq.current) return false;
        setChipInfo(data?.chipInfo || null);
        setGroups(data?.profiles || []);
        setLoadFailure(null);
        for (const timer of recoveryTimers.current) window.clearTimeout(timer);
        recoveryTimers.current = [];
        setForm((prev) => ({ ...prev, aidHex: selectDefaultAid(data?.chipInfo || null, prev.aidHex) }));
        return true;
      } catch (e) {
        if (controller.signal.aborted) return false;
        const detail = apiMessage(e) || t("获取 eSIM 信息失败");
        const code = String((e as { code?: string })?.code || "");
        const channelStuck = isEUICCChannelStuck(code, detail);
        setLoadFailure({ code, message: detail, channelStuck });
        if (code === "multisim_active") setMultiSIMOwned(true);
        if (!quiet && !channelStuck && code !== "multisim_active") message.error(detail);
        return false;
      } finally {
        if (seq === loadSeq.current) {
          if (refresh) setRefreshing(false);
          else setInitialLoading(false);
        }
      }
    },
    [deviceId],
  );

  const rebootForChannelRecovery = useCallback(async () => {
    if (!onRebootModem || rebooting) return;
    const accepted = await onRebootModem();
    if (!accepted) return;
    for (const timer of recoveryTimers.current) window.clearTimeout(timer);
    recoveryTimers.current = [8000, 16000, 30000].map((delay) =>
      window.setTimeout(() => {
        if (isActive) void loadOverview(true, true);
      }, delay),
    );
  }, [isActive, loadOverview, onRebootModem, rebooting]);

  const loadNotifications = useCallback(async () => {
    setNotificationsLoading(true);
    try {
      const data = await api<{ items?: EsimNotification[] }>(`/devices/${deviceId}/esim/notifications`);
      setNotifications(data?.items || []);
    } catch (e) {
      message.error(apiMessage(e) || t("获取当前通知列表失败"));
    } finally {
      setNotificationsLoading(false);
    }
  }, [deviceId]);

  const openNotifications = useCallback(async () => {
    setNotificationsOpen(true);
    await loadNotifications();
  }, [loadNotifications]);

  const retryNotification = useCallback(
    async (item: EsimNotification) => {
      if (!item.canRetry || retryingSeq !== null) return;
      setRetryingSeq(item.sequenceNumber);
      try {
        const res = await api<{ status?: string; message?: string }>(
          `/devices/${deviceId}/esim/notifications/${item.sequenceNumber}/actions/retry${item.aidHex ? `?aid_hex=${encodeURIComponent(item.aidHex)}` : ""}`,
          { method: "POST" },
        );
        message.success(res?.message || t("通知重试发送成功"));
        await loadNotifications();
      } catch (e) {
        message.error(apiMessage(e) || t("通知重试发送失败"));
      } finally {
        setRetryingSeq(null);
      }
    },
    [deviceId, retryingSeq, loadNotifications],
  );
  const loadProfiles = useCallback(
    async (refresh = false) => {
      setRefreshing(true);
      try {
        const data = await api<EsimProfileGroup[]>(`/devices/${deviceId}/esim/profiles${refresh ? "?refresh=true" : ""}`);
        const next = Array.isArray(data) ? data : [];
        setGroups(next);
        return next;
      } catch (e) {
        message.error(apiMessage(e) || t("获取 eSIM Profiles 失败"));
        return null;
      } finally {
        setRefreshing(false);
      }
    },
    [deviceId],
  );

  const switchProfile = useCallback(
    async (iccid: string, state: number | undefined, aidHex?: string) => {
      const disabling = state === 1;
      const action = disabling ? t("禁用") : t("启用");
      const prompt = disabling
        ? tf("确定禁用当前 Profile ({iccid}) 吗？禁用后此 eUICC 将没有活动号码，蜂窝网络、VoWiFi 和短信都会中断，直到重新启用或下载其他 Profile。若 Profile 策略要求禁用即删除，eUICC 可能在禁用时直接删除它。", { iccid })
        : tf("确定启用此 Profile ({iccid}) 吗？切换后设备会短暂断网。", { iccid });
      const ok = await confirmDialog(prompt, tf("{action} Profile", { action }), {
        confirmText: action,
        cancelText: t("取消"),
        type: "warning",
      });
      if (!ok) return;
      setSwitchingIccid(iccid);
      try {
        const endpoint = disabling ? "disable" : "switch";
        const result = await api<{ verified?: boolean }>(`/devices/${deviceId}/esim/actions/${endpoint}`, {
          method: "POST",
          body: { iccid, aidHex },
        });
        if (disabling) {
          message.success(t("Profile 已禁用；模组正在重新初始化，恢复后即可删除"));
          setGroups((prev) => applyDisableLocal(prev, iccid, aidHex));
        } else {
          if (result?.verified !== true) {
            throw new Error("后端未确认目标 ICCID 已在模组中生效");
          }
          const refreshed = await loadProfiles(true);
          const cleanICCID = iccid.replace(/\s+/g, "");
          const targetAID = normAid(aidHex);
          const confirmed = refreshed?.some(
            (group) =>
              (!targetAID || normAid(group.aidHex) === targetAID) &&
              group.profiles.some(
                (profile) => profile.iccid.replace(/\s+/g, "") === cleanICCID && profile.state === 1,
              ),
          );
          if (!confirmed) {
            throw new Error("模组 ICCID 已切换，但重新读取 eUICC 后没有确认目标 Profile 为已启用状态");
          }
          message.success(t("Profile 启用成功"));
        }
        notifyProfileChanged();
      } catch (e) {
        message.error(apiMessage(e) || tf("{action}失败", { action }));
      } finally {
        setSwitchingIccid(null);
      }
    },
    [deviceId, notifyProfileChanged, loadProfiles],
  );

  const startRename = useCallback((iccid: string, name?: string) => {
    setRenamingIccid(iccid);
    setRenameValue(name || "");
  }, []);
  const cancelRename = useCallback(() => {
    setRenamingIccid(null);
    setRenameValue("");
  }, []);
  const submitRename = useCallback(
    async (iccid: string, aidHex?: string) => {
      const name = renameValue.trim();
      if (!name) {
        message.warning(t("名称不能为空"));
        return;
      }
      try {
        await api(`/devices/${deviceId}/esim/profiles/${encodeURIComponent(iccid)}`, { method: "PATCH", body: { name, aidHex } });
        message.success(t("名称修改成功"));
        setRenamingIccid(null);
        await loadProfiles(true);
      } catch (e) {
        message.error(apiMessage(e) || t("修改名称失败"));
      }
    },
    [deviceId, renameValue, loadProfiles],
  );

  const openDelete = useCallback((iccid: string, name?: string, aidHex?: string) => {
    setDeleteTarget({ iccid, name, aidHex });
    setDeleteInput("");
  }, []);
  const confirmDelete = useCallback(async () => {
    if (!deleteTarget) return;
    const { iccid, aidHex } = deleteTarget;
    setDeletingIccid(iccid);
    try {
      const res = await api<{ warning?: string; spaceDelta?: { bytes?: number; direction?: string } }>(
        `/devices/${deviceId}/esim/profiles/${encodeURIComponent(iccid)}${aidHex ? `?aid_hex=${encodeURIComponent(aidHex)}` : ""}`,
        { method: "DELETE" },
      );
      showSpaceNotice(aidHex, res?.spaceDelta);
      if (res?.warning) message.warning(res.warning);
      else {
        const s = spaceDeltaText(res?.spaceDelta);
        message.success(s ? tf("Profile 删除成功，{s}", { s }) : t("Profile 删除成功"));
      }
      setDeleteTarget(null);
      await loadOverview(true);
      notifyProfileChanged();
    } catch (e) {
      message.error(apiMessage(e) || t("删除失败"));
    } finally {
      setDeletingIccid(null);
    }
  }, [deviceId, deleteTarget, loadOverview, showSpaceNotice, notifyProfileChanged]);
  const download = useCallback(async () => {
    const { smdp, matchingId, confirmationCode, aidHex } = formRef.current;
    const aid = aidHex || selectDefaultAid(chipRef.current, "");
    const imei = (deviceImei || "").trim();
    if (!smdp) {
      message.warning(t("请输入 SM-DP+ 地址"));
      return;
    }
    setDownloading(true);
    setDownloadPct(0);
    setDownloadMsg(t("正在连接..."));
    setDownloadErr("");
    const controller = new AbortController();
    downloadAbort.current = controller;
    try {
      await readEventStream(
        `/devices/${deviceId}/esim/actions/download`,
        {
          smdp,
          matching_id: matchingId || undefined,
          confirmation_code: confirmationCode || undefined,
          aid_hex: aid || undefined,
          imei: imei || undefined,
        },
        {
          signal: controller.signal,
          onData: (line) => {
            try {
              const ev = JSON.parse(line);
              if (ev.step === "error") {
                const friendlyErrors: Record<string, string> = {
                  euicc_insufficient_memory: t("eUICC 安装 profile 时空间不足，请删除未使用的 profile 后重试。"),
                  euicc_ci_incompatible: t("此 SM-DP+ 的证书链不受当前 eUICC 信任；该卡不能使用此测试服务器。"),
                  activation_code_refused: t("激活码已被使用、已过期或被 SM-DP+ 拒绝，请更换新的 Matching ID。"),
                  profile_pool_empty: t("SM-DP+ 的公开 Profile 库存已耗尽，请稍后重试或更换服务。"),
                };
                setDownloadErr(friendlyErrors[ev.code] || ev.msg || t("下载失败"));
                return;
              }
              if (typeof ev.pct === "number") setDownloadPct(ev.pct);
              if (ev.msg) setDownloadMsg(ev.msg);
              if (ev.step === "done") {
                showSpaceNotice(aid, ev.space_delta);
                const s = spaceDeltaText(ev.space_delta);
                if (ev.warning) message.warning(ev.warning);
                else message.success(s ? tf("Profile 下载成功，{s}", { s }) : t("Profile 下载成功"));
                setForm({ smdp: "", matchingId: "", confirmationCode: "", aidHex: aid });
                loadOverview(true);
                notifyProfileChanged();
              }
            } catch {
              /* ignore */
            }
          },
        },
      );
    } catch (e) {
      if (!controller.signal.aborted) setDownloadErr((prev) => prev || apiMessage(e) || t("下载失败"));
    } finally {
      setDownloading(false);
    }
  }, [deviceId, deviceImei, loadOverview, showSpaceNotice, notifyProfileChanged]);

  // Load when the tab becomes active for a device.
  useEffect(() => {
    overviewAbort.current?.abort();
    for (const timer of recoveryTimers.current) window.clearTimeout(timer);
    recoveryTimers.current = [];
    setPolicyIccid(null);
    if (!deviceId || !isActive) return;
    if (spaceTimer.current !== null) {
      window.clearTimeout(spaceTimer.current);
      spaceTimer.current = null;
    }
    setSpaceNotice(null);
    setLoadFailure(null);
    setChipInfo(null);
    setGroups([]);
    setForm((prev) => ({ ...prev, aidHex: "" }));
    loadOverview(false);
  }, [deviceId, isActive, loadOverview]);

  // Auto-parse full LPA activation codes / strip http(s) prefixes in SM-DP+.
  useEffect(() => {
    const value = form.smdp;
    if (!value) return;
    if (value.startsWith("LPA:")) {
      const parts = value.split("$");
      if (parts.length >= 3) {
        setForm((prev) => ({ ...prev, smdp: parts[1], matchingId: parts[2] }));
        message.success(t("已自动解析完整的 LPA 激活码"));
      }
    } else if (value.startsWith("http://") || value.startsWith("https://")) {
      setForm((prev) => ({ ...prev, smdp: value.replace(/^https?:\/\//i, "") }));
    }
  }, [form.smdp]);

  // Cleanup on unmount.
  useEffect(
    () => () => {
      overviewAbort.current?.abort();
      downloadAbort.current?.abort();
      if (spaceTimer.current !== null) window.clearTimeout(spaceTimer.current);
      for (const timer of recoveryTimers.current) window.clearTimeout(timer);
      recoveryTimers.current = [];
    },
    [],
  );
  const multiSIMPanel = <DeviceMultiSIMPanel deviceId={deviceId} isActive={isActive} deviceOnline={deviceOnline}
    supported={deviceType === "pcie_ec20_ec25"} groups={groups} onOwnedChange={setMultiSIMOwned} />;
  if (initialLoading) {
    return (
      <div className="space-y-5">
        {multiSIMPanel}
        <EsimLoadingHero />
      </div>
    );
  }
  return (
    <div className="space-y-5">
      {multiSIMPanel}
      {multiSIMOwned ? <div className="rounded-xl border p-4 text-sm text-gray-500">{t("多隧道运行中暂停读卡和卡片操作。请先停止多隧道，再刷新或编辑 eSIM 列表。")}</div> : <>
      {!chipInfo ? <Button loading={refreshing} disabled={!deviceOnline} onClick={() => void loadOverview(true)} icon={<ArrowSyncRegular />}>{t("读取 eSIM 列表")}</Button> : null}
      {loadFailure?.channelStuck ? (
        <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-amber-900 dark:border-amber-500/25 dark:bg-amber-500/10 dark:text-amber-100">
          <div className="flex items-start gap-3">
            <WarningRegular className="mt-0.5 shrink-0 text-xl text-amber-600 dark:text-amber-300" />
            <div className="min-w-0 flex-1">
              <div className="font-bold">{t("热插拔后 eSIM 通道未恢复")}</div>
              <div className="mt-1 text-sm leading-6 text-amber-800 dark:text-amber-200">
                {t("EC20 已检测到新 SIM，但基带的 UIM/APDU 逻辑通道仍卡在旧卡状态。普通刷新只会重复失败，需要重启模组重新初始化卡通道；不会删除、下载或切换任何 Profile。")}
              </div>
              <div className="mt-2 break-all rounded-lg bg-white/60 px-3 py-2 font-mono text-xs text-amber-700 dark:bg-black/10 dark:text-amber-200">
                {loadFailure.message}
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <Button variant="primary" loading={!!rebooting} onClick={rebootForChannelRecovery} icon={<PowerRegular />}>
                  {t("重启模组并自动复检")}
                </Button>
                <Button loading={refreshing} onClick={() => void loadOverview(true)} icon={<ArrowSyncRegular />}>
                  {t("重新检测")}
                </Button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {chipInfo ? (
        <EsimChipHeader
          chipInfo={chipInfo}
          showSensitive={showSensitive}
          refreshing={refreshing}
          notificationsLoading={notificationsLoading}
          onRefresh={() => loadOverview(true)}
          onOpenNotifications={openNotifications}
          onToggleSensitive={toggleSensitive}
        />
      ) : null}
      {groups.map((g, i) => (
        <EsimEuiccGroup
          key={g.aidHex || g.eid || `group-${i}`}
          deviceId={deviceId}
          deviceOnline={deviceOnline}
          group={g}
          index={i}
          chipInfo={chipInfo}
          showSensitive={showSensitive}
          spaceNotice={spaceNotice}
          renamingIccid={renamingIccid}
          renameValue={renameValue}
          switchingIccid={switchingIccid}
          deletingIccid={deletingIccid}
          policyIccid={policyIccid}
          onRenameValueChange={setRenameValue}
          onSwitch={switchProfile}
          onStartRename={startRename}
          onSubmitRename={submitRename}
          onCancelRename={cancelRename}
          onTogglePolicy={(iccid) => setPolicyIccid((prev) => (prev === iccid ? null : iccid))}
          onDelete={openDelete}
          onPolicyChanged={() => loadOverview(true)}
        />
      ))}
      <EsimNotificationsModal
        open={notificationsOpen}
        loading={notificationsLoading}
        items={notifications}
        retryingSeq={retryingSeq}
        onClose={() => setNotificationsOpen(false)}
        onRetry={retryNotification}
      />
      {chipInfo ? (
        <EsimDownloadForm
          form={form}
          chipInfo={chipInfo}
          downloading={downloading}
          downloadPct={downloadPct}
          downloadMsg={downloadMsg}
          downloadErr={downloadErr}
          onFormChange={setForm}
          onDownload={download}
        />
      ) : null}
      {groups.length === 0 && !chipInfo && !loadFailure?.channelStuck ? (
        <EmptyState title={t("未检测到 eUICC")} subtitle={t("此SIM卡可能不支持 eUICC 功能")} />
      ) : null}
      <DeleteProfileModal
        open={!!deleteTarget}
        target={deleteTarget}
        deleting={!!deletingIccid}
        input={deleteInput}
        onInputChange={setDeleteInput}
        onCancel={() => setDeleteTarget(null)}
        onConfirm={confirmDelete}
      />
      </>}
    </div>
  );
}
