import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { CallEndRegular, CallInboundRegular, CallOutboundRegular, CallRegular, MicRegular } from "@fluentui/react-icons";
import { apiMessage } from "../api";
import { Button, EmptyState, ErrorState, Input, PageHeader, RefreshButton, Select, Switch, Tag, message } from "../components/ui";
import { usePolling } from "../lib/usePolling";
import { formatTime } from "../lib/utils";
import { tf, useI18n } from "../lib/i18n";
import type { DeviceListItem } from "../types";
import {
  answerCall,
  callLineOptions,
  callMediaSocketURL,
  dialCall,
  getCalls,
  hangupCall,
  isLiveCall,
  listCallDevices,
  type CallItem,
  type CallsSnapshot,
} from "../components/calls/callsApi";
import { CallAudioBridge, type BridgeStatus } from "../components/calls/audioBridge";

const STATE_TONE: Record<string, "success" | "danger" | "warning" | "info" | "primary"> = {
  active: "success",
  ringing: "warning",
  dialing: "primary",
  held: "info",
  waiting: "warning",
  ended: "info",
  failed: "danger",
};

const STATE_LABEL: Record<string, string> = {
  active: "通话中",
  ringing: "振铃",
  dialing: "呼出中",
  held: "保持",
  waiting: "等待",
  ended: "已结束",
  failed: "失败",
  unknown: "未知",
};

const LIVE_PRIORITY: Record<string, number> = { active: 0, ringing: 1, dialing: 2, held: 3, waiting: 4 };

function FormItem({ label, children }: { label: ReactNode; children: ReactNode }) {
  return (
    <div>
      <label className="mb-1.5 block text-sm font-medium text-gray-700 dark:text-gray-300">{label}</label>
      {children}
    </div>
  );
}

/** The call the panel should focus on: a live one first, otherwise the latest verdict. */
function pickCurrent(calls: CallItem[]): CallItem | null {
  const live = calls.filter(isLiveCall).sort((a, b) => (LIVE_PRIORITY[a.state] ?? 9) - (LIVE_PRIORITY[b.state] ?? 9));
  if (live.length) return live[0];
  const done = [...calls].sort((a, b) => (b.startedAt || "").localeCompare(a.startedAt || ""));
  return done[0] || null;
}

function sipVerdict(call: CallItem): string {
  const parts: string[] = [];
  if (call.sipCode) parts.push(`SIP ${call.sipCode}`);
  if (call.reason) parts.push(call.reason);
  return parts.join(" · ");
}

export default function CallsPage() {
  const { t } = useI18n();
  const [devices, setDevices] = useState<DeviceListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [line, setLine] = useState("");
  const [number, setNumber] = useState("");
  const [snapshot, setSnapshot] = useState<CallsSnapshot | null>(null);
  const [callsError, setCallsError] = useState("");
  const [busy, setBusy] = useState("");
  const [audioEnabled, setAudioEnabled] = useState(true);
  const [audioStatus, setAudioStatus] = useState<BridgeStatus>("idle");
  const [audioDetail, setAudioDetail] = useState("");
  const bridgeRef = useRef<CallAudioBridge | null>(null);
  const boundCallRef = useRef("");
  const lastConnectRef = useRef(0);

  const options = useMemo(() => callLineOptions(devices, t("未就绪"), t("蜂窝")), [devices, t]);
  const selected = useMemo(() => options.find((o) => o.value === line), [options, line]);
  const selectOptions = useMemo(
    () => options.map((o) => ({ value: o.value, label: o.label, disabled: o.disabled })),
    [options],
  );

  const loadDevices = useCallback(async (initial = false) => {
    if (initial) setLoading(true);
    try {
      const list = await listCallDevices();
      setDevices(list);
      setLoadError("");
    } catch (error) {
      setLoadError(apiMessage(error));
    } finally {
      if (initial) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadDevices(true);
  }, [loadDevices]);
  usePolling(() => void loadDevices(), 10000, false);

  // Keep a valid selection: first ready line, otherwise the first line.
  useEffect(() => {
    if (line && options.some((o) => o.value === line)) return;
    const first = options.find((o) => !o.disabled) || options[0];
    setLine(first?.value || "");
  }, [options, line]);

  const loadCalls = useCallback(async () => {
    if (!selected) {
      setSnapshot(null);
      return;
    }
    try {
      const next = await getCalls(selected.deviceId);
      setSnapshot(next);
      setCallsError("");
    } catch (error) {
      setCallsError(apiMessage(error));
    }
  }, [selected]);

  useEffect(() => {
    void loadCalls();
  }, [loadCalls]);
  usePolling(() => void loadCalls(), 2000, false);

  const lineCalls = useMemo(() => {
    const calls = snapshot?.calls || [];
    if (!snapshot?.multisim || !selected?.sessionId) return calls;
    return calls.filter((c) => c.sessionId === selected.sessionId);
  }, [snapshot, selected]);
  const otherRinging = useMemo(() => {
    if (!snapshot?.multisim || !selected?.sessionId) return [];
    return (snapshot.calls || []).filter(
      (c) => c.sessionId !== selected.sessionId && c.direction === "incoming" && isLiveCall(c),
    );
  }, [snapshot, selected]);
  const current = useMemo(() => pickCurrent(lineCalls), [lineCalls]);
  const history = useMemo(
    () => [...lineCalls].sort((a, b) => (b.startedAt || "").localeCompare(a.startedAt || "")),
    [lineCalls],
  );

  const bridge = useCallback(() => {
    if (!bridgeRef.current) {
      bridgeRef.current = new CallAudioBridge({
        onStatus: (status, detail) => {
          setAudioStatus(status);
          setAudioDetail(detail || "");
        },
      });
    }
    return bridgeRef.current;
  }, []);

  // Must run inside the click handler so the browser grants the microphone.
  const prepareAudio = useCallback(async () => {
    if (!audioEnabled) return;
    try {
      await bridge().prepare();
    } catch (error) {
      message.warning(tf("浏览器音频不可用：{reason}", { reason: error instanceof Error ? error.message : String(error) }));
    }
  }, [audioEnabled, bridge]);

  // Bind the PCM socket once the call is active and its media is negotiated
  // (the server refuses earlier); release it when the call is over. A socket
  // that dropped while the call is still up is retried, at most once per poll.
  useEffect(() => {
    const instance = bridgeRef.current;
    if (!instance || !audioEnabled || !selected) return;
    const target =
      current && current.state === "active" && current.mediaReady && snapshot?.transport === "vowifi" ? current : null;
    if (target) {
      const dropped = instance.state === "closed" || instance.state === "error";
      const stale = Date.now() - lastConnectRef.current > 2000;
      if (instance.state !== "idle" && (boundCallRef.current !== target.id || (dropped && stale))) {
        boundCallRef.current = target.id;
        lastConnectRef.current = Date.now();
        try {
          instance.connect(callMediaSocketURL(selected.deviceId, target.id, target.sessionId || selected.sessionId));
        } catch (error) {
          setAudioDetail(error instanceof Error ? error.message : String(error));
        }
      }
      return;
    }
    if (boundCallRef.current) {
      boundCallRef.current = "";
      instance.hangup();
    }
  }, [current, audioEnabled, selected, snapshot?.transport]);

  useEffect(() => () => bridgeRef.current?.stop(), []);

  const toggleAudio = useCallback((next: boolean) => {
    setAudioEnabled(next);
    if (!next) {
      boundCallRef.current = "";
      bridgeRef.current?.stop();
    }
  }, []);

  const onDial = useCallback(async () => {
    if (!selected || !number.trim()) return;
    setBusy("dial");
    try {
      await prepareAudio();
      const result = await dialCall(selected.deviceId, number.trim(), selected.sessionId);
      message.success(tf("已发起呼叫 {number}", { number: number.trim() }) + (result.transport === "vowifi" ? "" : ` · ${t("蜂窝")}`));
      await loadCalls();
    } catch (error) {
      message.error(apiMessage(error));
    } finally {
      setBusy("");
    }
  }, [selected, number, prepareAudio, loadCalls, t]);

  const onAnswer = useCallback(
    async (call: CallItem) => {
      if (!selected) return;
      setBusy(`answer:${call.id}`);
      try {
        await prepareAudio();
        await answerCall(selected.deviceId, call.id, call.sessionId || selected.sessionId);
        if (call.sessionId && call.sessionId !== selected.sessionId) setLine(`${selected.deviceId}|${call.sessionId}`);
        await loadCalls();
      } catch (error) {
        message.error(apiMessage(error));
      } finally {
        setBusy("");
      }
    },
    [selected, prepareAudio, loadCalls],
  );

  const onHangup = useCallback(
    async (call: CallItem) => {
      if (!selected) return;
      setBusy(`hangup:${call.id}`);
      try {
        await hangupCall(selected.deviceId, call.id, call.sessionId || selected.sessionId);
        boundCallRef.current = "";
        bridgeRef.current?.hangup();
        await loadCalls();
      } catch (error) {
        message.error(apiMessage(error));
      } finally {
        setBusy("");
      }
    },
    [selected, loadCalls],
  );

  const audioText = useMemo(() => {
    const map: Record<BridgeStatus, string> = {
      idle: "未启用",
      ready: "麦克风就绪，等待接通",
      connecting: "正在连接音频",
      connected: "音频已接通",
      closed: "音频已断开",
      error: "音频出错",
    };
    return `${t(map[audioStatus])}${audioDetail ? ` (${audioDetail})` : ""}`;
  }, [audioStatus, audioDetail, t]);

  const renderStateTag = (call: CallItem) => (
    <Tag type={STATE_TONE[call.state] || "info"}>{t(STATE_LABEL[call.state] || call.state)}</Tag>
  );

  const canDial = Boolean(selected) && number.trim().length > 0 && !lineCalls.some(isLiveCall);

  return (
    <div>
      <PageHeader
        title={t("通话")}
        subtitle={t("通过线路的 IMS 会话拨打、接听和挂断电话；SIP 状态码和原因原样显示。")}
        actions={<RefreshButton loading={loading} onClick={() => void Promise.all([loadDevices(), loadCalls()])} />}
      />

      {loadError ? (
        <ErrorState title={t("设备列表加载失败")} message={loadError} onRetry={() => void loadDevices(true)} className="mb-6" />
      ) : null}

      <div className="grid gap-6 lg:grid-cols-[minmax(0,400px)_minmax(0,1fr)]">
        <div className="ui-card space-y-4 p-5">
          <h3 className="text-base font-semibold text-gray-900 dark:text-white">{t("拨号")}</h3>
          <FormItem label={t("线路")}>
            <Select value={line} onChange={setLine} options={selectOptions} placeholder={t("选择线路")} />
          </FormItem>
          <FormItem label={t("目标号码")}>
            <Input
              value={number}
              onChange={(e) => setNumber(e.target.value)}
              placeholder="+12025550177"
              onKeyDown={(e) => {
                if (e.key === "Enter" && canDial && busy === "") void onDial();
              }}
            />
          </FormItem>
          <Button
            variant="primary"
            block
            icon={<CallRegular />}
            loading={busy === "dial"}
            disabled={!canDial || (busy !== "" && busy !== "dial")}
            onClick={() => void onDial()}
          >
            {t("拨打")}
          </Button>
          <div className="rounded-xl border border-gray-200 p-3 dark:border-white/10">
            <div className="flex items-center justify-between gap-3">
              <div className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-300">
                <MicRegular className="text-base" />
                {t("浏览器通话音频")}
              </div>
              <Switch checked={audioEnabled} onChange={toggleAudio} ariaLabel={t("浏览器通话音频")} />
            </div>
            <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">{audioText}</p>
            <p className="mt-1 text-xs text-gray-400 dark:text-gray-500">
              {t("音频走 8 kHz 单声道 PCM，只在 VoWiFi 线路接通后可用；点拨打或接听时浏览器会请求麦克风。")}
            </p>
          </div>
          {selected ? (
            <p className="text-xs text-gray-500 dark:text-gray-400">
              {tf("当前线路：{line}", { line: selected.label })}
            </p>
          ) : null}
        </div>

        <div className="space-y-6">
          {otherRinging.length ? (
            <div className="rounded-2xl border border-amber-300 bg-amber-50 p-4 dark:border-amber-500/30 dark:bg-amber-500/10">
              {otherRinging.map((call) => (
                <div key={call.id} className="flex flex-wrap items-center justify-between gap-3">
                  <div className="text-sm text-amber-800 dark:text-amber-200">
                    <CallInboundRegular className="mr-1 inline text-base" />
                    {tf("其他线路 {line} 有来电：{number}", { line: call.phoneNumber || call.sessionId || "", number: call.number || "-" })}
                  </div>
                  <div className="flex gap-2">
                    <Button size="small" variant="success" loading={busy === `answer:${call.id}`} onClick={() => void onAnswer(call)}>
                      {t("接听")}
                    </Button>
                    <Button size="small" variant="danger" loading={busy === `hangup:${call.id}`} onClick={() => void onHangup(call)}>
                      {t("拒接")}
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          ) : null}

          <div className="ui-card p-5">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">{t("当前通话")}</h3>
              {snapshot ? (
                <Tag type={snapshot.transport === "vowifi" ? "primary" : "warning"}>
                  {snapshot.transport === "vowifi" ? "VoWiFi" : t("蜂窝")}
                </Tag>
              ) : null}
            </div>
            {callsError ? <p className="mb-3 text-sm text-red-600 dark:text-red-400">{callsError}</p> : null}
            {!selected ? (
              <EmptyState title={t("没有可用线路")} subtitle={t("先在设备管理里启用 VoWiFi 或多隧道。")} />
            ) : !current ? (
              <EmptyState title={t("没有进行中的通话")} subtitle={t("拨打一个号码，或等待来电。")} icon={<CallRegular />} />
            ) : (
              <div className="space-y-4">
                <div className="flex flex-wrap items-center gap-3">
                  {current.direction === "incoming" ? (
                    <CallInboundRegular className="text-2xl text-amber-500" />
                  ) : (
                    <CallOutboundRegular className="text-2xl text-indigo-500" />
                  )}
                  <div className="text-2xl font-semibold tracking-tight text-gray-900 dark:text-white">{current.number || "-"}</div>
                  {renderStateTag(current)}
                  {current.codec ? <Tag>{current.codec}</Tag> : null}
                  {current.mediaReady ? <Tag type="success">{t("媒体已协商")}</Tag> : null}
                </div>
                <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm sm:grid-cols-3">
                  <div>
                    <dt className="text-gray-500 dark:text-gray-400">{t("方向")}</dt>
                    <dd className="text-gray-900 dark:text-gray-100">{current.direction === "incoming" ? t("来电") : t("呼出")}</dd>
                  </div>
                  <div>
                    <dt className="text-gray-500 dark:text-gray-400">{t("本方号码")}</dt>
                    <dd className="text-gray-900 dark:text-gray-100">{current.phoneNumber || selected.phone || "-"}</dd>
                  </div>
                  <div>
                    <dt className="text-gray-500 dark:text-gray-400">{t("开始时间")}</dt>
                    <dd className="text-gray-900 dark:text-gray-100">{formatTime(current.startedAt)}</dd>
                  </div>
                  {current.answeredAt ? (
                    <div>
                      <dt className="text-gray-500 dark:text-gray-400">{t("接通时间")}</dt>
                      <dd className="text-gray-900 dark:text-gray-100">{formatTime(current.answeredAt)}</dd>
                    </div>
                  ) : null}
                  {current.endedAt ? (
                    <div>
                      <dt className="text-gray-500 dark:text-gray-400">{t("结束时间")}</dt>
                      <dd className="text-gray-900 dark:text-gray-100">{formatTime(current.endedAt)}</dd>
                    </div>
                  ) : null}
                  <div className="col-span-2 sm:col-span-3">
                    <dt className="text-gray-500 dark:text-gray-400">{t("通话 ID")}</dt>
                    <dd className="break-all font-mono text-xs text-gray-700 dark:text-gray-300">{current.id}</dd>
                  </div>
                </dl>
                {sipVerdict(current) ? (
                  <div
                    className={
                      current.state === "failed"
                        ? "rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-300"
                        : "rounded-xl border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700 dark:border-white/10 dark:bg-white/5 dark:text-gray-300"
                    }
                  >
                    <div className="text-xs uppercase tracking-wide opacity-70">{t("网络侧结果")}</div>
                    <div className="mt-1 font-mono text-base">{sipVerdict(current)}</div>
                    {current.state === "failed" && current.sipCode && current.sipCode >= 500 ? (
                      <div className="mt-1 text-xs opacity-80">{t("5xx 是运营商网络拒绝，不是本地故障。")}</div>
                    ) : null}
                  </div>
                ) : null}
                <div className="flex flex-wrap gap-2">
                  {current.direction === "incoming" && current.state === "ringing" ? (
                    <Button
                      variant="success"
                      icon={<CallRegular />}
                      loading={busy === `answer:${current.id}`}
                      onClick={() => void onAnswer(current)}
                    >
                      {t("接听")}
                    </Button>
                  ) : null}
                  {isLiveCall(current) ? (
                    <Button
                      variant="danger"
                      icon={<CallEndRegular />}
                      loading={busy === `hangup:${current.id}`}
                      onClick={() => void onHangup(current)}
                    >
                      {current.direction === "incoming" && current.state === "ringing" ? t("拒接") : t("挂断")}
                    </Button>
                  ) : null}
                </div>
              </div>
            )}
          </div>

          <div className="ui-card overflow-hidden">
            <div className="border-b border-gray-200 px-5 py-3 text-sm font-semibold text-gray-900 dark:border-white/10 dark:text-white">
              {t("本线路通话记录")}
            </div>
            {history.length === 0 ? (
              <p className="px-5 py-6 text-sm text-gray-500 dark:text-gray-400">{t("这条线路自本次注册以来没有通话。")}</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="text-left text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    <tr>
                      <th className="px-5 py-2">{t("时间")}</th>
                      <th className="px-3 py-2">{t("方向")}</th>
                      <th className="px-3 py-2">{t("号码")}</th>
                      <th className="px-3 py-2">{t("状态")}</th>
                      <th className="px-3 py-2">{t("网络侧结果")}</th>
                      <th className="px-3 py-2"></th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-white/5">
                    {history.map((call) => (
                      <tr key={call.id}>
                        <td className="whitespace-nowrap px-5 py-2 text-gray-700 dark:text-gray-300">{formatTime(call.startedAt)}</td>
                        <td className="px-3 py-2 text-gray-700 dark:text-gray-300">{call.direction === "incoming" ? t("来电") : t("呼出")}</td>
                        <td className="px-3 py-2 font-mono text-gray-900 dark:text-gray-100">{call.number || "-"}</td>
                        <td className="px-3 py-2">{renderStateTag(call)}</td>
                        <td className="px-3 py-2 font-mono text-xs text-gray-700 dark:text-gray-300">{sipVerdict(call) || "-"}</td>
                        <td className="px-3 py-2 text-right">
                          {isLiveCall(call) ? (
                            <Button size="small" variant="danger" loading={busy === `hangup:${call.id}`} onClick={() => void onHangup(call)}>
                              {t("挂断")}
                            </Button>
                          ) : null}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
