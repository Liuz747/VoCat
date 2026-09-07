// Shared phase labels for the multi-tunnel (multisim) group and its lines.
// The group phase comes from the multisim manager; line phases are the
// per-line VoWiFi orchestrator phases.
type Translate = (text: string) => string;

export function multiSIMGroupPhaseLabel(phase: string, t: Translate): string {
  const phases: Record<string, string> = {
    disabled: t("已停止"), idle: t("未启动"), pending: t("等待启动"), starting: t("正在建立线路"),
    preparing: t("正在接管设备"), stopping: t("正在停止并恢复设备"), ready: t("运行中"),
    active: t("运行中"), running: t("运行中"), restoring: t("正在恢复设备"), partial: t("部分线路可用"), failed: t("运行失败"),
    cleanup_failed: t("线路清理失败"), restore_failed: t("设备恢复失败"),
  };
  return phases[phase] || phase || t("等待启动");
}

export function multiSIMLinePhaseLabel(phase: string, t: Translate): string {
  const phases: Record<string, string> = {
    idle: t("未启动"), pending: t("等待启动"), starting: t("正在建立线路"), stopping: t("正在拆除线路"),
    failed: t("线路失败，等待重连"), disabled: t("已停止"),
    sim_ready: t("SIM 已就绪"), access_ready: t("正在建立隧道"), tunnel_ready: t("隧道已建立"),
    ims_ready: t("IMS 已注册"), sms_ready: t("可接收短信"),
  };
  return phases[phase] || phase || t("等待启动");
}

// Human-readable reason codes recorded by the orchestrator in last_reason.
export function multiSIMReasonLabel(reason: string | undefined, t: Translate): string {
  if (!reason) return "";
  const reasons: Record<string, string> = {
    sms_ready: t("可接收短信"), ims_registered: t("IMS 已注册"), ipsec_tunnel_ready: t("隧道已建立"),
    epdg_access_ready: t("ePDG 可达"), sim_and_aka_ready: t("SIM 与鉴权就绪"), enable_requested: t("已请求启动"),
    enable_failed: t("建立失败"), runtime_tunnel_failed: t("隧道运行中断"), runtime_ims_failed: t("IMS 会话中断"),
    reconnect_requested: t("已请求重连"), disable_requested: t("已请求停止"), disabled: t("已停止"),
    ims_registered_sms_unavailable: t("IMS 已注册但短信能力未确认"),
  };
  return reasons[reason] || reason;
}

// Relative age such as "3 分钟" for status rows; empty when the input is unusable.
export function ageText(iso: string | undefined, t: Translate): string {
  if (!iso) return "";
  const ms = Date.now() - new Date(iso).getTime();
  if (!Number.isFinite(ms) || ms < 0) return "";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s} ${t("秒")}`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} ${t("分钟")}`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h} ${t("小时")} ${m % 60} ${t("分钟")}`;
  return `${Math.floor(h / 24)} ${t("天")}`;
}
