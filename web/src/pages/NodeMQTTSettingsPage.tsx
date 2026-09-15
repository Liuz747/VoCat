import { useCallback, useEffect, useState, type ReactNode } from "react";
import {
  ArrowClockwiseRegular,
  CheckmarkCircleRegular,
  CloudDismissRegular,
  PlugConnectedRegular,
  ShieldCheckmarkRegular,
} from "@fluentui/react-icons";
import { api, apiMessage } from "../api";
import type { NodeMQTTSettings, NodeMQTTStatus } from "../types";
import { Button, Input, PageHeader, Select, Switch, Textarea, message } from "../components/ui";
import { PasswordInput } from "../components/settings/controls";

const EMPTY: NodeMQTTSettings = {
  enabled: false,
  scheme: "tcp",
  host: "",
  port: 1883,
  node: "",
  clientId: "",
  username: "",
  password: "",
  caCertificate: "",
  keepAliveSeconds: 30,
  sessionExpirySeconds: 86400,
  heartbeatSeconds: 30,
  businessAckEnabled: false,
  commandWorkers: 4,
  maxPayloadBytes: 131072,
  phoneCacheSeconds: 30,
  phoneRefreshTimeoutSeconds: 15,
};

const EMPTY_STATUS: NodeMQTTStatus = {
  enabled: false,
  connected: false,
  subscribed: false,
  outboxPending: 0,
};

function Field({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) {
  return (
    <label className="space-y-1.5">
      <span className="block text-xs font-bold uppercase tracking-wide text-gray-500 dark:text-gray-400">{label}</span>
      {children}
      {hint ? <span className="block text-[11px] leading-5 text-gray-400">{hint}</span> : null}
    </label>
  );
}

function StatusCard({ label, value, tone = "neutral" }: { label: string; value: ReactNode; tone?: "good" | "warn" | "neutral" }) {
  const toneClass = tone === "good"
    ? "border-emerald-200 bg-emerald-50/70 text-emerald-700 dark:border-emerald-500/20 dark:bg-emerald-500/10 dark:text-emerald-300"
    : tone === "warn"
      ? "border-amber-200 bg-amber-50/70 text-amber-700 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-300"
      : "border-gray-200 bg-white text-gray-700 dark:border-white/10 dark:bg-white/5 dark:text-gray-200";
  return <div className={`rounded-xl border px-4 py-3 ${toneClass}`}><div className="text-[11px] font-bold uppercase tracking-wider opacity-65">{label}</div><div className="mt-1 font-mono text-sm font-bold">{value}</div></div>;
}

export default function NodeMQTTSettingsPage() {
  const [form, setForm] = useState<NodeMQTTSettings>(EMPTY);
  const [status, setStatus] = useState<NodeMQTTStatus>(EMPTY_STATUS);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);

  const load = useCallback(async (quiet = false) => {
    if (!quiet) setLoading(true);
    try {
      const value = await api<{ settings: NodeMQTTSettings; status: NodeMQTTStatus }>("/settings/node-mqtt");
      if (!quiet) setForm({ ...EMPTY, ...(value.settings || {}) });
      setStatus(value.status || EMPTY_STATUS);
    } catch (error) {
      if (!quiet) message.error(apiMessage(error) || "读取总云 MQTT 配置失败");
    } finally {
      if (!quiet) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(true), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  const update = <K extends keyof NodeMQTTSettings>(key: K, value: NodeMQTTSettings[K]) => {
    setForm((current) => {
      const next = { ...current, [key]: value };
      if (key === "node" && (!current.clientId || current.clientId === `cp-node-${current.node}`)) {
        next.clientId = value ? `cp-node-${String(value)}` : "";
      }
      if (key === "scheme" && (current.port === 1883 || current.port === 8883)) {
        next.port = value === "tls" ? 8883 : 1883;
      }
      return next;
    });
  };

  async function testConnection() {
    setTesting(true);
    try {
      await api("/settings/node-mqtt/test", { method: "POST", body: form });
      message.success("Broker、MQTT 5.0 鉴权与连接验证成功");
    } catch (error) {
      message.error(apiMessage(error) || "总云 MQTT 连接测试失败");
    } finally {
      setTesting(false);
    }
  }

  async function save() {
    setSaving(true);
    try {
      const value = await api<{ settings: NodeMQTTSettings; status: NodeMQTTStatus }>("/settings/node-mqtt", { method: "PUT", body: form });
      setForm({ ...EMPTY, ...(value.settings || {}) });
      setStatus(value.status || EMPTY_STATUS);
      message.success(form.enabled ? "总云 MQTT 配置已保存并开始连接" : "总云 MQTT 接入已关闭");
    } catch (error) {
      message.error(apiMessage(error) || "保存总云 MQTT 配置失败");
    } finally {
      setSaving(false);
    }
  }

  const ready = status.connected && status.subscribed;

  return (
    <div className="h-full overflow-y-auto">
      <PageHeader
        title="总云 MQTT 接入设置"
        subtitle="让总云通过 cardpool/v1/nodes/{node} 调度当前 SimHub 节点"
        actions={<Button icon={<ArrowClockwiseRegular />} loading={loading} onClick={() => void load()}>刷新状态</Button>}
      />

      <div className={`mx-auto max-w-6xl space-y-5 px-4 pb-12 sm:px-6 ${loading ? "pointer-events-none opacity-65" : ""}`}>
        <section className="relative overflow-hidden rounded-2xl border border-sky-200 bg-gradient-to-br from-sky-50 via-white to-emerald-50 p-5 shadow-sm dark:border-sky-500/20 dark:from-sky-500/10 dark:via-white/[.03] dark:to-emerald-500/10">
          <div className="absolute -right-10 -top-12 h-40 w-40 rounded-full border-[24px] border-sky-500/5" />
          <div className="relative flex flex-col justify-between gap-5 md:flex-row md:items-center">
            <div className="flex items-start gap-4">
              <div className={`grid h-12 w-12 shrink-0 place-items-center rounded-xl text-2xl ${ready ? "bg-emerald-500 text-white" : "bg-gray-200 text-gray-500 dark:bg-white/10 dark:text-gray-300"}`}>
                {ready ? <CheckmarkCircleRegular /> : <CloudDismissRegular />}
              </div>
              <div>
                <h2 className="text-lg font-bold text-gray-900 dark:text-white">{ready ? "总云链路已就绪" : form.enabled ? "正在等待总云链路" : "总云接入未启用"}</h2>
                <p className="mt-1 max-w-2xl text-sm leading-6 text-gray-500 dark:text-gray-400">上层总云 MQTT 与下层卡池 MQTT 使用独立连接和权限。关闭本开关不会停止卡池设备，也不会删除尚未 ACK 的任务记录。</p>
              </div>
            </div>
            <div className="flex shrink-0 items-center gap-3 rounded-xl border border-white/70 bg-white/70 px-4 py-3 shadow-sm backdrop-blur dark:border-white/10 dark:bg-black/10">
              <div className="text-right"><div className="text-sm font-bold text-gray-800 dark:text-white">启用总云 MQTT</div><div className="text-[11px] text-gray-400">保存后立即生效</div></div>
              <Switch checked={form.enabled} onChange={(value) => update("enabled", value)} />
            </div>
          </div>
          <div className="relative mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <StatusCard label="Broker" value={status.connected ? "CONNECTED" : "OFFLINE"} tone={status.connected ? "good" : form.enabled ? "warn" : "neutral"} />
            <StatusCard label="Command subscription" value={status.subscribed ? "READY" : "NOT READY"} tone={status.subscribed ? "good" : form.enabled ? "warn" : "neutral"} />
            <StatusCard label="Node" value={form.node || "--"} />
            <StatusCard label="Outbox pending" value={status.outboxPending ?? 0} tone={(status.outboxPending ?? 0) > 0 ? "warn" : "neutral"} />
          </div>
          {status.lastError ? <div className="relative mt-4 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-500/20 dark:bg-red-500/10 dark:text-red-300">最近连接错误：{status.lastError}</div> : null}
        </section>

        <div className="grid gap-5 lg:grid-cols-[1.25fr_.75fr]">
          <section className="rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-white/10 dark:bg-white/[.03]">
            <div className="mb-5 flex items-center gap-3"><div className="grid h-9 w-9 place-items-center rounded-lg bg-sky-100 text-sky-600 dark:bg-sky-500/10 dark:text-sky-300"><PlugConnectedRegular /></div><div><h2 className="font-bold text-gray-900 dark:text-white">连接与节点身份</h2><p className="text-xs text-gray-400">这些值构成节点唯一的总云命名空间</p></div></div>
            <div className="grid gap-4 sm:grid-cols-2">
              <Field label="连接协议"><Select value={form.scheme} onChange={(value) => update("scheme", value as "tcp" | "tls")} options={[{ value: "tcp", label: "MQTT / TCP" }, { value: "tls", label: "MQTT / TLS" }]} /></Field>
              <Field label="Broker 端口"><Input type="number" min={1} max={65535} value={form.port} onChange={(event) => update("port", Number(event.target.value))} /></Field>
              <Field label="Broker 地址" hint="只填写主机名或 IP，不含协议和路径"><Input value={form.host} onChange={(event) => update("host", event.target.value)} placeholder="mqtt.example.com" /></Field>
              <Field label="节点编号 node" hint="保存后用于 Topic，重启保持不变"><Input value={form.node} onChange={(event) => update("node", event.target.value)} placeholder="mdd-001" /></Field>
              <Field label="Client ID" hint="不得与卡池 Client ID 共用"><Input value={form.clientId} onChange={(event) => update("clientId", event.target.value)} placeholder="cp-node-mdd-001" /></Field>
              <Field label="用户名"><Input value={form.username} onChange={(event) => update("username", event.target.value)} autoComplete="off" /></Field>
              <Field label="密码" hint="显示 ******** 时留空或不修改将保留原密码"><PasswordInput value={form.password} onChange={(value) => update("password", value)} autoComplete="new-password" /></Field>
              <Field label="Keep Alive"><Input type="number" min={5} max={3600} value={form.keepAliveSeconds} onChange={(event) => update("keepAliveSeconds", Number(event.target.value))} suffix="秒" /></Field>
              <div className="sm:col-span-2 flex items-center justify-between gap-4 rounded-xl border border-gray-200 bg-gray-50/70 px-4 py-3 dark:border-white/10 dark:bg-white/[.03]">
                <div><div className="text-sm font-bold text-gray-800 dark:text-white">启用业务 ACK 校验</div><div className="mt-1 text-[11px] leading-5 text-gray-400">关闭时只确认 MQTT 发布成功，不等待总云 ACK，也不会重发已成功发布的消息。</div></div>
                <Switch checked={form.businessAckEnabled} onChange={(value) => update("businessAckEnabled", value)} />
              </div>
            </div>
            {form.scheme === "tls" ? <div className="mt-4"><Field label="CA 证书（PEM）" hint="留空使用系统 CA；不支持跳过证书验证"><Textarea rows={7} value={form.caCertificate || ""} onChange={(event) => update("caCertificate", event.target.value)} placeholder="-----BEGIN CERTIFICATE-----" /></Field></div> : null}
          </section>

          <div className="space-y-5">
            <section className="rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-white/10 dark:bg-white/[.03]">
              <div className="mb-4 flex items-center gap-3"><div className="grid h-9 w-9 place-items-center rounded-lg bg-emerald-100 text-emerald-600 dark:bg-emerald-500/10 dark:text-emerald-300"><ShieldCheckmarkRegular /></div><div><h2 className="font-bold text-gray-900 dark:text-white">协议固定参数</h2><p className="text-xs text-gray-400">按首版文档执行</p></div></div>
              <dl className="space-y-3 text-sm">
                <div className="flex justify-between border-b border-gray-100 pb-2 dark:border-white/10"><dt className="text-gray-500">MQTT 版本</dt><dd className="font-mono font-bold">5.0</dd></div>
                <div className="flex justify-between border-b border-gray-100 pb-2 dark:border-white/10"><dt className="text-gray-500">Session Expiry</dt><dd className="font-mono font-bold">{form.sessionExpirySeconds}s</dd></div>
                <div className="flex justify-between border-b border-gray-100 pb-2 dark:border-white/10"><dt className="text-gray-500">Heartbeat</dt><dd className="font-mono font-bold">{form.heartbeatSeconds}s</dd></div>
                <div className="flex justify-between border-b border-gray-100 pb-2 dark:border-white/10"><dt className="text-gray-500">Payload 上限</dt><dd className="font-mono font-bold">128 KiB</dd></div>
                <div className="flex justify-between border-b border-gray-100 pb-2 dark:border-white/10"><dt className="text-gray-500">号码缓存</dt><dd className="font-mono font-bold">30s</dd></div>
                <div className="flex justify-between"><dt className="text-gray-500">刷新等待</dt><dd className="font-mono font-bold">15s</dd></div>
              </dl>
            </section>

            <section className="rounded-2xl border border-sky-200 bg-sky-50/60 p-5 dark:border-sky-500/20 dark:bg-sky-500/10">
              <h3 className="text-sm font-bold text-sky-900 dark:text-sky-100">总云 Topic</h3>
              <code className="mt-2 block break-all rounded-lg bg-slate-900 px-3 py-3 text-xs leading-5 text-emerald-300">cardpool/v1/nodes/{form.node || "{node}"}/&#123;command | task | event | ack | presence | heartbeat&#125;</code>
              <p className="mt-3 text-xs leading-5 text-sky-700/80 dark:text-sky-200/70">总云账号不应获得 <code>vsim/+/cmd</code> 或 <code>vsim/push</code> 权限。</p>
            </section>
          </div>
        </div>

        <div className="flex flex-col-reverse gap-3 rounded-2xl border border-gray-200 bg-white p-4 shadow-sm dark:border-white/10 dark:bg-white/[.03] sm:flex-row sm:items-center sm:justify-between">
          <p className="text-xs leading-5 text-gray-400">测试连接不会订阅或消费业务命令；保存并应用后才开始订阅当前节点的 command/ack。</p>
          <div className="flex shrink-0 gap-2"><Button loading={testing} onClick={testConnection}>测试连接</Button><Button variant="primary" loading={saving} onClick={save}>保存并应用</Button></div>
        </div>
      </div>
    </div>
  );
}
