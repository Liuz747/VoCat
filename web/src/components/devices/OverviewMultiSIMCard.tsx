import { cx } from "../../lib/utils";
import { StatusDot } from "../ui";
import type { DeviceDetail } from "./types";
import { tf, useI18n } from "../../lib/i18n";
import { multiSIMLinePhaseLabel, multiSIMGroupPhaseLabel } from "./multisimLabels";

// Overview card shown instead of the single-line VoWiFi card while a
// multi-tunnel group owns the modem: the legacy runtime is idle then, so the
// per-line summary is the only truthful picture of what is receiving.
export function OverviewMultiSIMCard({ device }: { device: DeviceDetail }) {
  const { t } = useI18n();
  const m = device.multisim;
  if (!m) return null;
  const overall = m.linesTotal > 0 && m.linesReady === m.linesTotal ? "ok" : m.linesReady > 0 ? "partial" : "off";
  const tone = overall === "ok" ? "success" : overall === "partial" ? "warning" : "danger";
  return (
    <>
      <div
        className={cx(
          "mb-3 flex items-center gap-2.5 rounded-xl border px-3.5 py-2.5",
          overall === "ok" && "border-emerald-200 bg-emerald-50 dark:border-emerald-500/25 dark:bg-emerald-500/10",
          overall === "partial" && "border-amber-200 bg-amber-50 dark:border-amber-500/25 dark:bg-amber-500/10",
          overall === "off" && "border-red-200 bg-red-50 dark:border-red-500/25 dark:bg-red-500/10",
        )}
      >
        <StatusDot tone={tone} size="sm" animated={overall !== "off"} />
        <div className="min-w-0">
          <div
            className={cx(
              "text-sm font-bold leading-tight",
              overall === "ok" && "text-emerald-700 dark:text-emerald-300",
              overall === "partial" && "text-amber-700 dark:text-amber-300",
              overall === "off" && "text-red-700 dark:text-red-300",
            )}
          >
            {t("多隧道")} · {tf("{ready}/{total} 可收", { ready: m.linesReady, total: m.linesTotal })}
          </div>
          <div className="mt-0.5 truncate text-xs text-gray-500 dark:text-gray-400">
            {multiSIMGroupPhaseLabel(m.phase, t)}{m.owned ? "" : ` · ${t("已启用但尚未接管设备")}`}{m.lastError ? ` · ${m.lastError}` : ""}
          </div>
        </div>
      </div>
      <div className="divide-y divide-gray-100 overflow-hidden rounded-lg border border-gray-200 text-sm dark:divide-white/5 dark:border-white/10">
        {m.lines.map((line) => (
          <div key={line.sessionId} className="flex items-center justify-between gap-3 px-3 py-2">
            <div className="flex min-w-0 items-center gap-2">
              <StatusDot tone={line.smsReady ? "success" : line.phase === "failed" ? "danger" : "warning"} size="sm" animated={line.smsReady} />
              <div className="min-w-0">
                <div className="truncate font-medium text-gray-800 dark:text-gray-100">
                  {line.phoneNumber || line.name || `…${line.iccidSuffix}`}
                  {line.name && line.phoneNumber ? <span className="ml-2 text-xs text-gray-400">{line.name}</span> : null}
                </div>
                <div className="truncate text-xs text-gray-500">
                  {line.smsReady ? t("可接收短信") : multiSIMLinePhaseLabel(line.phase, t)}
                  {line.proxyId ? ` · ${line.proxyId}` : ""}
                  {line.attempt > 1 ? ` · ${tf("第 {n} 次建立", { n: line.attempt })}` : ""}
                  {!line.smsReady && line.lastErrorClass ? ` · ${line.lastErrorClass}` : ""}
                </div>
              </div>
            </div>
            <span className="shrink-0 font-mono text-[11px] text-gray-400">…{line.iccidSuffix}</span>
          </div>
        ))}
      </div>
      <p className="mt-2 text-xs text-gray-400">{t("逐号详情、重连与续约在「eSIM → 多号码常驻接收」面板。")}</p>
    </>
  );
}
