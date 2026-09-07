import {
  Cellular3GRegular, Cellular4GRegular, Cellular5GRegular, CellularData1Regular,
  Wifi1Regular,
} from "@fluentui/react-icons";
import type { DashboardDevice } from "../types";
import { cx, signalBars, signalColor } from "../lib/utils";
import { deviceTypeImage } from "../lib/deviceTypes";
import { StatusDot } from "./ui/StatusDot";
import { useI18n } from "../lib/i18n";

const BAR_HEIGHTS = ["h-1/4", "h-2/4", "h-3/4", "h-full"];

export function DeviceCard({ device, onOpen }: { device: DashboardDevice; onOpen: (id: string) => void }) {
  const { t } = useI18n();
  const multi = device.multisim && (device.multisim.owned || device.multisim.enabled) ? device.multisim : null;
  const mode = `${device.networkDuplex ? `${device.networkDuplex} ` : ""}${device.networkMode || ""}`.trim();
  const up = mode.toUpperCase();
  const has = mode.length > 0;
  const NetIcon = device.vowifiActive || multi ? Wifi1Regular
    : !has ? CellularData1Regular
    : up.includes("5G") || up.includes("NR") ? Cellular5GRegular
    : up.includes("4G") || up.includes("LTE") ? Cellular4GRegular
    : up.includes("3G") || up.includes("WCDMA") || up.includes("HSPA") || up.includes("UMTS") ? Cellular3GRegular
    : CellularData1Regular;
  const netColor = multi ? (multi.linesReady === multi.linesTotal ? "text-emerald-500" : "text-amber-500")
    : device.vowifiActive ? "text-emerald-500"
    : !has ? "text-gray-400"
    : up.includes("5G") || up.includes("NR") ? "text-purple-500"
    : up.includes("4G") || up.includes("LTE") ? "text-blue-500"
    : up.includes("3G") ? "text-orange-500" : "text-gray-400";
  const words = mode.split(/\s+/).filter(Boolean);
  const second = words.length > 1 ? words[1] : words[0] || "";
  const isLte = second.toUpperCase() === "LTE";
  const bars = signalBars(device.signalDbm);

  return (
    <button
      type="button"
      onClick={() => onOpen(device.id)}
      className="ui-card ui-card-hover group relative block w-full overflow-hidden text-left transition-all duration-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-[#0ea5e9] focus-visible:ring-offset-2 focus-visible:ring-offset-white dark:focus-visible:ring-offset-gray-950"
    >
      <div className="absolute right-0 top-0 -mr-8 -mt-8 h-32 w-32 rounded-bl-full bg-gradient-to-br from-indigo-500/10 to-indigo-400/10 transition-transform group-hover:scale-150" />
      <div className="relative z-10 p-6">
        <div className="mb-6 flex items-start justify-between">
          <div className="flex items-center gap-3">
            <img src={deviceTypeImage(device.deviceType)} alt="" className="h-10 w-10 flex-shrink-0 object-contain" />
            <div>
              <h3 className="text-base font-bold text-gray-800 dark:text-gray-100">{device.name || device.id}</h3>
              <div className="mt-0.5 flex items-center gap-1.5">
                <StatusDot tone={device.healthy ? "success" : "danger"} size="md" animated={device.healthy} />
                <span className={cx("text-xs font-medium", device.healthy ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400")}>
                  {device.healthy ? t("在线") : t("离线")}
                </span>
              </div>
            </div>
          </div>
        </div>
        <div className="space-y-4">
          <div className="flex items-center justify-between rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-white/5 dark:bg-white/5">
            <div className="flex min-w-0 items-center gap-2">
              <div className="flex items-center gap-1.5 opacity-80">
                <NetIcon className={cx("h-[18px] w-[18px]", netColor)} />
                {!device.vowifiActive && !multi && mode && second ? (
                  <span className={cx("text-[11px] font-bold leading-none tracking-tighter", isLte ? "hidden xl:inline" : "")}>{second}</span>
                ) : null}
              </div>
              <span className="min-w-0 flex-1 truncate whitespace-nowrap text-sm font-medium text-gray-700 dark:text-gray-300">
                {multi ? `${t("多隧道")} · ${multi.linesReady}/${multi.linesTotal}` : device.vowifiActive ? "Wi-Fi Calling" : device.operator || t("检测中...")}
              </span>
            </div>
            {multi ? (
              <div className="flex items-center gap-1" title={multi.lines.map((l) => `${l.phoneNumber || l.name || l.iccidSuffix}: ${l.smsReady ? t("可接收短信") : l.phase}`).join("\n")}>
                {multi.lines.map((l) => (
                  <span key={l.sessionId} className={cx("inline-block h-2 w-2 rounded-full", l.smsReady ? "bg-emerald-500" : "bg-amber-500")} />
                ))}
              </div>
            ) : null}
            {!device.vowifiActive && !multi && (
              <div className="flex items-center gap-1" title={t("信号强度")}>
                <div className="flex h-3 items-end gap-[2px]">
                  {[1, 2, 3, 4].map((b) => (
                    <div key={b} className={cx("w-1 rounded-sm transition-all duration-500", BAR_HEIGHTS[b - 1], bars >= b ? signalColor(device.signalDbm) : "bg-gray-200 dark:bg-gray-700")} />
                  ))}
                </div>
                <span className="ml-1 hidden text-xs font-mono text-gray-400 xl:inline">{device.signalDbm}dBm</span>
              </div>
            )}
          </div>
        </div>
      </div>
    </button>
  );
}
