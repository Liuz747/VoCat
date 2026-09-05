import type { DeviceListItem } from "../../types";
import { cx } from "../../lib/utils";
import { Tag, StatusDot } from "../ui";
import { deviceStatusMeta } from "./shared";
import { deviceTypeImage } from "../../lib/deviceTypes";
import { useI18n } from "../../lib/i18n";
import { rotationSequenceText, type RotationTaskSummary } from "./types";

export interface DeviceListItemCardProps {
  device: DeviceListItem;
  selected: boolean;
  statusText: string;
  rotationTask?: RotationTaskSummary;
  onSelect: (id: string) => void;
}

export function DeviceListItemCard({ device, selected, statusText, rotationTask, onSelect }: DeviceListItemCardProps) {
  const { t } = useI18n();
  const meta = deviceStatusMeta(device);
  return (
    <div className="device-list-item">
      <button
        type="button"
        onClick={() => onSelect(device.id)}
        className={cx(
          "h-full w-full rounded-xl border p-3 text-left transition-all",
          selected
            ? "border-indigo-200 bg-indigo-50/70 dark:border-indigo-500/30 dark:bg-indigo-500/10"
            : "border-gray-100 hover:bg-gray-50/60 dark:border-white/10 dark:hover:bg-white/5",
        )}
      >
        <div className="flex items-start justify-between gap-2">
          <img src={deviceTypeImage(device.deviceType)} alt="" className="h-10 w-10 shrink-0 object-contain" />
          <div className="min-w-0 flex-1">
            <div className="truncate font-bold text-gray-800 dark:text-gray-100">{device.name || device.id}</div>
            <div className="mt-0.5 truncate text-xs text-gray-500">
              {device.id} · {device.interface || "--"}
            </div>
            <div className="mt-1 truncate text-xs text-gray-400">{statusText}</div>
            {rotationTask ? (
              <div className="mt-0.5 truncate text-xs text-sky-600 dark:text-sky-300" title={`${rotationTask.name}：${rotationSequenceText(rotationTask)}`}>
                {t("轮询中")} · {rotationSequenceText(rotationTask)}
              </div>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            <StatusDot tone={meta.tone} size="sm" animated={meta.animated} />
            <Tag type={meta.tag}>{meta.label}</Tag>
          </div>
        </div>
      </button>
    </div>
  );
}
