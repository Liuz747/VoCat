import { SettingsRegular } from "@fluentui/react-icons";
import type { DeveloperSettings } from "../../types";
import { useI18n } from "../../lib/i18n";
import { Button } from "../ui/Button";
import { Input } from "../ui/Input";
import { CardDecor, CardIcon, CardTitle } from "./Cards";

export function DeviceQuotaCard({
  value,
  limit,
  loading,
  saving,
  onLimitChange,
  onSave,
}: {
  value: DeveloperSettings | null;
  limit: number;
  loading: boolean;
  saving: boolean;
  onLimitChange: (limit: number) => void;
  onSave: () => void;
}) {
  const { lang } = useI18n();
  const zh = lang === "zh";
  return (
    <div className="ui-card group relative overflow-hidden p-8">
      <CardDecor />
      <div className="relative z-10 mb-6 flex items-center gap-3">
        <CardIcon>
          <SettingsRegular className="text-[24px]" />
        </CardIcon>
        <CardTitle
          title={zh ? "设备配额" : "Device quota"}
          subtitle={zh ? "可选配额，0 表示不限制" : "Optional quota; 0 means unlimited"}
        />
      </div>
      <div className="relative z-10 space-y-4">
        <Input
          type="number"
          min={0}
          value={Number.isFinite(limit) ? limit : ""}
          disabled={loading || saving}
          onChange={(event) => onLimitChange(Number(event.target.value))}
          suffix={zh ? "台" : "devices"}
        />
        <p className="text-xs text-gray-500 dark:text-gray-400">
          {zh
            ? "默认不限制设备数量；填 0 即不限制。恢复默认配置会清除配额，不会删除已经添加的设备。"
            : "Unlimited by default; enter 0 for no quota. Restoring the default configuration clears the quota; existing devices are not deleted."}
        </p>
        <Button variant="primary" loading={saving} disabled={loading} onClick={onSave} className="w-full !border-0">
          {zh ? "保存设备配额" : "Save device quota"}
        </Button>
      </div>
    </div>
  );
}
