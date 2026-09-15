import { cx } from "../../lib/utils";
import { Button, Input, Tag } from "../ui";
import { EsimCardPolicyInline } from "./EsimCardPolicyInline";
import type { EsimProfileItem } from "./types";
import { useI18n } from "../../lib/i18n";

export interface EsimProfileRowProps {
  deviceId: string;
  deviceOnline: boolean;
  aidHex?: string;
  profile: EsimProfileItem;
  showSensitive: boolean;
  renaming: boolean;
  renameValue: string;
  switching: boolean;
  deleting: boolean;
  policyOpen: boolean;
  // A running multi-tunnel group decides which profile is enabled.
  switchLocked?: boolean;
  onRenameValueChange: (v: string) => void;
  onSwitch: () => void;
  onStartRename: () => void;
  onSubmitRename: () => void;
  onCancelRename: () => void;
  onTogglePolicy: () => void;
  onDelete: () => void;
  onPolicyChanged: () => void;
}

export function EsimProfileRow(props: EsimProfileRowProps) {
  const { t } = useI18n();
  const { profile: p, renaming } = props;
  const active = p.state === 1;
  return (
    <>
      <div className="flex items-center justify-between gap-3 px-4 py-3 transition-colors hover:bg-gray-50/50 dark:hover:bg-white/5">
        <div className="min-w-0 flex-1">
          {!renaming ? (
            <>
              <div className="flex items-center gap-2">
                <span className={cx("h-2 w-2 flex-shrink-0 rounded-full", active ? "bg-green-500" : "bg-gray-300 dark:bg-gray-600")} />
                <span className="truncate text-sm font-medium text-gray-900 dark:text-white">{p.name || p.iccid}</span>
                <Tag type={active ? "success" : "info"} className="flex-shrink-0">
                  {p.stateText}
                </Tag>
              </div>
              <div className="ml-4 mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-gray-500 transition-all dark:text-gray-400">
                <span>{p.serviceProviderName}</span>
                <span className={cx(!props.showSensitive && "select-none blur-sm")}>{p.iccid}</span>
              </div>
            </>
          ) : (
            <div className="flex items-center gap-2">
              <Input
                inputSize="default"
                value={props.renameValue}
                onChange={(e) => props.onRenameValueChange(e.target.value)}
                placeholder={t("输入新名称")}
                autoFocus
                className="!w-52"
                onKeyDown={(e) => {
                  if (e.key === "Enter") props.onSubmitRename();
                  else if (e.key === "Escape") props.onCancelRename();
                }}
              />
              <Button variant="primary" size="small" onClick={props.onSubmitRename} className="!border-0">
                {t("保存")}
              </Button>
              <Button size="small" onClick={props.onCancelRename} className="!border-0">
                {t("取消")}
              </Button>
            </div>
          )}
        </div>
        {!renaming ? (
          <div className="flex flex-shrink-0 items-center gap-2">
            <span title={props.switchLocked ? t("多隧道运行中由多隧道决定启用哪个 Profile") : undefined}>
              <Button variant={active ? "warning" : "success"} size="small" plain loading={props.switching} disabled={props.switchLocked} onClick={props.onSwitch}>
                {active ? t("禁用") : t("切换")}
              </Button>
            </span>
            <Button variant="primary" size="small" plain onClick={props.onStartRename}>
              {t("改名")}
            </Button>
            <Button variant={props.policyOpen ? "primary" : "default"} size="small" plain onClick={props.onTogglePolicy}>
              {t("策略")}
            </Button>
            <span title={active && !props.switchLocked ? t("当前启用的 Profile 不能删除；请先切换到另一张卡") : undefined}>
              <Button variant="danger" size="small" plain loading={props.deleting} disabled={active && !props.switchLocked} onClick={props.onDelete}>
                {t("删除")}
              </Button>
            </span>
          </div>
        ) : null}
      </div>
      {props.policyOpen ? (
        <div className="border-t-0 px-4 pb-3">
          <EsimCardPolicyInline
            deviceId={props.deviceId}
            iccid={p.iccid}
            isActiveCard={active}
            deviceOnline={props.deviceOnline}
            onPolicyChanged={props.onPolicyChanged}
          />
        </div>
      ) : null}
    </>
  );
}
