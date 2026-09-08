import { useEffect, useMemo, useState, type ReactNode } from "react";
import { SendRegular } from "@fluentui/react-icons";
import { Button, Input, Modal, Select, Textarea } from "../ui";
import type { DeviceListItem } from "../../types";
import { analyzeSmsEncoding } from "./smsText";
import { senderOptions } from "./smsSenders";
import { tf, useI18n } from "../../lib/i18n";

export interface NewSmsPayload {
  deviceId: string;
  phone: string;
  message: string;
  /** Line session id when the device runs multi-tunnel (one IMS session per profile). */
  sessionId?: string;
}

export interface NewSmsModalProps {
  open: boolean;
  devices: DeviceListItem[];
  defaultDeviceId: string;
  sending: boolean;
  onClose: () => void;
  onSend: (payload: NewSmsPayload) => void;
}

function FormItem({ label, children }: { label: ReactNode; children: ReactNode }) {
  return (
    <div>
      <label className="mb-1.5 block text-sm font-medium text-gray-700 dark:text-gray-300">{label}</label>
      {children}
    </div>
  );
}

export function NewSmsModal({ open, devices, defaultDeviceId, sending, onClose, onSend }: NewSmsModalProps) {
  const { t } = useI18n();
  const senders = useMemo(() => senderOptions(devices, t("未就绪")), [devices, t]);
  const defaultSender = useMemo(() => {
    const ofDevice = senders.filter((s) => s.deviceId === defaultDeviceId);
    return (ofDevice.find((s) => !s.disabled) || ofDevice[0] || senders.find((s) => !s.disabled) || senders[0])?.value || "";
  }, [senders, defaultDeviceId]);
  const [sender, setSender] = useState(defaultSender);
  const [phone, setPhone] = useState("");
  const [content, setContent] = useState("");

  // Reset the form every time the dialog opens (reference `Yt`).
  useEffect(() => {
    if (open) {
      setSender(defaultSender);
      setPhone("");
      setContent("");
    }
  }, [open, defaultSender]);
  // Keep the selection valid when the device list refreshes underneath the dialog.
  useEffect(() => {
    if (sender && !senders.some((s) => s.value === sender)) setSender(defaultSender);
  }, [senders, sender, defaultSender]);

  const selected = senders.find((s) => s.value === sender);
  const senderSelectOptions = useMemo(
    () => senders.map((s) => ({ value: s.value, label: s.label, disabled: s.disabled })),
    [senders],
  );
  const info = useMemo(() => analyzeSmsEncoding(content), [content]);
  const length = useMemo(() => Array.from(content || "").length, [content]);

  return (
    <Modal
      open={open}
      onClose={onClose}
      title={t("发送短信")}
      width="max-w-[min(520px,92vw)]"
      footer={
        <>
          <Button onClick={onClose}>{t("取消")}</Button>
          <Button
            variant="primary"
            loading={sending}
            disabled={!selected}
            icon={<SendRegular />}
            onClick={() => selected && onSend({ deviceId: selected.deviceId, phone, message: content, sessionId: selected.sessionId })}
          >
            {t("发送")}
          </Button>
        </>
      }
    >
      <div className="mt-2 space-y-4">
        <FormItem label={t("发送号码")}>
          <Select value={sender} onChange={setSender} options={senderSelectOptions} placeholder={t("选择发送号码")} />
        </FormItem>
        <FormItem label={t("目标号码")}>
          <Input value={phone} onChange={(e) => setPhone(e.target.value)} placeholder="+12025550177" />
        </FormItem>
        <FormItem label={t("短信内容")}>
          <Textarea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            placeholder={t("输入短信内容...")}
            rows={4}
            className="max-h-64 resize-none [field-sizing:content]"
          />
          <div className="mt-2 flex justify-end text-xs text-gray-400">
            {tf("{encoding} · 预计 {parts} 段 · {length} 字", { encoding: info.encoding, parts: info.parts, length })}
          </div>
        </FormItem>
      </div>
    </Modal>
  );
}
