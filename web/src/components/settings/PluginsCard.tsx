import { useCallback, useEffect, useRef, useState } from "react";
import { AddRegular, ArrowUploadRegular, DeleteRegular, PlugConnectedRegular } from "@fluentui/react-icons";
import { api, apiMessage } from "../../api";
import { listPlugins, type InstalledPlugin } from "../../extensions";
import { Button, confirmDialog, Input, message } from "../ui";
import { CardDecor, CardIcon, CardTitle } from "./Cards";
import { useI18n } from "../../lib/i18n";

export function PluginsCard() {
  const { t } = useI18n();
  const [plugins, setPlugins] = useState<InstalledPlugin[]>([]);
  const [url, setURL] = useState("");
  const [sha256, setSHA256] = useState("");
  const [busy, setBusy] = useState("");
  const fileRef = useRef<HTMLInputElement>(null);

  const reload = useCallback(async () => {
    try { setPlugins(await listPlugins()); }
    catch (error) { message.error(apiMessage(error) || t("插件列表加载失败")); }
  }, [t]);

  useEffect(() => { void reload(); }, [reload]);

  async function installURL() {
    if (!url.trim()) return message.warning(t("请输入插件包 URL"));
    const confirmed = await confirmDialog(
      t("插件页面以当前管理员权限运行，插件后端还可以运行外部代码。仅安装你完全信任的插件。"),
      t("安装外部插件？"),
      { type: "warning", confirmText: t("安装") },
    );
    if (!confirmed) return;
    setBusy("install");
    try {
      await api("/extensions/install-url", { method: "POST", body: { url: url.trim(), sha256: sha256.trim() } });
      setURL(""); setSHA256("");
      message.success(t("插件已安装并启用"));
      await reload();
      window.dispatchEvent(new Event("vocat:plugins-changed"));
    } catch (error) { message.error(apiMessage(error) || t("插件安装失败")); }
    finally { setBusy(""); }
  }

  async function upload(file?: File) {
    if (!file) return;
    const confirmed = await confirmDialog(
      t("插件页面以当前管理员权限运行，插件后端还可以运行外部代码。仅安装你完全信任的插件。"),
      t("安装上传的插件？"),
      { type: "warning", confirmText: t("安装") },
    );
    if (!confirmed) { if (fileRef.current) fileRef.current.value = ""; return; }
    const form = new FormData();
    form.append("package", file);
    setBusy("upload");
    try {
      await api("/extensions/upload", { method: "POST", body: form });
      message.success(t("插件已安装并启用"));
      await reload();
      window.dispatchEvent(new Event("vocat:plugins-changed"));
    } catch (error) { message.error(apiMessage(error) || t("插件安装失败")); }
    finally { setBusy(""); if (fileRef.current) fileRef.current.value = ""; }
  }

  async function toggle(plugin: InstalledPlugin) {
    setBusy(plugin.id);
    try {
      await api(`/extensions/${encodeURIComponent(plugin.id)}`, { method: "PUT", body: { enabled: !plugin.enabled } });
      await reload();
      window.dispatchEvent(new Event("vocat:plugins-changed"));
    } catch (error) { message.error(apiMessage(error) || t("插件状态更新失败")); }
    finally { setBusy(""); }
  }

  async function uninstall(plugin: InstalledPlugin) {
    const confirmed = await confirmDialog(t("插件代码和插件数据将从本机删除。"), t("卸载插件？"), { type: "warning", confirmText: t("卸载") });
    if (!confirmed) return;
    setBusy(plugin.id);
    try {
      await api(`/extensions/${encodeURIComponent(plugin.id)}`, { method: "DELETE" });
      message.success(t("插件已卸载"));
      await reload();
      window.dispatchEvent(new Event("vocat:plugins-changed"));
    } catch (error) { message.error(apiMessage(error) || t("插件卸载失败")); }
    finally { setBusy(""); }
  }

  return (
    <div className="ui-card group relative overflow-hidden p-8 lg:col-span-2">
      <CardDecor />
      <div className="relative z-10 mb-6 flex items-center gap-3">
        <CardIcon><PlugConnectedRegular className="text-[24px]" /></CardIcon>
        <CardTitle title={t("插件")} subtitle={t("通过 URL 或本地插件包扩展功能")} />
      </div>
      <div className="relative z-10 grid gap-3 md:grid-cols-[1fr_18rem_auto]">
        <Input value={url} onChange={(event) => setURL(event.target.value)} placeholder="https://example.com/plugin.zip" />
        <Input value={sha256} onChange={(event) => setSHA256(event.target.value)} placeholder={t("SHA-256（可选，推荐）")} />
        <Button variant="primary" icon={<AddRegular />} loading={busy === "install"} onClick={() => void installURL()}>{t("从 URL 安装")}</Button>
      </div>
      <div className="relative z-10 mt-3">
        <input ref={fileRef} type="file" accept=".zip,.vocat-plugin,application/zip" className="hidden" onChange={(event) => void upload(event.target.files?.[0])} />
        <Button icon={<ArrowUploadRegular />} loading={busy === "upload"} onClick={() => fileRef.current?.click()}>{t("上传插件包")}</Button>
      </div>
      <div className="relative z-10 mt-6 space-y-3">
        {plugins.length === 0 ? <div className="ui-panel-muted p-5 text-sm text-gray-500">{t("尚未安装插件")}</div> : plugins.map((plugin) => (
          <div key={plugin.id} className="ui-panel-muted flex flex-col gap-3 p-4 sm:flex-row sm:items-center">
            <div className="min-w-0 flex-1">
              <div className="font-semibold">{plugin.name} <span className="text-xs font-normal text-gray-400">v{plugin.version}</span></div>
              <div className="mt-1 text-xs text-gray-500">{plugin.description || plugin.id}</div>
              <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-gray-400">
                {(plugin.permissions || []).map((permission) => <span key={permission} className="rounded bg-black/5 px-2 py-1 dark:bg-white/5">{permission}</span>)}
                {plugin.backendError ? <span className="text-red-500">{plugin.backendError}</span> : null}
              </div>
            </div>
            <Button loading={busy === plugin.id} onClick={() => void toggle(plugin)}>{plugin.enabled ? t("禁用") : t("启用")}</Button>
            <Button variant="danger" icon={<DeleteRegular />} disabled={busy === plugin.id} onClick={() => void uninstall(plugin)}>{t("卸载")}</Button>
          </div>
        ))}
      </div>
    </div>
  );
}
