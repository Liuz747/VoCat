import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import type { DashboardDevice, DashboardHost, DashboardUpcomingTask } from "../types";
import { usePolling } from "../lib/usePolling";
import { tf, useI18n } from "../lib/i18n";
import { PageHeader } from "../components/ui/PageHeader";
import { RefreshButton } from "../components/ui/RefreshButton";
import { ErrorState } from "../components/ui/ErrorState";
import { ListSkeleton } from "../components/ui/ListSkeleton";
import { EmptyState } from "../components/ui/EmptyState";
import { DeviceCard } from "../components/DeviceCard";
import { HostInfoCard } from "../components/dashboard/HostInfoCard";
import { HostPerfCard } from "../components/dashboard/HostPerfCard";
import { UpcomingTasksCard } from "../components/dashboard/UpcomingTasksCard";
import { OnlineRateCard } from "../components/dashboard/OnlineRateCard";

interface LoadError { message: string; status?: number; method?: string; url?: string }

// 任务卡只展示最近的三条；定时任务变化慢，轮询间隔比设备/性能数据更宽。
const UPCOMING_TASK_COUNT = 3;
const TASKS_POLL_INTERVAL = 15000;

export default function DashboardPage() {
  const { t } = useI18n();
  const navigate = useNavigate();
  const [devices, setDevices] = useState<DashboardDevice[]>([]);
  const [devicesLoading, setDevicesLoading] = useState(false);
  const [devicesError, setDevicesError] = useState<LoadError | null>(null);
  const [devicesOkAt, setDevicesOkAt] = useState<number | null>(null);
  const [host, setHost] = useState<DashboardHost | null>(null);
  const [upcomingTasks, setUpcomingTasks] = useState<DashboardUpcomingTask[]>([]);

  const fetchDevices = useCallback(async () => {
    setDevicesLoading(true);
    try {
      const list = await api<DashboardDevice[]>("/dashboard/devices");
      setDevices(list || []);
      setDevicesError(null);
      setDevicesOkAt(Date.now());
    } catch (e: any) {
      setDevicesError({ message: e?.message || t("加载失败"), status: e?.status });
    } finally {
      setDevicesLoading(false);
    }
  }, []);

  // 宿主机信息 + 性能数据：2s 轮询让网络速率足够"实时"。
  const fetchHost = useCallback(async () => {
    try {
      setHost(await api<DashboardHost>("/dashboard/host"));
    } catch {
      /* 宿主机数据失败不打断设备监控；保留上一次成功值。 */
    }
  }, []);

  const fetchUpcomingTasks = useCallback(async () => {
    try {
      const data = await api<{ tasks?: DashboardUpcomingTask[] }>("/automatic-tasks");
      const upcoming = (data.tasks || [])
        .filter((task) => task.enabled && task.nextRunAt && !task.nextRunAt.startsWith("0001-"))
        .sort((a, b) => new Date(a.nextRunAt).getTime() - new Date(b.nextRunAt).getTime())
        .slice(0, UPCOMING_TASK_COUNT);
      setUpcomingTasks(upcoming);
    } catch {
      /* 任务列表加载失败时保留旧数据。 */
    }
  }, []);

  usePolling(fetchDevices, 5000);
  usePolling(fetchHost, 2000);
  usePolling(fetchUpcomingTasks, TASKS_POLL_INTERVAL);

  // Records whose module is unplugged or moved to another host stay configured but are not part of the live fleet.
  const attached = devices.filter((d) => d.physicalPresent !== false && d.lifecyclePhase !== "missing");
  const detachedCount = devices.length - attached.length;
  const total = attached.length;
  const online = attached.filter((d) => d?.healthy).length;
  const openDevice = (id: string) => navigate(`/devices?device=${encodeURIComponent(id)}&tab=overview`);

  return (
    <div>
      <PageHeader
        title={t("设备监控")}
        subtitle={t("实时监测模组检测状态与出口连通性")}
        actions={<RefreshButton loading={devicesLoading} onClick={fetchDevices} />}
      />
      <div className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <HostInfoCard info={host?.host} />
        <HostPerfCard perf={host?.perf} />
        <UpcomingTasksCard tasks={upcomingTasks} />
        <OnlineRateCard online={online} total={total} />
      </div>
      {devicesError ? (
        <ErrorState className="mb-6" title={t("设备列表加载失败")} message={devicesError.message} statusCode={devicesError.status} requestMethod={devicesError.method} requestUrl={devicesError.url} lastSuccessAt={devicesOkAt} retryText={t("重试")} onRetry={fetchDevices} />
      ) : null}
      {devicesLoading && devices.length === 0 ? (
        <ListSkeleton rows={10} />
      ) : attached.length === 0 ? (
        <EmptyState title={t("暂无设备接入")} subtitle={t("请先在设备管理中添加或接管设备")} />
      ) : (
        <>
          <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4">
            {attached.map((d) => (
              <DeviceCard key={d.id} device={d} onOpen={openDevice} />
            ))}
          </div>
          {detachedCount > 0 ? (
            <div className="mt-4 text-center text-xs text-[var(--text-secondary,#888)]">
              {tf("另有 {count} 台已拔出", { count: detachedCount })}
            </div>
          ) : null}
        </>
      )}
    </div>
  );
}
