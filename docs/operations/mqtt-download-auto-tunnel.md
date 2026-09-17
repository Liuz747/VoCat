# MQ 写卡后自动开线

〔源码 2026-09-17〕现有 `esim.profile.download` 命令保持原请求格式。节点回读确认新 ICCID 后，自动保存并启动该 Profile 的多隧道配置，业务端无需再发送开隧道命令。

- 没有组时创建单 Profile 的多隧道组；后续写卡逐条增量加入。
- 运行中的组保留已有线路，新增线路使用 eUICC 的 ISD-R AID。
- 已停止的组只启用本次新写入的 Profile，不把旧的停用线路一并恢复。
- 写卡和回读使用同一物理读卡事务；事务结束后才启动隧道，避免读卡锁与鉴权互锁。
- 开线配置持久保存，进程重启后沿用现有组恢复机制。已启动线路使用现有独立退避重试；硬件不可用、组准备失败不保证自愈。
- 白卡占位 ICCID 不再作为停组恢复目标，恢复目标选择卡上实际存在的已选 Profile。

## 成功与部分完成

只有新线路达到 `sms_ready`，整个任务才返回 `succeeded`。结果保留 `binding_version`、`iccid`、`written`、`profile_state`，增加 `tunnel_state="registered"` 和 `sms_ready=true`。

`profile_state` 表示下载回读时的 Profile 状态，不表示当前多隧道在线情况。多个 Profile 共享读卡器并轮流启用进行鉴权，业务可用性应看 `sms_ready` / `tunnel_state`。

卡已写入但自动开线未完成时，任务返回 `uncertain`，结果仍包含 `written=true` 和新 ICCID，`sms_ready=false`：

- `AUTO_TUNNEL_START_FAILED`：自动配置或运行时接纳失败；结果带 `tunnel_error`。失败的配置变更回滚，已写入的 Profile 保留。
- `TUNNEL_NOT_READY`：自动开线配置已保存，但在任务执行期限内未确认短信就绪。已启动的运行时重试独立于任务响应继续进行。

遇到部分完成必须查询卡片和线路状态，不能换任务 ID 盲目重复下载。重投同一任务仍需保持原 ID 和内容，节点回放已有结果，不会再次写卡。后续线路恢复不追改已经结束的任务结果，使用 `phones.check` / `phones.list` 查询最新状态。

## 换卡与无卡查询

〔源码 2026-09-17〕`phones.check` 和号码级操作按请求中的设备/IMEI 重新读取当前硬件 Profile，不把多隧道期望配置或上一轮缓存当作卡片归属。换卡后，旧槽位不再凭旧配置接受该 ICCID；新槽位即使尚未建组，也能正确识别卡上的 Profile。

`phones.list` 对模块明确返回“SIM not inserted”的槽位保留 IMEI，返回空 ICCID、`available=false`、`reason="未检测到 SIM 卡"`。它与读到 0 个 Profile 的“空卡”不同。其他读卡错误不会伪装为空卡；读取前后发现 IMEI/USB 枚举代次变化仍拒绝本轮清单。

## 验证边界

自动化测试覆盖首次建组、运行组增量开线且旧线路不重建、停用组处理、临时初始化故障恢复、开线未就绪时保留写卡证据、回读失败不入组、配置拒绝回滚和占位 ICCID 恢复目标。

实机版本、部署及指定 IMEI 的验收结果统一记录在项目 `wiki/00-现状速查.md`，不把软件测试等同于真实短信收发通过。

## 已写入旧号的显式补开

〔源码 2026-09-17〕现有 `tunnel.ensure` 可为卡上已存在的指定 ICCID 首次创建多隧道组，也可向运行组增量加入；已经就绪的号码直接返回 changed=false。首次从单线转组时，先实读确认当前在线的单线 Profile 仍在卡上，再将它和请求的 Profile 一起保存，避免补开新号时丢掉原在线号码。不会恢复历史停用组的其它号码。配置接纳失败按原配置回滚。

这个命令用于旧号补开；新号仍由 `esim.profile.download` 完成写入后自动启动，不要求客户端另调开线。
