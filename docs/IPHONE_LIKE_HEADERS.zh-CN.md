# 让 IMS 注册/短信的 SIP 头更像 iPhone（T-Mobile US）

VoCat 的 SIP 头由「运营商配置文件」决定（`internal/vowifi/carrier_profiles.json` 内置，
`<数据库目录>/carrier-profiles.d/*.json` 可覆盖，见 `LoadCarrierProfileDirectory`）。
本仓库在 `deploy/carrier-profiles.d/tmobile-us-iphone-like.json` 提供一份 310240/310260 的样例，
把它拷到运行主机的 `carrier-profiles.d/` 后重启 vocat 即生效，不需要改代码。

| 头 | 样例里的值 | 依据 | 可信度 |
|---|---|---|---|
| `Contact` 的 `+sip.instance` | `urn:gsma:imei:TAC8-SNR6-0`（本分支已把代码从 15 位连写改为 GSMA 分段格式） | RFC 7254、T-Mobile Wi-Fi Calling 抓包（Berkeley EECS-2014-156）、vohive 二进制模板 `<urn:gsma:imei:%s-%s-%s>` | 高 |
| `Contact` 特征标签 | `+g.3gpp.smsip;audio;+g.3gpp.icsi-ref=mmtel;video;+g.3gpp.mid-call;+g.3gpp.srvcc-alerting;+g.3gpp.ps2cs-srvcc-orig-pre-alerting` | 3GPP 24.229 手机常见集合；vohive 二进制含同名标签 | 中 |
| `P-Access-Network-Info` | `IEEE-802.11;i-wlan-node-id=<12 位十六进制 MAC>` | T-Mobile 抓包、vohive 模板 `IEEE-802.11; i-wlan-node-id="%s"` | 高（格式）；节点 ID 建议填真实网卡 MAC（`ims.pani_node`） |
| `P-Preferred-Identity` | 开启 | T-Mobile 抓包 | 高 |
| `Supported` | `path, gruu, sec-agree` | vohive 用 `path,sec-agree`；iPhone 会带 sec-agree（Apple 开发者论坛） | 中 |
| `Allow` | `INVITE, ACK, CANCEL, BYE, UPDATE, REFER, NOTIFY, MESSAGE, PRACK, OPTIONS, INFO` | 常见 IMS 终端集合 | 中 |
| `User-Agent` | `iOS/18.2.1 iPhone (iPhone15,4)` | 来自 vohive 二进制里的机型字符串表（它在 T-Mobile 上注册成功过）；**没有公开抓包证实 iPhone 的 IMS User-Agent 原文** | 低，请用 rvictl 抓自己的 iPhone 后替换 |

未做 / 做不到的：
- 真正的 iPhone `User-Agent` 只能从自己的 iPhone 抓（Mac + Xcode `rvictl`，Wireshark 过滤 `sip.Method == "REGISTER"`）。
- `Security-Client` 的算法顺序、Via/Call-ID 的随机形态等实现细节不在配置范围。
- 这些头只影响注册与业务路由；短信被运营商静默不投的问题（见 DEVICE-REPORT §20/§22 与 Note）与头部无关。
