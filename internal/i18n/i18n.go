// Package i18n translates the backend's user-facing Chinese strings into
// English when the persisted UI language is English.
//
// The SPA keeps its own Chinese-keyed dictionary (web/src/lib/i18n-en.ts) for
// static UI copy. This package covers the strings the Go backend generates as
// DATA — status text, error messages, probe hints, and country names — which
// the SPA renders verbatim and therefore cannot translate client-side.
//
// The product serves a single administrator whose language is persisted in the
// settings store, so a process-level language value is sufficient; the server
// refreshes it on startup and whenever the preference is read or written.
package i18n

import (
	"fmt"
	"sync/atomic"
)

// current holds the active UI language: "en" or "zh". It defaults to Chinese so
// the backend behaves exactly as it always has unless the persisted preference
// is explicitly English — the server syncs it from the settings store on
// startup and on every preferences read/write.
var current atomic.Value // stores string

func init() {
	current.Store("zh")
}

// Set records the active UI language. Anything other than "zh" is treated as
// English, matching the SPA's fallback.
func Set(language string) {
	if language == "zh" {
		current.Store("zh")
		return
	}
	current.Store("en")
}

// Lang reports the active UI language ("en" or "zh").
func Lang() string {
	if language, ok := current.Load().(string); ok {
		return language
	}
	return "zh"
}

// T translates a Chinese string to English when the active language is
// English; otherwise it returns the input unchanged. Strings with no entry are
// returned as-is (Chinese), mirroring the SPA dictionary's fallback.
func T(zh string) string {
	if Lang() != "en" {
		return zh
	}
	if en, ok := zhToEn[zh]; ok {
		return en
	}
	return zh
}

// Tf translates a Chinese printf-style template and then formats it with args,
// so interpolated values land inside the translated sentence.
func Tf(template string, args ...any) string {
	return fmt.Sprintf(T(template), args...)
}

// zhToEn maps every user-facing Chinese string the backend emits onto its
// English equivalent. Keys must match the source string byte-for-byte.
var zhToEn = map[string]string{
	// ---- eSIM ----
	"已启用": "Enabled",
	"已禁用": "Disabled",
	"该 eSIM 操作暂未实现：当前仅支持列出 Profile 与切卡（启用已安装的 Profile），不支持下载/删除/改名": "This eSIM operation is not available: only listing profiles and switching (enabling an already-installed profile) are supported; download, delete, and rename are not.",

	// ---- devices ----
	"设备数量已达上限，最多只能添加 %d 台设备":  "Device limit reached; at most %d devices can be added.",
	"请先禁用该设备已绑定的导出代理，再关闭漫游数据": "Disable the export proxy bound to this device before turning off roaming data.",

	// ---- settings / update ----
	"未配置受信任的软件更新源；不会从未知地址下载或执行文件。": "No trusted update source is configured; no files will be downloaded or executed from unknown addresses.",
	"未配置受信任的软件更新源；未执行任何更新。":        "No trusted update source is configured; no update was performed.",

	// ---- proxy probe / save ----
	"检查地址、端口、防火墙与上游代理监听状态。":                       "Check the address, port, firewall, and that the upstream proxy is listening.",
	"该代理不能承载 ePDG 所需的 UDP；启用上游 SOCKS5 UDP 转发后重试。": "This proxy cannot carry the UDP that ePDG requires; enable upstream SOCKS5 UDP forwarding and retry.",
	"TCP 握手、认证和 UDP ASSOCIATE 均通过。":               "TCP handshake, authentication, and UDP ASSOCIATE all passed.",
	"代理已保存；UDP ASSOCIATE 尚未通过。":                   "Proxy saved; UDP ASSOCIATE has not passed yet.",
	"代理已保存，SOCKS5 认证与 UDP ASSOCIATE 均通过。":         "Proxy saved; SOCKS5 authentication and UDP ASSOCIATE both passed.",
	"代理不能承载 VoWiFi 所需的 UDP。":                      "The proxy cannot carry the UDP that VoWiFi requires.",
	"SOCKS5 认证与 UDP ASSOCIATE 探测通过。":              "SOCKS5 authentication and UDP ASSOCIATE probe passed.",

	// ---- country names (upstream proxy country rules) ----
	"中国":    "China",
	"中国香港":  "Hong Kong (China)",
	"中国澳门":  "Macau (China)",
	"中国台湾":  "Taiwan (China)",
	"美国":    "United States",
	"加拿大":   "Canada",
	"英国":    "United Kingdom",
	"德国":    "Germany",
	"法国":    "France",
	"意大利":   "Italy",
	"西班牙":   "Spain",
	"葡萄牙":   "Portugal",
	"荷兰":    "Netherlands",
	"比利时":   "Belgium",
	"瑞士":    "Switzerland",
	"奥地利":   "Austria",
	"爱尔兰":   "Ireland",
	"丹麦":    "Denmark",
	"瑞典":    "Sweden",
	"挪威":    "Norway",
	"芬兰":    "Finland",
	"波兰":    "Poland",
	"捷克":    "Czechia",
	"希腊":    "Greece",
	"罗马尼亚":  "Romania",
	"匈牙利":   "Hungary",
	"乌克兰":   "Ukraine",
	"俄罗斯":   "Russia",
	"土耳其":   "Türkiye",
	"日本":    "Japan",
	"韩国":    "South Korea",
	"新加坡":   "Singapore",
	"马来西亚":  "Malaysia",
	"泰国":    "Thailand",
	"越南":    "Vietnam",
	"菲律宾":   "Philippines",
	"印度尼西亚": "Indonesia",
	"印度":    "India",
	"巴基斯坦":  "Pakistan",
	"阿联酋":   "United Arab Emirates",
	"沙特阿拉伯": "Saudi Arabia",
	"以色列":   "Israel",
	"澳大利亚":  "Australia",
	"新西兰":   "New Zealand",
	"巴西":    "Brazil",
	"墨西哥":   "Mexico",
	"阿根廷":   "Argentina",
	"智利":    "Chile",
	"哥伦比亚":  "Colombia",
	"南非":    "South Africa",
	"埃及":    "Egypt",
	"尼日利亚":  "Nigeria",
	"肯尼亚":   "Kenya",
}
