import { useState } from "react";
import { cx } from "../lib/utils";
import { useI18n } from "../lib/i18n";

const PHRASES = { zh: "我同意并确认", en: "I agree and confirm" } as const;

function WarningGlyph() {
  return (
    <svg className="h-8 w-8 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="2"
        d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
      />
    </svg>
  );
}

function Item({ index, children }: { index: number; children: React.ReactNode }) {
  return (
    <div className="flex items-start">
      <div className="mr-3 mt-0.5 flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-indigo-100 text-xs font-bold text-indigo-700 shadow-sm dark:bg-indigo-900/60 dark:text-indigo-300">
        {index}
      </div>
      <p>{children}</p>
    </div>
  );
}

// 中文许可说明与使用提醒。
function ZhItems() {
  return (
    <>
      <Item index={1}>
        本软件（vocat）依据 MIT License 分发，允许使用、复制、修改、合并、发布、分发、再许可及销售软件副本；完整条款见 LICENSE。
      </Item>
      <Item index={2}>
        本项目用于对自研 / 定制高通模组（首发 Quectel EC20）进行功能验证与故障诊断。仅应使用测试卡、开发卡、实验室卡、授权 eSIM profile，或本人拥有 / 被明确授权测试的 SIM/eSIM 资源；
        <strong className="text-indigo-600 dark:text-indigo-400">不得使用属于他人的生产用订户凭证</strong>。
      </Item>
      <Item index={3}>
        SIM 和短信功能是否可用取决于硬件、套餐及运营商支持。
      </Item>
      <Item index={4}>
        <strong className="text-red-500 dark:text-red-400">禁止用途：</strong>
        未授权接入电信网络、冒用他人订户或设备、SIM 克隆、未授权 eSIM 开通、使用被盗 / 泄露凭证、电信欺诈、大规模群发短信、绕过运营商鉴权或合法限制、未授权拦截 / 监听、干扰移动网络基础设施。
      </Item>
      <Item index={5}>
        软件副本或软件的重要部分须保留原版权声明和 MIT 许可声明。
      </Item>
      <Item index={6}>
        软件按 “AS IS” 提供，不附带任何明示或暗示的担保。作者、维护者、贡献者与分发者不对使用或滥用造成的损失负责，包括 SIM 卡损坏、eSIM profile 丢失、SIM 停用、modem / 基带故障、PCB / 模组 / 宿主设备损坏、网络服务中断、运营商 / 账户限制、数据丢失、监管后果及未授权的电信活动；
        <strong>使用者有责任确保其使用符合适用法律、运营商政策与合同义务</strong>。
      </Item>
      <Item index={7}>
        点击继续即表示你承诺：你的使用是授权的，并符合适用许可、法律、电信法规、运营商政策与测试要求。若拒绝，本软件将被卸载并清理运行环境。
      </Item>
    </>
  );
}

// English license information and usage guidance.
function EnItems() {
  return (
    <>
      <Item index={1}>
        This software (vocat) is distributed under the MIT License, which permits use, copying, modification, merging, publication, distribution, sublicensing, and sale of copies. See LICENSE for the full terms.
      </Item>
      <Item index={2}>
        This project is for function verification and fault diagnosis of custom Qualcomm modules (primarily Quectel
        EC20). Only test, development, or lab SIM cards, authorized eSIM profiles, or SIM/eSIM resources you own or are
        explicitly authorized to test may be used;{" "}
        <strong className="text-indigo-600 dark:text-indigo-400">
          production subscriber credentials belonging to others must not be used
        </strong>
        .
      </Item>
      <Item index={3}>
        SIM and SMS availability depends on hardware, subscription, and carrier support.
      </Item>
      <Item index={4}>
        <strong className="text-red-500 dark:text-red-400">Prohibited uses:</strong>{" "}
        unauthorized network access, impersonating another subscriber or device, SIM cloning, unauthorized eSIM
        provisioning, use of stolen or leaked credentials, telecom fraud, mass SMS sending, bypassing operator
        authentication or lawful restrictions, unauthorized interception, and disrupting mobile network infrastructure.
      </Item>
      <Item index={5}>
        Copies or substantial portions of the software must retain the copyright notice and MIT permission notice.
      </Item>
      <Item index={6}>
        The software is provided &ldquo;AS IS&rdquo; without any express or implied warranty. The authors, maintainers,
        contributors, and distributors are not liable for losses from use or misuse, including SIM damage, eSIM profile
        loss, SIM deactivation, modem/baseband failure, PCB/module/host device damage, network service interruption,
        operator/account restrictions, data loss, regulatory consequences, and unauthorized telecom activity;{" "}
        <strong>the user is responsible for ensuring their use complies with applicable laws, operator policies, and contractual obligations</strong>.
      </Item>
      <Item index={7}>
        Clicking continue signifies that you agree to ensure your use is authorized and complies with the applicable
        license, laws, telecom regulations, operator policies, and testing requirements. If you decline, the software
        will be uninstalled and its environment cleaned up.
      </Item>
    </>
  );
}

// Disclaimer / EULA overlay shown after login (first run requires typing the
// phrase; subsequent periodic confirmations only require a click).
export function Disclaimer({
  firstTime,
  onAgree,
}: {
  firstTime: boolean;
  onAgree: () => void;
}) {
  const { t, lang } = useI18n();
  const zh = lang === "zh";
  const phrase = PHRASES[lang];
  const [typed, setTyped] = useState("");
  const canAgree = !firstTime || typed === phrase;

  function reject() {
    window.close();
  }

  return (
    <div className="disclaimer-overlay fixed inset-0 z-[9999] flex items-center justify-center bg-black/60 backdrop-blur-md">
      <div className="disclaimer-dialog relative mx-4 w-full max-w-lg overflow-hidden rounded-3xl border border-white/20 bg-white/90 p-8 shadow-2xl backdrop-blur-2xl dark:border-gray-700/50 dark:bg-gray-900/90">
        <div className="pointer-events-none absolute left-0 top-0 h-32 w-full bg-gradient-to-b from-indigo-500/20 to-transparent" />
        <div className="disclaimer-dialog-content relative z-10 flex min-h-0 flex-col">
          <div className="disclaimer-icon mx-auto mb-6 flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-[#0ea5e9] to-[#0284c7] shadow-lg shadow-indigo-500/30">
            <WarningGlyph />
          </div>
          <h2 className="disclaimer-title mb-5 text-center text-2xl font-extrabold tracking-tight text-gray-900 dark:text-white">
            {zh ? t("vocat 最终用户许可与免责声明") : "vocat End User License Agreement & Disclaimer"}
          </h2>
          <div className="disclaimer-body space-y-4 text-[14px] font-medium leading-relaxed text-gray-600 dark:text-gray-300">
            {zh ? <ZhItems /> : <EnItems />}
          </div>
          <div className="disclaimer-actions mt-6 border-t border-gray-100 pt-5 dark:border-gray-800">
            {firstTime ? (
              <p className="mb-3 text-center text-xs font-bold text-gray-500 dark:text-gray-400">
                {zh ? t("请输入") : "Please type"}「
                <span className="select-all text-indigo-600 dark:text-indigo-400">{phrase}</span>」
                {zh ? t("以解锁按钮") : "to unlock the button"}
              </p>
            ) : (
              <p className="mb-3 text-center text-xs font-bold text-gray-500 dark:text-gray-400">
                {zh ? t("本次为周期性确认，点击") : "Periodic confirmation. Click"}「
                <span className="text-indigo-600 dark:text-indigo-400">{phrase}</span>」
                {zh ? t("即可继续") : "to continue"}
              </p>
            )}
            {firstTime && (
              <div className="disclaimer-input-wrap mb-5">
                <input
                  type="text"
                  value={typed}
                  onChange={(event) => setTyped(event.target.value)}
                  onPaste={(event) => event.preventDefault()}
                  autoComplete="off"
                  placeholder={zh ? `请输入：${phrase}` : `Please type: ${phrase}`}
                  className="w-full rounded-xl border border-gray-200 bg-gray-50 px-4 py-3 text-center text-sm font-semibold outline-none transition-all placeholder-gray-400 focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/50 dark:border-gray-700 dark:bg-gray-800/80 dark:text-white dark:placeholder-gray-500 dark:focus:border-indigo-500"
                />
              </div>
            )}
            <div className="disclaimer-button-row flex gap-4">
              <button
                type="button"
                onClick={reject}
                className="flex-1 rounded-xl border border-gray-200 bg-gray-50 px-4 py-3 text-sm font-bold tracking-wide text-gray-500 transition-all duration-300 hover:border-red-200 hover:bg-red-50 hover:text-red-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-400 dark:hover:border-red-900/50 dark:hover:bg-red-900/20 dark:hover:text-red-400"
              >
                {zh ? t("拒绝&退出程序") : "Decline & Exit"}
              </button>
              <button
                type="button"
                onClick={() => canAgree && onAgree()}
                disabled={!canAgree}
                className={cx(
                  "flex-[1.5] rounded-xl px-4 py-3 text-sm font-bold tracking-wide transition-all duration-300",
                  canAgree
                    ? "cursor-pointer bg-gradient-to-r from-[#0ea5e9] to-[#0284c7] text-white shadow-lg shadow-indigo-500/30 hover:-translate-y-0.5 hover:shadow-indigo-500/50 active:translate-y-0"
                    : "cursor-not-allowed border border-gray-300 bg-gray-200 text-gray-400 opacity-60 shadow-none dark:border-gray-700 dark:bg-gray-800 dark:text-gray-500",
                )}
              >
                {firstTime ? (zh ? t("同意并继续") : "Agree & Continue") : phrase}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
