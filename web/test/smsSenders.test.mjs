import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import ts from "typescript";

const source = await readFile(new URL("../src/components/sms/smsSenders.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.ES2022, target: ts.ScriptTarget.ES2022 },
});
const { senderOptions } = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString("base64")}`);

const devices = [
  { id: "usb-2c7c-0123456789abcdef-1-1-3-1", name: "Quectel EC20 / EC25", vowifiRuntime: { localPhone: "+18605550182" } },
  {
    id: "usb-2c7c-0123456789abcdef-1-1-3-4-2",
    name: "Quectel EC20 / EC25",
    multisim: {
      enabled: true,
      lines: [
        { sessionId: "multisim-aaa", phoneNumber: "+18605550123", name: "A 7994", iccidSuffix: "778332", smsReady: true },
        { sessionId: "multisim-bbb", phoneNumber: "", name: "B 8552", iccidSuffix: "779025", smsReady: false },
      ],
    },
  },
  { id: "usb-2c7c-0123456789abcdef-1-1-3-4-4", name: "Quectel EC20 / EC25", multisim: { enabled: false, lines: [] } },
];

test("every line of a multi-tunnel device becomes its own sender, single-line devices stay one entry", () => {
  const senders = senderOptions(devices, "not ready");
  assert.deepEqual(
    senders.map((s) => [s.label, s.deviceId, s.sessionId ?? null, s.disabled]),
    [
      ["+18605550182 · …1-3-1", "usb-2c7c-0123456789abcdef-1-1-3-1", null, false],
      ["+18605550123 · …3-4-2", "usb-2c7c-0123456789abcdef-1-1-3-4-2", "multisim-aaa", false],
      ["B 8552 · …3-4-2 · not ready", "usb-2c7c-0123456789abcdef-1-1-3-4-2", "multisim-bbb", true],
      ["Quectel EC20 / EC25 · …3-4-4", "usb-2c7c-0123456789abcdef-1-1-3-4-4", null, false],
    ],
  );
  assert.equal(new Set(senders.map((s) => s.value)).size, senders.length, "sender values must be unique");
});
