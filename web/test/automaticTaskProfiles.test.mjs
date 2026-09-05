import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import ts from "typescript";

const source = await readFile(new URL("../src/lib/automaticTaskProfiles.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.ES2022,
    target: ts.ScriptTarget.ES2022,
  },
});
const moduleURL = `data:text/javascript;base64,${Buffer.from(compiled.outputText).toString("base64")}`;
const {
  buildAutomaticTaskProfileOptions,
  createAutomaticTaskProfileRequestGuard,
  selectAutomaticTaskProfileOption,
} = await import(moduleURL);

test("uses the current physical SIM when the device has no eSIM profiles", () => {
  const iccid = "8944100000000000001";

  assert.deepEqual(buildAutomaticTaskProfileOptions([], iccid, "Current SIM"), [
    {
      iccid,
      aidHex: "",
      label: `Current SIM · ${iccid}`,
    },
  ]);
});

test("does not duplicate the current SIM when it is already in the eSIM inventory", () => {
  const iccid = "8944100000000000001";

  assert.deepEqual(
    buildAutomaticTaskProfileOptions(
      [{ aidHex: "a0000005591010ffffffff8900000100", profiles: [{ iccid, name: "Travel" }] }],
      iccid,
      "Current SIM",
    ),
    [
      {
        iccid,
        aidHex: "a0000005591010ffffffff8900000100",
        label: `Travel · ${iccid}`,
      },
    ],
  );
});

test("does not replace a saved profile when a failed inventory only exposes the current SIM", () => {
  const currentICCID = "8944100000000000001";
  const savedICCID = "89104100000028106378";
  const options = buildAutomaticTaskProfileOptions([], currentICCID, "Current SIM");

  assert.equal(selectAutomaticTaskProfileOption(options, savedICCID), undefined);
});

test("accepts state updates only from the latest profile request", () => {
  const guard = createAutomaticTaskProfileRequestGuard();
  const first = guard.begin();
  const second = guard.begin();

  assert.equal(guard.isCurrent(first), false);
  assert.equal(guard.isCurrent(second), true);
  guard.invalidate();
  assert.equal(guard.isCurrent(second), false);
});

const {
  toggleRotationProfile,
  moveRotationProfile,
  estimateRotationCycleSeconds,
} = await import(moduleURL);

const a = { iccid: "8901240527185779025", aidHex: "A0", label: "+18602628552 · 8901240527185779025" };
const b = { iccid: "8901240527185778332", aidHex: "A0", label: "+18609847994 · 8901240527185778332" };
const c = { iccid: "8901240527185778316", aidHex: "A0", label: "+19592010936 · 8901240527185778316" };

test("toggling appends a profile in click order and removes it again", () => {
  const once = toggleRotationProfile([], b);
  const twice = toggleRotationProfile(once, a);
  assert.deepEqual(twice.map((p) => p.iccid), [b.iccid, a.iccid]);
  assert.deepEqual(toggleRotationProfile(twice, b).map((p) => p.iccid), [a.iccid]);
});

test("moving a profile reorders the rotation and clamps at the ends", () => {
  const list = [a, b, c];
  assert.deepEqual(moveRotationProfile(list, c.iccid, -1).map((p) => p.iccid), [a.iccid, c.iccid, b.iccid]);
  assert.deepEqual(moveRotationProfile(list, a.iccid, -1).map((p) => p.iccid), [a.iccid, b.iccid, c.iccid]);
  assert.deepEqual(moveRotationProfile(list, c.iccid, 1).map((p) => p.iccid), [a.iccid, b.iccid, c.iccid]);
  assert.deepEqual(moveRotationProfile(list, "missing", 1).map((p) => p.iccid), [a.iccid, b.iccid, c.iccid]);
});

test("cycle estimate counts one switch plus one dwell per profile", () => {
  assert.equal(estimateRotationCycleSeconds(3, 30), 3 * (10 + 30));
  assert.equal(estimateRotationCycleSeconds(0, 30), 0);
});

const { splitLocalDateTime } = await import(moduleURL);

test("splitLocalDateTime maps an instant to local date/time inputs and treats the zero time as unset", () => {
  const instant = new Date(2030, 0, 2, 9, 30); // local wall clock
  assert.deepEqual(splitLocalDateTime(instant.toISOString()), { date: "2030-01-02", time: "09:30" });
  assert.deepEqual(splitLocalDateTime("0001-01-01T00:00:00Z"), { date: "", time: "" });
  assert.deepEqual(splitLocalDateTime(""), { date: "", time: "" });
  assert.deepEqual(splitLocalDateTime("garbage"), { date: "", time: "" });
});
