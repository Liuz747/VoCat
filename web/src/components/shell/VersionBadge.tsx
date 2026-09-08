import { useEffect, useState } from "react";
import { api } from "../../api";
import type { SystemInfo } from "../../types";

export function VersionBadge() {
  const [version, setVersion] = useState<string>("");

  useEffect(() => {
    let cancelled = false;
    api<SystemInfo>("/system/info")
      .then((info) => {
        if (!cancelled) setVersion(info?.version ?? "");
      })
      .catch(() => {
        // A failed info probe leaves the badge at its dev fallback; the
        // shell still renders and other clusters are unaffected.
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const label = version ? `v${version}` : "vdev";
  return (
    <span
      className="flex h-7 items-center justify-center rounded-lg px-2 font-mono text-xs text-gray-400 select-none dark:text-gray-500"
      title={version ? `v${version}` : "Development build"}
    >
      {label}
    </span>
  );
}
