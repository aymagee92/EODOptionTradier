#!/usr/bin/env python3
import os
import sys
import logging
import shutil
import subprocess
import json
from datetime import datetime, timezone, date as date_type

from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("backendStorage")

PG_DSN = os.environ["PG_DSN"]

ROOT_MOUNT_PATH = os.environ.get("ROOT_MOUNT_PATH", "/")
VOLUME_MOUNT_PATH = os.environ.get("VOLUME_MOUNT_PATH")

def _lsblk_detect_volume_mount() -> str | None:
    if shutil.which("lsblk") is None:
        return None
    try:
        out = subprocess.check_output(
            ["lsblk", "-J", "-b", "-o", "NAME,TYPE,SIZE,MOUNTPOINT"],
            text=True,
        )
        data = json.loads(out)
    except Exception:
        return None

    candidates: list[tuple[int, str]] = []

    def walk(nodes):
        for n in nodes or []:
            mnt = n.get("mountpoint")
            typ = n.get("type")
            size = n.get("size")
            children = n.get("children") or []

            if mnt and typ in ("part", "lvm", "crypt", "disk"):
                try:
                    sz = int(size)
                except Exception:
                    sz = 0

                bad_exact = {"/", "/boot", "/boot/efi"}
                bad_prefixes = ("/run", "/dev", "/proc", "/sys")

                if mnt in bad_exact:
                    pass
                elif any(mnt.startswith(p) for p in bad_prefixes):
                    pass
                else:
                    candidates.append((sz, mnt))

            if children:
                walk(children)

    walk(data.get("blockdevices") or [])

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _df_detect_volume_mount() -> str | None:
    if shutil.which("df") is None:
        return None
    try:
        out = subprocess.check_output(["df", "-B1"], text=True)
    except Exception:
        return None

    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    candidates: list[tuple[int, str]] = []

    for ln in lines[1:]:
        parts = ln.split()
        if len(parts) < 6:
            continue
        fs, total_b, used_b, avail_b, usepct, mnt = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]

        if fs.startswith(("tmpfs", "udev")):
            continue
        if mnt in ("/", "/boot", "/boot/efi"):
            continue
        if mnt.startswith(("/run", "/dev", "/proc", "/sys")):
            continue

        try:
            tb = int(total_b)
        except Exception:
            continue

        candidates.append((tb, mnt))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def detect_volume_mount() -> str:
    if VOLUME_MOUNT_PATH:
        return VOLUME_MOUNT_PATH

    mnt = _lsblk_detect_volume_mount()
    if mnt:
        return mnt

    mnt = _df_detect_volume_mount()
    if mnt:
        return mnt

    return "/"


def get_engine():
    return create_engine(PG_DSN, pool_pre_ping=True)


def ensure_schema(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS disk_usage_daily (
        captured_at        TIMESTAMPTZ PRIMARY KEY,
        root_path          TEXT NOT NULL,
        volume_path        TEXT NOT NULL,

        root_total_bytes   BIGINT NOT NULL,
        root_used_bytes    BIGINT NOT NULL,

        vol_total_bytes    BIGINT NOT NULL,
        vol_used_bytes     BIGINT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS disk_usage_daily_captured_at_idx
    ON disk_usage_daily (captured_at);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def fs_usage_bytes(path: str) -> tuple[int, int]:
    st = os.statvfs(path)
    total = st.f_frsize * st.f_blocks
    free = st.f_frsize * st.f_bfree
    used = total - free
    return int(total), int(used)


def bytes_to_gb(x: int) -> float:
    return float(x) / (1024.0 ** 3)


def pct_used(used: int, total: int) -> float:
    return (used / total * 100.0) if total else 0.0


def record_snapshot(engine, captured_at: datetime | None = None):
    ensure_schema(engine)

    if captured_at is None:
        captured_at = datetime.now(timezone.utc)

    snap_day: date_type = captured_at.date()

    volume_path = detect_volume_mount()

    root_total, root_used = fs_usage_bytes(ROOT_MOUNT_PATH)
    vol_total, vol_used = fs_usage_bytes(volume_path)

    sql = """
    INSERT INTO disk_usage_daily (
        captured_at, root_path, volume_path,
        root_total_bytes, root_used_bytes,
        vol_total_bytes,  vol_used_bytes
    )
    SELECT
        :captured_at, :root_path, :volume_path,
        :root_total_bytes, :root_used_bytes,
        :vol_total_bytes,  :vol_used_bytes
    WHERE NOT EXISTS (
        SELECT 1
        FROM disk_usage_daily
        WHERE captured_at::date = :snap_day
    );
    """

    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "captured_at": captured_at,
                "snap_day": snap_day,
                "root_path": ROOT_MOUNT_PATH,
                "volume_path": volume_path,
                "root_total_bytes": root_total,
                "root_used_bytes": root_used,
                "vol_total_bytes": vol_total,
                "vol_used_bytes": vol_used,
            },
        )

    log.info(
        "SNAPSHOT | day=%s | root %.2f/%.2f GB (%.2f%%) | vol %.2f/%.2f GB (%.2f%%) | volume_path=%s",
        snap_day.isoformat(),
        bytes_to_gb(root_used), bytes_to_gb(root_total), pct_used(root_used, root_total),
        bytes_to_gb(vol_used),  bytes_to_gb(vol_total),  pct_used(vol_used, vol_total),
        volume_path,
    )


def main():
    if len(sys.argv) < 2:
        print("Usage: python backendStorage [init|snapshot]")
        sys.exit(2)

    cmd = sys.argv[1].lower()
    engine = get_engine()

    if cmd == "init":
        ensure_schema(engine)
        log.info("Schema ensured: disk_usage_daily")
        return

    if cmd == "snapshot":
        record_snapshot(engine)
        return

    print(f"Unknown command: {cmd}. Use init or snapshot.")
    sys.exit(2)


if __name__ == "__main__":
    main()


