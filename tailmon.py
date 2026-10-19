#!/usr/bin/env python3
"""tailmon — agentless CPU / GPU / RAM / IO / disk monitor for the machines on your tailnet.

Hosts are discovered with `tailscale status --json`. Each Linux peer is probed over SSH by
piping a small stdlib-only Python script to `python3 -` (nothing is installed remotely);
the local machine is probed directly.

    tailmon.py                    one snapshot of every online Linux node
    tailmon.py -w 1               live view, refreshed every second (one persistent SSH stream per host)
    tailmon.py -w 60 --log m.jsonl   keep a history (one JSON object per host per sample)
    tailmon.py --json             machine-readable snapshot
    tailmon.py --hosts biatri-1,kastanienallee
"""
import argparse
import concurrent.futures as cf
import json
import os
import shutil
import subprocess
import sys
import threading
import time

CONFIG_PATH = os.path.expanduser("~/.config/tailmon.json")
DEFAULT_CONFIG = {
    # SSH user per Tailscale hostname; hosts not listed use "default_user".
    "users": {"biatri-1": "thibault.derrien", "biatri-2": "thibault.derrien"},
    "default_user": "thibault",
    "identity": "~/.ssh/id_rsa_hilase",
    "exclude": [],
    "sample_seconds": 1.0,
}

# ---------------------------------------------------------------------------------------------
# Probe: runs on the target machine. Python >= 3.6, standard library only, reads /proc.
# ---------------------------------------------------------------------------------------------
PROBE = r'''
import json, os, socket, subprocess, sys, time
SAMPLE = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
COUNT = int(sys.argv[2]) if len(sys.argv) > 2 else 1  # 0 = stream forever, one JSON line per SAMPLE seconds
LOCAL_FS = {"ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "f2fs", "jfs", "reiserfs", "vfat", "exfat", "ntfs", "fuseblk"}

def cpu_times():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    idle = v[3] + (v[4] if len(v) > 4 else 0)
    return sum(v[:8]), idle

def disks_io():
    phys = {d for d in os.listdir("/sys/block") if os.path.exists("/sys/block/%s/device" % d)}
    r = w = 0
    with open("/proc/diskstats") as f:
        for line in f:
            p = line.split()
            if p[2] in phys:
                r += int(p[5]); w += int(p[9])
    return r * 512, w * 512

def net_io():
    rx = tx = 0
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            name, data = line.split(":", 1)
            name = name.strip()
            if name == "lo" or name.startswith(("docker", "veth", "br-", "virbr", "tailscale")):
                continue
            d = data.split()
            rx += int(d[0]); tx += int(d[8])
    return rx, tx

def meminfo():
    m = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            m[k] = int(v.split()[0]) * 1024
    avail = m.get("MemAvailable", m["MemFree"] + m.get("Cached", 0))
    return {"total": m["MemTotal"], "used": m["MemTotal"] - avail,
            "swap_total": m.get("SwapTotal", 0), "swap_used": m.get("SwapTotal", 0) - m.get("SwapFree", 0)}

def filesystems():
    out, seen = [], set()
    with open("/proc/mounts") as f:
        for line in f:
            dev, mnt, fstype = line.split()[:3]
            if fstype not in LOCAL_FS or dev in seen or mnt.startswith(("/snap/", "/boot/efi")):
                continue
            seen.add(dev)
            try:
                s = os.statvfs(mnt)
            except OSError:
                continue
            total = s.f_blocks * s.f_frsize
            if total < 1 << 30:
                continue
            out.append({"mount": mnt, "total": total, "used": total - s.f_bfree * s.f_frsize,
                        "free": s.f_bavail * s.f_frsize,
                        "inodes_total": s.f_files, "inodes_used": s.f_files - s.f_ffree})
    return out

def gpus():
    try:
        r = subprocess.run(["nvidia-smi", "--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu",
                            "--format=csv,noheader,nounits"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                           universal_newlines=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return []
    out = []
    for line in r.stdout.strip().splitlines():
        p = [x.strip() for x in line.split(",")]
        num = lambda s: float(s) if s.replace(".", "", 1).isdigit() else None
        out.append({"index": int(p[0]), "name": p[1], "util": num(p[2]),
                    "mem_used": (num(p[3]) or 0) * 2**20, "mem_total": (num(p[4]) or 0) * 2**20, "temp": num(p[5])})
    return out

def counters():
    return time.time(), cpu_times(), disks_io(), net_io()

def report(prev, cur):
    t0, (c0, i0), (dr0, dw0), (nr0, nt0) = prev
    t1, (c1, i1), (dr1, dw1), (nr1, nt1) = cur
    dt = t1 - t0
    return json.dumps({
        "hostname": socket.gethostname(), "time": t1, "ncpu": os.cpu_count(), "load": os.getloadavg(),
        "cpu": 100.0 * (1 - (i1 - i0) / max(c1 - c0, 1)),
        "mem": meminfo(),
        "disk_io": {"read": (dr1 - dr0) / dt, "write": (dw1 - dw0) / dt},
        "net_io": {"rx": (nr1 - nr0) / dt, "tx": (nt1 - nt0) / dt},
        "fs": filesystems(), "gpus": gpus(),
    })

prev, n = counters(), 0
deadline = prev[0]
while True:
    deadline += SAMPLE
    time.sleep(max(0.0, deadline - time.time()))
    cur = counters()
    print(report(prev, cur), flush=True)
    prev, n = cur, n + 1
    if COUNT and n >= COUNT:
        break
'''


# ---------------------------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------------------------
def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg.update(json.load(f))
    return cfg


def discover():
    """Return (self_node, [peer nodes]) from tailscale; each node is a dict."""
    st = json.loads(subprocess.run(["tailscale", "status", "--json"], check=True,
                                   stdout=subprocess.PIPE).stdout)

    def node(p):
        return {"name": p["DNSName"].split(".")[0] or p["HostName"], "dns": p["DNSName"].rstrip("."),
                "ip": (p.get("TailscaleIPs") or [None])[0], "os": p.get("OS", ""), "online": p.get("Online", False)}
    return node(st["Self"]), [node(p) for p in (st.get("Peer") or {}).values()]


def probe_cmd(node, cfg, is_self, sample, count):
    probe_args = ["-", str(sample), str(count)]
    if is_self:
        return [sys.executable] + probe_args
    user = cfg["users"].get(node["name"], cfg["default_user"])
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-o", "StrictHostKeyChecking=accept-new",
           "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=3"]
    if cfg.get("identity"):
        ssh += ["-i", os.path.expanduser(cfg["identity"])]
    return ssh + ["%s@%s" % (user, node["dns"] or node["ip"]), "python3"] + probe_args


def probe(node, cfg, is_self):
    args = probe_cmd(node, cfg, is_self, cfg["sample_seconds"], 1)
    try:
        r = subprocess.run(args, input=PROBE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True, timeout=30 + cfg["sample_seconds"])
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    if r.returncode != 0:
        err = (r.stderr.strip().splitlines() or ["exit %d" % r.returncode])[-1]
        return {"error": err[:120]}
    return json.loads(r.stdout)


def targets_for(cfg, only=None):
    """Return ([(node, is_self)], [skipped descriptions])."""
    me, peers = discover()
    targets, skipped = [(me, True)], []
    for p in peers:
        if only and p["name"] not in only:
            continue
        if p["name"] in cfg["exclude"]:
            continue
        if p["os"] != "linux" or not p["online"]:
            skipped.append("%s (%s)" % (p["name"], p["os"] if p["online"] else "offline"))
            continue
        targets.append((p, False))
    if only:
        targets = [t for t in targets if t[0]["name"] in only]
    return targets, skipped


def collect(cfg, only=None):
    targets, skipped = targets_for(cfg, only)
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        futs = [(n, ex.submit(probe, n, cfg, s)) for n, s in targets]
        results = [dict(f.result(), node=n["name"]) for n, f in futs]
    return results, skipped


class Stream:
    """One long-lived probe per host that prints a sample every `interval` seconds; reconnects if it dies."""
    RETRY = 10.0

    def __init__(self, node, cfg, is_self, interval):
        self.node, self.cfg, self.is_self, self.interval = node, cfg, is_self, interval
        self.proc, self.latest, self.received, self.seq, self.retry_at = None, None, 0.0, 0, 0.0

    def ensure(self):
        if (self.proc and self.proc.poll() is None) or time.time() < self.retry_at:
            return
        self.retry_at = time.time() + self.RETRY
        self.proc = subprocess.Popen(probe_cmd(self.node, self.cfg, self.is_self, self.interval, 0),
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     universal_newlines=True, bufsize=1)
        self.proc.stdin.write(PROBE)
        self.proc.stdin.close()
        threading.Thread(target=self._read, args=(self.proc,), daemon=True).start()

    def _read(self, proc):
        for line in proc.stdout:
            try:
                self.latest = json.loads(line)
            except ValueError:
                continue
            self.received, self.seq = time.time(), self.seq + 1
        err = proc.stderr.read().strip().splitlines()
        code = proc.wait()
        self.latest = {"error": (err or ["exit %d" % code])[-1][:120]}
        self.received, self.seq = time.time(), self.seq + 1

    def result(self):
        if self.latest is None:
            r = {"pending": "connecting…"}
        elif "error" not in self.latest and time.time() - self.received > max(3 * self.interval, 15):
            r = {"error": "no data for %ds" % (time.time() - self.received)}
        else:
            r = self.latest
        return dict(r, node=self.node["name"])

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()


# ---------------------------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------------------------
COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


def c(text, pct):
    if not COLOR or pct is None:
        return text
    code = "31" if pct >= 90 else "33" if pct >= 70 else "32"
    return "\033[%sm%s\033[0m" % (code, text)


def hb(n, rate=False):
    for unit in ("B", "K", "M", "G", "T", "P"):
        if abs(n) < 1024 or unit == "P":
            s = ("%.0f%s" if unit == "B" or n >= 100 else "%.1f%s") % (n, unit)
            return s + "/s" if rate else s
        n /= 1024.0


def bar(pct, width=10):
    filled = int(round(pct / 100.0 * width))
    return c("█" * filled + "·" * (width - filled), pct)


def render(results, skipped):
    w = shutil.get_terminal_size((120, 40)).columns
    lines = [time.strftime("tailmon  %Y-%m-%d %H:%M:%S"), ""]
    for r in sorted(results, key=lambda r: r["node"]):
        if "pending" in r:
            lines.append("%-16s %s" % (r["node"], r["pending"]))
            lines.append("")
            continue
        if "error" in r:
            lines.append("%-16s %s" % (r["node"], c("UNREACHABLE: " + r["error"], 100)))
            lines.append("")
            continue
        m = r["mem"]
        mem_pct = 100.0 * m["used"] / m["total"]
        lines.append("%-16s %d cores  load %.2f %.2f %.2f" % ((r["node"], r["ncpu"]) + tuple(r["load"])))
        lines.append("  CPU  %s %s" % (bar(r["cpu"]), c("%5.1f%%" % r["cpu"], r["cpu"])))
        swap = "  swap %s/%s" % (hb(m["swap_used"]), hb(m["swap_total"])) if m["swap_total"] else ""
        lines.append("  RAM  %s %s  %s / %s%s" % (bar(mem_pct), c("%5.1f%%" % mem_pct, mem_pct),
                                                    hb(m["used"]), hb(m["total"]), swap))
        for g in r["gpus"]:
            mp = 100.0 * g["mem_used"] / g["mem_total"] if g["mem_total"] else 0
            util = "%5.1f%%" % g["util"] if g["util"] is not None else "  n/a "
            temp = "  %d°C" % g["temp"] if g["temp"] is not None else ""
            lines.append("  GPU%d %s %s  mem %s / %s  %s%s" % (g["index"], bar(g["util"] or 0), c(util, g["util"]),
                                                              hb(g["mem_used"]), hb(g["mem_total"]), g["name"], temp))
        lines.append("  IO   disk r %-9s w %-9s  net rx %-9s tx %s" % (
            hb(r["disk_io"]["read"], True), hb(r["disk_io"]["write"], True),
            hb(r["net_io"]["rx"], True), hb(r["net_io"]["tx"], True)))
        fss = list(r["fs"])
        if len(fss) > 1:
            fss.append({"mount": "total", **{k: sum(fs.get(k, 0) for fs in fss)
                                             for k in ("total", "used", "free", "inodes_total", "inodes_used")}})
        for fs in fss:
            p = 100.0 * fs["used"] / fs["total"]
            # btrfs, vfat, ... report no inode counts (f_files == 0); older probes don't send them at all
            ip = 100.0 * fs["inodes_used"] / fs["inodes_total"] if fs.get("inodes_total") else None
            inodes = "  inodes %s" % c("%5.1f%%" % ip, ip) if ip is not None else ""
            lines.append("  HDD  %s %s  %-24s %-6s free of %-6s%s" % (
                bar(p), c("%5.1f%%" % p, p), fs["mount"][:24], hb(fs["free"]), hb(fs["total"]), inodes))
        lines.append("")
    if skipped:
        lines.append("not probed: " + ", ".join(skipped))
    return "\n".join(l if len(l) < w + 20 else l[:w + 20] for l in lines)


def draw(results, skipped, live):
    out = render(results, skipped)
    if live and COLOR:
        # home the cursor and overwrite in place (clear to end of each line / screen) to avoid flicker
        out = "\033[H" + out.replace("\n", "\033[K\n") + "\033[K\033[J"
    print(out, flush=True)


def watch(cfg, only, a):
    targets, skipped = targets_for(cfg, only)
    streams = [Stream(n, cfg, s, a.watch) for n, s in targets]
    logged = {}
    if not a.quiet and COLOR:
        print("\033[2J", end="")
    try:
        deadline = time.time()
        while True:
            for s in streams:
                s.ensure()
            results = [s.result() for s in streams]
            if a.log:
                with open(os.path.expanduser(a.log), "a") as f:
                    for s, r in zip(streams, results):
                        if s.seq != logged.get(s.node["name"], 0):
                            logged[s.node["name"]] = s.seq
                            f.write(json.dumps(r) + "\n")
            if not a.quiet:
                draw(results, skipped, True)
            deadline += a.watch
            time.sleep(max(0.0, deadline - time.time()))
    finally:
        for s in streams:
            s.stop()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-w", "--watch", type=float, metavar="SEC", help="refresh every SEC seconds")
    ap.add_argument("--json", action="store_true", help="print a JSON snapshot and exit")
    ap.add_argument("--log", metavar="FILE", help="append each sample as JSON lines to FILE")
    ap.add_argument("--hosts", help="comma-separated Tailscale hostnames to restrict to")
    ap.add_argument("--quiet", action="store_true", help="with --log: don't draw the table (for cron/systemd)")
    a = ap.parse_args()
    cfg = load_config()
    only = set(a.hosts.split(",")) if a.hosts else None

    if a.watch and not a.json:
        return watch(cfg, only, a)
    results, skipped = collect(cfg, only)
    if a.log:
        with open(os.path.expanduser(a.log), "a") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")
    if a.json:
        print(json.dumps(results, indent=2))
    elif not a.quiet:
        draw(results, skipped, False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
