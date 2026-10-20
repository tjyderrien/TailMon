#!/usr/bin/env python3
"""tailmon — agentless CPU / GPU / RAM / IO / disk monitor for the machines on your tailnet.

Hosts are discovered with `tailscale status --json`. Each Linux peer is probed over SSH by
piping a small stdlib-only Python script to `python3 -` (nothing is installed remotely);
the local machine is probed directly.

    tailmon.py                    one snapshot of every online Linux node
    tailmon.py -w 1               live view, refreshed every second (one persistent SSH stream per host)
    tailmon.py -w 60 --log m.jsonl   keep a history (one JSON object per host per sample)
    tailmon.py --json             machine-readable snapshot
    tailmon.py --hosts web-1,db-1
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
    # SSH user per Tailscale hostname; hosts not listed use "default_user"
    # (null: whatever ssh picks, i.e. ~/.ssh/config or your local username).
    "users": {},
    "default_user": None,
    # SSH private key passed with -i; null: ssh's usual keys / agent.
    "identity": None,
    "exclude": [],
    "sample_seconds": 1.0,
    # In watch mode, how often to re-run `tailscale status` to pick up hosts that came online / went away.
    "discover_seconds": 30.0,
}

# ---------------------------------------------------------------------------------------------
# Probe: runs on the target machine. Python >= 3.6, standard library only, reads /proc.
# ---------------------------------------------------------------------------------------------
PROBE = r'''
import json, os, pwd, socket, subprocess, sys, time
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

def run(args):
    try:
        r = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True,
                           timeout=10, env=dict(os.environ, LC_ALL="C"))
    except (OSError, subprocess.TimeoutExpired):
        return None
    return r.stdout if r.returncode == 0 else None

def sessions():
    """Interactive logins from utmp (`who`); non-interactive ssh commands, like this probe, don't appear."""
    out = []
    for line in (run(["who"]) or "").splitlines():
        p = line.split()
        if len(p) < 2:
            continue
        frm = p[-1][1:-1] if p[-1].startswith("(") and p[-1].endswith(")") else ""
        since = " ".join(p[2:-1] if frm else p[2:])
        out.append({"user": p[0], "tty": p[1], "from": frm, "since": since})
    return out

SLURM_EVERY = 10.0  # don't hammer slurmctld at the sample rate
slurm_cache = [0.0, None]

def slurm():
    if time.time() - slurm_cache[0] < SLURM_EVERY:
        return slurm_cache[1]
    slurm_cache[0] = time.time()
    nodes_out = run(["sinfo", "-h", "-N", "-o", "%N|%T|%C|%m|%e|%G"])
    jobs_out = run(["squeue", "-h", "-o", "%i|%u|%T|%j|%M|%D|%C|%N|%r"])
    if nodes_out is None and jobs_out is None:
        slurm_cache[1] = None  # no SLURM here
        return None
    nodes, seen = [], set()
    for line in (nodes_out or "").splitlines():
        p = line.split("|")
        if len(p) < 6 or p[0] in seen:  # a node in several partitions is listed once per partition
            continue
        seen.add(p[0])
        cpus = (p[2].split("/") + ["0"] * 4)[:4]  # allocated/idle/other/total
        num = lambda s: int(s) if s.isdigit() else None
        nodes.append({"name": p[0], "state": p[1], "cpu_alloc": num(cpus[0]), "cpu_total": num(cpus[3]),
                      "mem_total": (num(p[3]) or 0) * 2**20, "mem_free": (num(p[4]) or 0) * 2**20, "gres": p[5]})
    jobs = []
    for line in (jobs_out or "").splitlines():
        p = line.split("|", 8)
        if len(p) == 9:
            jobs.append({"id": p[0], "user": p[1], "state": p[2], "name": p[3], "time": p[4],
                         "nodes": p[5], "cpus": p[6], "nodelist": p[7], "reason": p[8]})
    slurm_cache[1] = {"nodes": nodes, "jobs": jobs}
    return slurm_cache[1]

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
        "user": pwd.getpwuid(os.getuid()).pw_name, "sessions": sessions(), "slurm": slurm(),
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
    # Connect by the short Tailscale name so `Host <name>` entries in ~/.ssh/config (User, IdentityFile, ...)
    # apply exactly as for a plain `ssh <name>`, but pin the address to the tailnet one.
    ssh += ["-o", "HostName=%s" % (node["dns"] or node["ip"])]
    host = node["name"]
    return ssh + ["%s@%s" % (user, host) if user else host, "python3"] + probe_args


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
    """Return ([(node, is_self)], [skipped nodes])."""
    me, peers = discover()
    targets, skipped = [(me, True)], []
    for p in peers:
        if only and p["name"] not in only:
            continue
        if p["name"] in cfg["exclude"]:
            continue
        if p["os"] != "linux" or not p["online"]:
            skipped.append(p)
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


def fs_total(fss, label):
    return {"mount": label, **{k: sum(fs.get(k, 0) for fs in fss)
                               for k in ("total", "used", "free", "inodes_total", "inodes_used")}}


def hdd_line(fs):
    p = 100.0 * fs["used"] / fs["total"] if fs["total"] else 0
    # btrfs, vfat, ... report no inode counts (f_files == 0); older probes don't send them at all
    ip = 100.0 * fs["inodes_used"] / fs["inodes_total"] if fs.get("inodes_total") else None
    inodes = "  inodes %s" % c("%5.1f%%" % ip, ip) if ip is not None else ""
    return "  HDD  %s %s  %-24s %-6s free of %-6s%s" % (
        bar(p), c("%5.1f%%" % p, p), fs["mount"][:24], hb(fs["free"]), hb(fs["total"]), inodes)


def render_total(results, skipped):
    """Storage summed over every host; drawn only when at most one Linux host is missing (offline / unreachable)."""
    ok = [r for r in results if "fs" in r]
    missing = sorted([r["node"] for r in results if "fs" not in r] +
                     [p["name"] for p in skipped if p["os"] == "linux" and not p["online"]])
    if len(ok) < 2 or len(missing) > 1:
        return []
    note = c("incomplete: %s missing" % missing[0], 75) if missing else "complete"
    lines = ["TOTAL            %d/%d hosts  %s" % (len(ok), len(ok) + len(missing), note)]
    lines += [hdd_line(fs_total([fs_total(r["fs"], "") for r in ok], "all hosts"))]
    return lines + [""]


def render_users(r):
    """One USERS row: interactive logins, other people highlighted (yellow); nothing if nobody is logged in."""
    by_user = {}
    for s in r.get("sessions") or []:
        by_user.setdefault(s["user"], []).append(s)
    if not by_user:
        return []
    me, parts = r.get("user"), []
    for user in sorted(by_user, key=lambda u: (u == me, u)):
        ss = by_user[user]
        if user == me:
            parts.append("%s (you)%s" % (user, " ×%d" % len(ss) if len(ss) > 1 else ""))
            continue
        where = ", ".join(" ".join(x for x in (s["tty"], s["from"], "since " + s["since"]) if x) for s in ss)
        parts.append(c("%s [%s]" % (user, where), 75))
    others = sum(1 for u in by_user if u != me)
    head = c("%d other" % others, 75) + ": " if others else ""
    return ["  USERS %s%s" % (head, "; ".join(parts))]


SLURM_MAX_JOBS = 15


def render_slurm(r, shown):
    """SLURM nodes + queue. Hosts of the same cluster report the same data: draw it once, under the first one."""
    sl = r.get("slurm")
    if not sl:
        return []
    key = tuple(sorted(n["name"] for n in sl["nodes"])) or ("jobs",) + tuple(j["id"] for j in sl["jobs"])
    if key in shown:
        return ["  SLURM same cluster as %s" % shown[key]]
    shown[key] = r["node"]
    me, lines = r.get("user"), []
    for n in sl["nodes"]:
        cp = 100.0 * n["cpu_alloc"] / n["cpu_total"] if n["cpu_total"] else 0
        mem = "  mem free %s / %s" % (hb(n["mem_free"]), hb(n["mem_total"])) if n["mem_total"] else ""
        gres = "  %s" % n["gres"] if n["gres"] not in ("", "(null)") else ""
        bad = any(s in n["state"] for s in ("down", "drain", "fail"))
        lines.append("  SLURM %s %s  %-14s %s%s %s/%s cpus alloc%s%s" % (
            bar(cp), c("%5.1f%%" % cp, cp), n["name"][:14], c(n["state"], 100) if bad else n["state"],
            " " * max(1, 10 - len(n["state"])), n["cpu_alloc"], n["cpu_total"], mem, gres))
    jobs = sl["jobs"]
    running = [j for j in jobs if j["state"] == "RUNNING"]
    pending = [j for j in jobs if j["state"] == "PENDING"]
    lines.append("  JOBS  %d running, %d pending%s" % (len(running), len(pending),
                                                        ", %d other" % (len(jobs) - len(running) - len(pending))
                                                        if len(jobs) > len(running) + len(pending) else ""))
    order = {"RUNNING": 0, "PENDING": 1}
    for j in sorted(jobs, key=lambda j: order.get(j["state"], 2))[:SLURM_MAX_JOBS]:
        where = j["nodelist"] if j["state"] == "RUNNING" else "(%s)" % j["reason"]
        user = "%-16s" % j["user"][:16]
        user = user if j["user"] == me else c(user, 75)
        lines.append("    %-9s %s %-8s %10s  %4s cpu  %-16s %s" % (
            j["id"][:9], user, j["state"][:8], j["time"], j["cpus"], where[:16], j["name"][:30]))
    if len(jobs) > SLURM_MAX_JOBS:
        lines.append("    … %d more" % (len(jobs) - SLURM_MAX_JOBS))
    return lines


def render(results, skipped):
    w = shutil.get_terminal_size((120, 40)).columns
    lines = [time.strftime("tailmon  %Y-%m-%d %H:%M:%S"), ""]
    slurm_shown = {}
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
            fss.append(fs_total(fss, "total"))
        lines += [hdd_line(fs) for fs in fss]
        lines += render_users(r)
        lines += render_slurm(r, slurm_shown)
        lines.append("")
    lines += render_total(results, skipped)
    if skipped:
        lines.append("not probed: " + ", ".join(
            "%s (%s)" % (p["name"], p["os"] if p["online"] else "offline") for p in skipped))
    return "\n".join(l if len(l) < w + 20 else l[:w + 20] for l in lines)


def draw(results, skipped, live):
    out = render(results, skipped)
    if live and COLOR:
        # home the cursor and overwrite in place (clear to end of each line / screen) to avoid flicker
        out = "\033[H" + out.replace("\n", "\033[K\n") + "\033[K\033[J"
    print(out, flush=True)


def rediscover(streams, logged, cfg, only, interval):
    """Start streams for newly seen hosts and stop those that went away; return the skipped list."""
    targets, skipped = targets_for(cfg, only)
    names = {n["name"] for n, _ in targets}
    for name in set(streams) - names:
        streams.pop(name).stop()
        logged.pop(name, None)
    for n, is_self in targets:
        if n["name"] not in streams:
            streams[n["name"]] = Stream(n, cfg, is_self, interval)
    return skipped


def watch(cfg, only, a):
    streams, logged = {}, {}
    skipped = rediscover(streams, logged, cfg, only, a.watch)
    next_discover = time.time() + cfg["discover_seconds"]
    if not a.quiet and COLOR:
        print("\033[2J", end="")
    try:
        deadline = time.time()
        while True:
            if time.time() >= next_discover:
                next_discover = time.time() + cfg["discover_seconds"]
                try:
                    skipped = rediscover(streams, logged, cfg, only, a.watch)
                except (OSError, ValueError, subprocess.CalledProcessError):
                    pass  # tailscale hiccup: keep the current host list and try again later
            for s in streams.values():
                s.ensure()
            results = [s.result() for s in streams.values()]
            if a.log:
                with open(os.path.expanduser(a.log), "a") as f:
                    for s, r in zip(streams.values(), results):
                        if s.seq != logged.get(s.node["name"], 0):
                            logged[s.node["name"]] = s.seq
                            f.write(json.dumps(r) + "\n")
            if not a.quiet:
                draw(results, skipped, True)
            deadline += a.watch
            time.sleep(max(0.0, deadline - time.time()))
    finally:
        for s in streams.values():
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
