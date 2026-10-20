# tailmon

Agentless CPU / GPU / RAM / IO / disk monitor for the machines on your [Tailscale](https://tailscale.com) tailnet.

```
tailmon  2026-10-19 19:52:13

server-1         64 cores  load 0.10 0.07 0.07
  CPU  ··········   0.1%
  RAM  █·········   8.0%  10.0G / 125G  swap 27.4M/32.0G
  IO   disk r 0B/s      w 40.0K/s    net rx 37.6K/s   tx 6.2K/s
  HDD  █████·····  50.2%  /                        55.8G  free of 125G    inodes  15.2%
  HDD  ███████···  74.3%  /home                    2.2T   free of 10.5T   inodes   5.9%
  HDD  ███████···  74.1%  total                    2.2T   free of 10.6T   inodes   6.1%
```

- **Zero install on the monitored hosts.** Hosts are discovered with `tailscale status --json`; each online
  Linux peer is probed over SSH by piping a small, standard-library-only Python script to `python3 -`.
  Nothing is copied or left behind. The local machine is probed directly.
- **Single file, no dependencies.** Just Python 3 and the `tailscale` and `ssh` CLIs.
- **Live view.** `-w 1` keeps one SSH session per host open and streams a sample every second. Hosts that
  join or leave the tailnet are picked up automatically, and dropped connections are retried.
- **NVIDIA GPUs** are reported (utilization, memory, temperature) when `nvidia-smi` is available.

## Requirements

On the machine running tailmon:

- Python ≥ 3.6
- `tailscale` CLI, logged in to your tailnet
- `ssh` with key-based (non-interactive) access to the hosts you want to monitor

On each monitored host:

- Linux with `python3` ≥ 3.6 on the `PATH` (the probe reads `/proc` and `/sys`)
- optionally `nvidia-smi` for GPU stats

Non-Linux and offline peers are listed as "not probed".

## Usage

```sh
./tailmon.py                         # one snapshot of every online Linux node
./tailmon.py -w 1                    # live view, refreshed every second
./tailmon.py -w 5                    # ... every 5 seconds
./tailmon.py --hosts web-1,db-1      # only these Tailscale hostnames
./tailmon.py --json                  # machine-readable snapshot
./tailmon.py -w 60 --log m.jsonl     # keep a history: one JSON object per host per sample
./tailmon.py -w 60 --log m.jsonl --quiet   # same, without drawing (for cron / systemd)
```

Colors are green / yellow / red at < 70 % / ≥ 70 % / ≥ 90 %. Set `NO_COLOR=1` to disable them.

## Configuration

Optional, in `~/.config/tailmon.json`. Any key you set overrides the built-in default:

```json
{
  "users": {"gpu-box": "alice"},
  "default_user": "admin",
  "identity": "~/.ssh/id_ed25519",
  "exclude": ["printer-pi"],
  "sample_seconds": 1.0,
  "discover_seconds": 30.0
}
```

| Key                | Meaning                                                                                  |
|--------------------|------------------------------------------------------------------------------------------|
| `users`            | SSH user per Tailscale hostname                                                          |
| `default_user`     | SSH user for hosts not listed in `users` (default `null`: your `~/.ssh/config` / local username) |
| `identity`         | SSH private key passed with `-i` (default `null`: your usual SSH keys / agent)           |
| `exclude`          | Tailscale hostnames never to probe                                                       |
| `sample_seconds`   | Measurement window for rates (CPU %, disk / network throughput) in snapshot mode         |
| `discover_seconds` | In watch mode, how often to re-run `tailscale status` to find new or departed hosts      |

tailmon runs `ssh <short-name>` with the Tailscale address pinned as `HostName`, so `Host <short-name>` entries in
`~/.ssh/config` (`User`, `IdentityFile`, …) apply as they would for a plain `ssh <short-name>`.
SSH connects with `BatchMode=yes` (it never prompts) and `StrictHostKeyChecking=accept-new`.

Nothing is required: with no config file, tailmon connects to each host the way a plain `ssh <host>` would.

## What is measured

| Row    | Source                                                                                          |
|--------|-------------------------------------------------------------------------------------------------|
| CPU    | `/proc/stat`, over the sample interval; plus core count and load average                        |
| RAM    | `/proc/meminfo`: used = total − `MemAvailable`; swap                                            |
| GPU    | `nvidia-smi`: utilization, memory, temperature per GPU                                          |
| IO     | `/proc/diskstats` (physical disks only) and `/proc/net/dev` (excluding `lo`, docker, veth, bridges, tailscale) |
| HDD    | `statvfs` on local filesystems ≥ 1 GiB (ext*, xfs, btrfs, zfs, …), with inode usage when the FS reports it, plus a per-host total |
| TOTAL  | storage summed over all hosts, shown only when at most one Linux host is offline or unreachable (then marked incomplete). Excluded and non-Linux peers don't count |
| USERS  | `who`: interactive logins; people other than the SSH user are highlighted                        |
| SLURM  | `sinfo` / `squeue`, when installed (refreshed at most every 10 s): node state and CPU allocation, then the job queue. Hosts reporting the same cluster show it once |

## License

[MIT](LICENSE)
