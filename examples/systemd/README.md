# graph-loader as a systemd user service

Back to: [Examples](../README.md) | [CLI reference](../../docs/CLI.md) | [Incremental load](../../docs/INCREMENTAL_LOAD.md)

`graph-loader.service` runs one `graph-loader start --mode stream --runtime process` under the **user** service manager: the
command starts every role (lease server, node loaders, edge loaders, retry consumer) as a child process, supervises them, and
systemd supervises the command. No Docker is involved. Tested on a real systemd 255 user manager (see
`plan/library-implementation/evidence/E14.F3.S9.md`).

```bash
python3 -m venv ~/graph-loader/venv && ~/graph-loader/venv/bin/pip install neo4j-graph-loader
install -d -m 700 ~/.config/graph-loader
install -m 600 graph-loader.env.example ~/.config/graph-loader/graph-loader.env     # Neo4j and Kafka settings, mode 600
$EDITOR ~/.config/graph-loader/graph-loader.env ~/graph-loader/schema.yaml
install -D -m 644 graph-loader.service ~/.config/systemd/user/graph-loader.service
systemctl --user daemon-reload && systemctl --user start graph-loader
curl -fsS http://127.0.0.1:8081/readyz                  # 200 once every role is ready and supervised
journalctl --user -u graph-loader -f                    # child lines carry a [graph-loader-<role>-...] prefix
loginctl enable-linger "$USER"                          # keep it running after you log out (needs your admin to allow it)
```

| You do | What happens |
| --- | --- |
| `systemctl --user stop graph-loader` | systemd sends SIGTERM to the command only (`KillMode=mixed`). It stops edge loaders and the retry consumer, then node loaders, then the lease server (about a second on an idle fleet), and exits 0. `TimeoutStopSec=120` is the ceiling before SIGKILL for the whole unit. Nothing is left in the unit's cgroup. |
| a role is killed by the kernel's OOM killer (a `MemoryMax=` that is too low) | `OOMPolicy=continue` (set in the unit) lets the command see it: one `stage=oom ... victim unknown` line in the journal, exit `1`, restart. With systemd's default `OOMPolicy=stop` the whole unit would be stopped before the command could report. |
| a role dies, or the command is killed with `kill -9` | The command stops the fleet and exits non-zero (or dies). systemd removes anything left in the cgroup, waits `RestartSec=10`, and starts the unit again (`Restart=on-failure`). The new run waits for the old consumer-group members to expire (session timeout + 5 s, or `--stream-group-member-wait-seconds`) and resumes from the committed offsets: about 50 s from the kill to `/readyz` in the host test. There is always exactly one fleet. |
| `systemctl --user kill -s TERM graph-loader` | Same as `stop` for the command: exit 0, no restart. |
| a clean stop followed by a wish to run again | `systemctl --user start graph-loader` (a unit that exited 0 is not restarted by `Restart=on-failure`). |

**Do not change `KillMode=mixed`.** With `KillMode=control-group` systemd signals every process at once, so the ordered stop is
skipped: in the host test the lease server was gone before the loaders that still held leases had drained. `KillMode=process`
would leave the children running.

Every start writes an attempt directory under `--run-dir` (registry, child logs, spill and plan files). They are kept after a
stop or a crash so that a failure can be examined; remove old ones yourself. `graph-loader stop --runtime process --recover
--run-dir <attempt>` stops what an abruptly killed command left behind (not needed under systemd, which cleans the cgroup).
