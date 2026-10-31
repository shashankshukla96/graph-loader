# Docker Desktop to native Docker Engine migration — 2026-10-30

Done on `192.168.1.75` at the user's explicit instruction ("remount the disk read write",
then "cleanly uninstall Docker Desktop and install Docker"). This changes the E09
substrate: the live proof now runs on a **native Docker Engine**, no longer Docker Desktop.

## Steps and results

1. **NTFS data disk.** `/media/shashank/Data` (ntfs3 on an LDM dynamic-disk volume, fstab
   `rw`) was mounted read-only. `mount -o remount,rw` succeeded with no repair and no dirty-flag
   change. The user's `graph-loader-neo4j` and `-kafka` then started.
2. **Backup before removal** (`backup_desktop.sh`, `backup-1.log`) to
   `/home/shashank/e09-desktop-migration-backup-20261030-214651/` (2.7 GB, sha256 list
   `SHA256SUMS`): container, network, volume and image inspect data; `~/.docker` config and
   contexts; the unattached volumes `graphloader_neo4j_data` and `graphloader_neo4j_logs`;
   the images neo4j:5-enterprise, cp-kafka:7.7.0, kafka-ui:v0.7.2, postgres:latest,
   python:3.12-slim and the four E09 amd64 candidates. The four user containers were stopped
   gracefully first. Kafka, only a minute into startup, hit the 60 s stop timeout and was
   killed (exit 137); it recovered its logs on restart.
3. **Docker Desktop removed:** `systemctl --user stop`, `apt-get purge docker-desktop`
   (4.37.1), then deletion of `~/.docker/desktop` (about 110 GB, the Desktop VM and
   everything inside it), `desktop-build`, `mutagen`, the `desktop-linux` context, and the
   `credsStore`/`currentContext` entries of `~/.docker/config.json`. The CLI, buildx and
   compose packages were kept (marked manually installed).
4. **Native engine installed** from the already configured Docker apt repository:
   `docker-ce` 5:29.9.0, `containerd.io` 2.4.2, `docker-ce-cli` 29.9.2 (a dependency
   upgrade). Services enabled; cgroup v2/systemd, runc, overlayfs; kernel 7.0.0-38.
   `shashank` was added to the `docker` group (root-equivalent; remove it if undesired).
5. **User stack restored** (`restore_user_stack.py`, `restore-1.log`) from the saved
   configuration with images loaded byte for byte (no registry pulls): networks
   `graph-loader-net` (172.19.0.0/16) and `ws_default` (172.18.0.0/16); containers
   `postgres` (restart=always, data in the host path `/volume/postgres`), `graph-loader-neo4j`,
   `graph-loader-kafka` and `graph-loader-kafka-ui`, all running. Neo4j reports healthy with
   its original database; Kafka still lists the `imdb-*` topics; Postgres accepts connections.
6. **Live smoke repeated on the native engine** (`smoke-3-native.log`): passed — Neo4j
   5.26.32 Enterprise with APOC and query logging, Kafka 7.7.0 topic round trip,
   container DNS/TCP, exact cleanup by ID and an unchanged before/after snapshot of all
   other resources (now including the user's restored stack).

## What was lost

Everything that lived only inside the Desktop VM and was not backed up: the `graph-loader-e08-*`
and unnamed volumes (listed in the backup's `volumes.ndjson`), the E08 test images and the build cache. These were E08 validation
artifacts and are regenerable. The Postgres data and the Neo4j and Kafka data were outside
the VM.

## Caveats

- This is a **shared native engine**: the user's own services run on it and members of
  `docker` can reach the socket. The fenced-daemon requirements were dropped as out of
  scope for a library release in the 2026-10-31 plan reset (D08).
- Update 2026-10-31: `/etc/sudoers.d/e09-claude` was removed, the 186 root-owned
  `/var/tmp/e09-bootstrap-*` directories and the two migration scripts in the home
  directory were deleted, and the user's stack was confirmed running. The migration
  backup under `/home/shashank/` is kept for the user. The host password that was typed
  into the chat earlier should still be rotated by the user.
- Nothing here completes any E09 story.

## Retained files (`var/e09-validation-20261028/entry-smj5281d/ubuntu-live/`)

| File | sha256 | bytes |
| --- | --- | --- |
| `backup_desktop.sh` | `f98bd5c2eaf05ecb95a3d4e317b0daab4889aa755dc664b1ba2c114294e72ba2` | 1715 |
| `backup-1.log` | `670a40079374e901e7e18edb941f1b83f97e4b0b78412e3f152f77840532fb91` | 1112 |
| `restore_user_stack.py` | `b66f5ec09498f4d63aff54f0660df6cb1637df85864aa296fa249878a304ecd8` | 2538 |
| `restore-1.log` | `07a1eea355dd9025e177ff5e7877783b52c9160c3a7ca231f647e191780fb3b4` | 1033 |
| `smoke-3-native.log` | `cb1a857e0664a8d83e09081d18a8ed9ec5e1203cfbc8cafe372e0e99d403fdce` | 1300 |
| `live_smoke.py` | `718883c20bb0d89a2806da5838dc372fb105c9736f7752686737b7bdd7a3fabf` | 10098 |
