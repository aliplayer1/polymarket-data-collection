# Cloud Deployment & Operations

This document covers server deployment on Hetzner, systemd background architecture, and operational monitoring.

## Server Management

> Essential commands for managing the live deployment on the server.

```bash
# Check all active polymarket service and timer statuses
ssh hetzner-root 'systemctl list-units "polymarket*"'

# Check all active pipeline timers and next scheduled runs
ssh hetzner-root 'systemctl list-timers --all | grep polymarket'

# Follow live WebSocket stream logs
ssh hetzner-root 'journalctl -u polymarket-websocket.service -f'

# Follow historical 6-hour fetch and upload logs
ssh hetzner-root 'journalctl -u polymarket-historical.service -f'

# Deploy a code update (no service file changes)
git push
ssh hetzner-root 'cd /opt/polymarket && git pull origin main && systemctl restart polymarket-websocket.service'

# Restart the WebSocket service (re-discovers active markets on startup)
ssh hetzner-root 'systemctl restart polymarket-websocket.service'

# Force an immediate historical fetch + Hugging Face upload (bypasses the 6 h timer)
ssh hetzner-root 'systemctl start polymarket-historical.service'
```

## Cloud Deployment (Hetzner)

The `deploy/` directory contains everything needed to run the pipeline continuously on a **Hetzner CAX21** instance (Ubuntu 22.04/24.04, ARM64). The same setup works on any Ubuntu 22.04+ server.

### Architecture

The pipeline runs as two complementary services that operate concurrently and are safe to run side-by-side.  They are protected by a two-layered locking mechanism:
1.  **In-process**: `threading.RLock` prevents thread-level races.
2.  **Cross-process**: An `fcntl` exclusive lock on `data/.write.lock` prevents process-level races between the WebSocket service and the historical/upload service.

The cross-process lock has a **5-minute (300 s)** timeout.  The `--upload` phase holds this lock only during shard consolidation (seconds), then releases it before the network upload to Hugging Face. The WebSocket service is never blocked — it writes lock-free shard files with unique names.

```text
Hetzner CAX21 (4 vCPU ARM64 / 8 GB RAM / 80 GB SSD)
  ├── polymarket-websocket.service  → 24/7 WebSocket tick stream (--websocket-only), auto-restart
  ├── polymarket-historical.timer   → incremental historical fetch + HF upload every 6 h
  ├── polymarket-upload.timer       → consolidate shard files + HF upload every 3 h
  └── polymarket-restart.timer      → restarts WebSocket service daily at 00:05 UTC (new market discovery)
```

  /opt/polymarket/          ← app code (cloned from GitHub)
  /opt/polymarket/data/     ← Parquet data storage
  /var/log/polymarket/      ← log files

  Hugging Face Hub dataset repo
  └── updated every 6 h via polymarket-historical.service --upload
```

| Unit | Type | Role |
| ---- | ---- | ---- |
| `polymarket-websocket.service` | persistent | Live WebSocket tick stream (`--websocket-only`) |
| `polymarket-historical.service` | oneshot | Historical scan + HF upload (`--historical-only --upload`) |
| `polymarket-historical.timer` | timer (every 6 h) | Triggers `polymarket-historical.service` |
| `polymarket-upload.service` | oneshot | Consolidate shard files + HF upload (`--upload-only`) |
| `polymarket-upload.timer` | timer (every 3 h) | Triggers `polymarket-upload.service` |
| `polymarket-restart.service` | oneshot | Restarts `polymarket-websocket.service` |
| `polymarket-restart.timer` | timer (daily 00:05 UTC) | Triggers `polymarket-restart.service` for market re-discovery |

### Quick Start

1. **Push this repo to GitHub** (must be a public repo for unauthenticated HTTPS clone, or use a PAT).

2. **Copy your `.env` to the server:**

```bash
scp .env root@<server-ip>:/tmp/polymarket.env
```

3. **Copy and run the provisioner as root:**

```bash
scp deploy/setup.sh root@<server-ip>:/tmp/setup.sh
ssh root@<server-ip> 'bash /tmp/setup.sh'
```

`setup.sh` will:

- Install system packages and Python
- Create the `polymarket` service user
- Clone the repo to `/opt/polymarket`
- Install the Python virtual environment and dependencies
- Install all systemd service and timer files
- Run the **initial full historical backfill** in the foreground (10–60 minutes)
- Enable and start all services automatically

4. **Verify the services are running:**

```bash
ssh root@<server-ip> 'systemctl list-units "polymarket*"'
ssh root@<server-ip> 'journalctl -fu polymarket-websocket'
```

### Troubleshooting

#### Hugging Face Upload Crashes (os error 2)
The `polymarket` system user is created without a home directory. If the Hugging Face `upload_folder` (specifically the `hf-xet` backend) cannot find a writable home to store its deduplication index, it may crash with an `I/O error (os error 2)`.

To fix this, ensure you have a writable cache directory and point to it in your `.env`:
```bash
# Create the cache directory
sudo mkdir -p /opt/polymarket/.cache
sudo chown polymarket:polymarket /opt/polymarket/.cache

# Add to .env
HF_HOME=/opt/polymarket/.cache
```

#### Tick Consolidation Fails with a DuckDB Out-of-Memory Error
The upload services (`--upload` / `--upload-only`) consolidate tick shard files before pushing to Hugging Face. If a partition such as `crypto=BTC/timeframe=5-minute` has accumulated many shard files, the consolidation query can still hit the service's effective memory ceiling (often lower than total RAM because of systemd/cgroup limits).

If you see a log like `Out of Memory Error: failed to pin block ...`, check the service-level cap first:
```bash
systemctl show polymarket-upload.service -p MemoryCurrent -p MemoryMax -p MemoryHigh
```

Then, if needed, pin safer DuckDB settings in `/opt/polymarket/.env` and restart the service:
```bash
PM_DUCKDB_MEMORY_LIMIT=3GB
PM_DUCKDB_THREADS=1
```
