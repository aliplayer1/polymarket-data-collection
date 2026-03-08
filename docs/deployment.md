# Cloud Deployment & Operations

This document covers server deployment on Hetzner, systemd background architecture, and operational monitoring.

## Server Management

> Essential commands for managing the live deployment on the Hetzner server.

```bash
# Check service and timer statuses
ssh hetzner-root 'systemctl status polymarket-live.service polymarket-historical.timer polymarket-restart.timer'

# Check all active pipeline timers and next scheduled runs
ssh hetzner-root 'systemctl list-timers --all | grep polymarket'

# Follow live pipeline logs (includes on-chain tick backfill and WebSocket stream)
ssh hetzner-root 'journalctl -u polymarket-live.service -f'

# Follow historical 6-hour cron upload logs
ssh hetzner-root 'journalctl -u polymarket-historical.service -f'

# Deploy a code update
git push
ssh hetzner-root 'cd /opt/polymarket && git pull origin main && systemctl restart polymarket-live.service'

# Start/Stop/Restart the live service (re-triggers catch-up backfill)
ssh hetzner-root 'systemctl restart polymarket-live.service'

# Force an immediate Hugging Face dataset sync (bypassing the 6h timer)
ssh hetzner-root 'systemctl start polymarket-historical.service'
```

## Cloud Deployment (Hetzner)

The `deploy/` directory contains everything needed to run the pipeline continuously on a **Hetzner CAX21** instance (Ubuntu 22.04/24.04, ARM64). The same setup works on any Ubuntu 22.04+ server.

### Architecture

```text
Hetzner CAX21 (4 vCPU ARM64 / 8 GB RAM / 80 GB SSD)
  ├── polymarket-live.service      → 24/7 WebSocket stream, auto-restart
  ├── polymarket-historical.timer  → incremental historical re-fetch every 6 h + HF upload
  └── polymarket-restart.timer     → restarts live service daily at 00:05 UTC (new market discovery)

  /opt/polymarket/          ← app code (cloned from GitHub)
  /opt/polymarket/data/     ← Parquet data storage
  /var/log/polymarket/      ← log files

  Hugging Face Hub dataset repo
  └── updated every 6 h via --upload
```

### Quick Start

1. **Push this repo to GitHub** (must be a public repo for unauthenticated HTTPS clone, or use a PAT).

2. **Copy your `.env` to the server:**

```bash
scp .env root@<server-ip>:/tmp/polymarket.env
```

1. **Copy and run the provisioner as root:**

```bash
scp deploy/setup.sh root@<server-ip>:/tmp/setup.sh
ssh root@<server-ip> 'bash /tmp/setup.sh'
```

`setup.sh` will:

- Install system packages and Python
- Create the `polymarket` service user
- Clone the repo to `/opt/polymarket`
- Install the Python virtual environment and dependencies
- Install the systemd service and timer files
- Run the **initial full historical backfill** in the foreground (10–60 minutes)
- Enable and start both services automatically

1. **Verify the services are running:**

```bash
ssh root@<server-ip> 'systemctl status polymarket-live polymarket-historical.timer'
ssh root@<server-ip> 'journalctl -fu polymarket-live'
```

### Deploying Code Updates

```bash
# Push changes from local machine
git push

# Pull and restart on the server
ssh hetzner-root 'cd /opt/polymarket && git pull origin main && systemctl restart polymarket-live.service'
```

If you changed a service or timer file:

```bash
ssh hetzner-root 'git -C /opt/polymarket pull && \
  cp /opt/polymarket/deploy/polymarket-live.service /etc/systemd/system/ && \
  cp /opt/polymarket/deploy/polymarket-historical.service /etc/systemd/system/ && \
  cp /opt/polymarket/deploy/polymarket-historical.timer /etc/systemd/system/ && \
  cp /opt/polymarket/deploy/polymarket-restart.service /etc/systemd/system/ && \
  cp /opt/polymarket/deploy/polymarket-restart.timer /etc/systemd/system/ && \
  systemctl daemon-reload && \
  systemctl enable --now polymarket-restart.timer && \
  systemctl restart polymarket-live'
```
