#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# setup.sh — One-shot provisioning script for OCI Ubuntu 22.04 instance
#
# Run as root:  sudo bash setup.sh
# Or as ubuntu: bash setup.sh  (will sudo as needed)
# -----------------------------------------------------------------------------
set -euo pipefail

APP_DIR=/opt/polymarket
DATA_DIR=/mnt/data/polymarket
LOG_DIR=/var/log/polymarket
PYTHON=python3.11   # Ubuntu 22.04 ships 3.10; install 3.11 from deadsnakes

# ── 1. System packages ────────────────────────────────────────────────────────
apt-get update -qq
apt-get install -y --no-install-recommends \
    software-properties-common git curl \
    python3.11 python3.11-venv python3.11-dev \
    build-essential libssl-dev libffi-dev

# ── 2. Block volume mount (edit DEVICE as needed) ─────────────────────────────
DEVICE=/dev/sdb          # change to the device shown by `lsblk`
if [ -b "$DEVICE" ]; then
    if ! blkid "$DEVICE" | grep -q ext4; then
        echo "Formatting $DEVICE as ext4..."
        mkfs.ext4 -L polymarket-data "$DEVICE"
    fi
    mkdir -p "$DATA_DIR"
    if ! grep -q "$DEVICE" /etc/fstab; then
        echo "LABEL=polymarket-data  $DATA_DIR  ext4  defaults,nofail  0  2" >> /etc/fstab
    fi
    mount -a
fi
mkdir -p "$DATA_DIR"
chown ubuntu:ubuntu "$DATA_DIR"

# ── 3. Log directory ──────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
chown ubuntu:ubuntu "$LOG_DIR"

# ── 4. App directory + code ───────────────────────────────────────────────────
mkdir -p "$APP_DIR"
chown ubuntu:ubuntu "$APP_DIR"

# Clone or update repo
if [ -d "$APP_DIR/.git" ]; then
    sudo -u ubuntu git -C "$APP_DIR" pull
else
    sudo -u ubuntu git clone \
        https://github.com/yourusername/polymarket-data-pipeline.git "$APP_DIR"
fi

# ── 5. Python virtual environment ─────────────────────────────────────────────
sudo -u ubuntu $PYTHON -m venv "$APP_DIR/.venv"
sudo -u ubuntu "$APP_DIR/.venv/bin/pip" install --upgrade pip wheel
sudo -u ubuntu "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

# ── 6. Environment file (edit with your real keys before running) ─────────────
if [ ! -f "$APP_DIR/.env" ]; then
    cat > "$APP_DIR/.env" <<'EOF'
# Polygon on-chain ticks (optional but recommended)
POLYGON_RPC_URL=
POLYGONSCAN_API_KEY=

# Hugging Face upload
HF_TOKEN=
HF_REPO_ID=yourusername/polymarket-crypto-updown

# Override these if needed
POLYMARKET_DATA_DIR=/mnt/data/polymarket
POLYMARKET_LOG_FILE=/var/log/polymarket/pipeline.log
EOF
    chown ubuntu:ubuntu "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    echo ">>> Edit $APP_DIR/.env with your credentials before starting services."
fi

# ── 7. systemd services ───────────────────────────────────────────────────────
cp "$APP_DIR/deploy/polymarket-live.service"        /etc/systemd/system/
cp "$APP_DIR/deploy/polymarket-historical.service"  /etc/systemd/system/
cp "$APP_DIR/deploy/polymarket-historical.timer"    /etc/systemd/system/

systemctl daemon-reload

systemctl enable polymarket-historical.timer
systemctl start  polymarket-historical.timer

systemctl enable polymarket-live.service
systemctl start  polymarket-live.service

echo ""
echo "=== Setup complete ==="
echo "  Live stream:      systemctl status polymarket-live"
echo "  Historical timer: systemctl status polymarket-historical.timer"
echo "  Logs (live):      journalctl -fu polymarket-live"
echo "  Logs (hist):      tail -f /var/log/polymarket/historical.log"
echo "  Data directory:   $DATA_DIR"
