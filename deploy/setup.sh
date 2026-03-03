#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# setup.sh — One-shot provisioning for a Hetzner Ubuntu 22.04/24.04 server
#
# Prerequisites (run these locally before this script):
#   1. Push your code to GitHub (see deployment guide in README)
#   2. Copy your .env to the server:
#        scp .env root@<server-ip>:/tmp/polymarket.env
#
# Then, on the server as root:
#   bash <(curl -fsSL https://raw.githubusercontent.com/YOURUSER/YOURREPO/main/deploy/setup.sh)
#
# Or if you've already cloned/copied setup.sh to the server:
#   bash /path/to/setup.sh
# -----------------------------------------------------------------------------
set -euo pipefail

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  EDIT THIS before running                                               │
# └─────────────────────────────────────────────────────────────────────────┘
REPO_URL="https://github.com/aliplayer1/polymarket-data-collection.git"   # GitHub repo

APP_DIR=/opt/polymarket
DATA_DIR=/opt/polymarket/data
LOG_DIR=/var/log/polymarket
SERVICE_USER=polymarket

# ── 1. System packages ────────────────────────────────────────────────────────
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    git curl build-essential libssl-dev libffi-dev \
    python3 python3-venv python3-dev python3-pip lsb-release

# On Ubuntu 22.04 the default python3 is 3.10; install 3.11 for compatibility.
UBUNTU_VER=$(lsb_release -rs 2>/dev/null || echo "24.04")
if [[ "$UBUNTU_VER" == "22.04" ]]; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        software-properties-common
    add-apt-repository -y ppa:deadsnakes/ppa
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        python3.11 python3.11-venv python3.11-dev
    PYTHON=python3.11
else
    # Ubuntu 24.04+ ships python3.12
    PYTHON=python3
fi

# ── 2. Service user ───────────────────────────────────────────────────────────
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi

# ── 3. Directories (data + log only; app dir is created by git clone) ─────────
mkdir -p "$DATA_DIR" "$LOG_DIR"
chown "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR" "$LOG_DIR"

# ── 4. Clone or update code from GitHub ───────────────────────────────────────
# Run git as root (this script already runs as root) to avoid credential
# prompts that a nologin system user cannot answer.
# Allow root to operate on a directory owned by the service user.
git config --global --add safe.directory "$APP_DIR"
if [ -d "$APP_DIR/.git" ]; then
    echo "Updating existing repository..."
    git -C "$APP_DIR" pull --ff-only
else
    rm -rf "$APP_DIR"
    echo "Cloning repository..."
    git clone "$REPO_URL" "$APP_DIR"
fi
chown -R "$SERVICE_USER:$SERVICE_USER" "$APP_DIR"

# ── 5. Install .env (copied from local machine before running this script) ────
ENV_FILE="$APP_DIR/.env"
if [ -f /tmp/polymarket.env ] && [ ! -f "$ENV_FILE" ]; then
    mv /tmp/polymarket.env "$ENV_FILE"
    chown "$SERVICE_USER:$SERVICE_USER" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "Installed .env from /tmp/polymarket.env"
elif [ ! -f "$ENV_FILE" ]; then
    # Create a blank template so the user knows what to fill in
    cat > "$ENV_FILE" <<'EOF'
# ── Hugging Face (required for --upload) ─────────────────────────────────────
HF_TOKEN=
HF_REPO_ID=yourusername/polymarket-crypto-updown

# ── Polygon on-chain tick data (optional but recommended) ─────────────────────
POLYGON_RPC_URL=https://polygon-bor-rpc.publicnode.com
POLYGONSCAN_API_KEY=

# ── Pipeline paths ─────────────────────────────────────────────────────────────
POLYMARKET_DATA_DIR=/opt/polymarket/data
POLYMARKET_LOG_FILE=/var/log/polymarket/pipeline.log
EOF
    chown "$SERVICE_USER:$SERVICE_USER" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
fi

# ── 6. Python virtual environment ─────────────────────────────────────────────
if [ ! -d "$APP_DIR/.venv" ]; then
    sudo -u "$SERVICE_USER" $PYTHON -m venv "$APP_DIR/.venv"
fi
sudo -u "$SERVICE_USER" "$APP_DIR/.venv/bin/pip" install --upgrade pip wheel -q
sudo -u "$SERVICE_USER" "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt" -q

# ── 7. systemd service files ──────────────────────────────────────────────────
cp "$APP_DIR/deploy/polymarket-websocket.service"   /etc/systemd/system/
cp "$APP_DIR/deploy/polymarket-historical.service"  /etc/systemd/system/
cp "$APP_DIR/deploy/polymarket-historical.timer"    /etc/systemd/system/
cp "$APP_DIR/deploy/polymarket-restart.service"     /etc/systemd/system/
cp "$APP_DIR/deploy/polymarket-restart.timer"       /etc/systemd/system/
systemctl daemon-reload

# ── 8. Check credentials before proceeding ────────────────────────────────────
HF_TOKEN_VAL=$(grep -E '^HF_TOKEN=' "$ENV_FILE" | cut -d= -f2- | tr -d '[:space:]')
if [ -z "$HF_TOKEN_VAL" ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "  IMPORTANT: .env credentials are not set."
    echo ""
    echo "  Copy your local .env to the server, then re-run this script:"
    echo "    scp .env root@<server-ip>:/tmp/polymarket.env"
    echo "    bash $0"
    echo ""
    echo "  Or edit directly on the server:"
    echo "    nano $ENV_FILE"
    echo "    # Set HF_TOKEN and HF_REPO_ID, then re-run this script"
    echo "════════════════════════════════════════════════════════════════════"
    exit 0
fi

# ── 9. Initial full historical backfill ───────────────────────────────────────
echo ""
echo "Running initial historical backfill — this may take 10–60 minutes..."
echo "Logs → $LOG_DIR/pipeline.log"
echo ""
sudo -u "$SERVICE_USER" bash -c "
    set -a
    source '$ENV_FILE'
    set +a
    cd '$APP_DIR'
    .venv/bin/python -m polymarket_pipeline --historical-only --upload
"

# ── 10. Enable and start continuous services ──────────────────────────────────
systemctl enable --now polymarket-websocket.service
systemctl enable --now polymarket-historical.timer
systemctl enable --now polymarket-restart.timer

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  Live WebSocket stream:"
echo "    systemctl status polymarket-websocket"
echo "    journalctl -fu polymarket-websocket"
echo ""
echo "  Historical sync (every 6 h) + HF upload:"
echo "    systemctl status polymarket-historical.timer"
echo "    journalctl -fu polymarket-historical"
echo ""
echo "  Daily WebSocket restart (market re-discovery at 00:05 UTC):"
echo "    systemctl status polymarket-restart.timer"
echo ""
echo "  Data directory: $DATA_DIR"
echo "  Log file:       $LOG_DIR/pipeline.log"
echo ""
echo "  To deploy a code update:"
echo "    git push  (from your local machine)"
echo "    ssh root@<server-ip> 'git -C $APP_DIR pull && systemctl restart polymarket-websocket'"
echo "════════════════════════════════════════════════════════════════════"
