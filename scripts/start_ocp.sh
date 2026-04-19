#!/usr/bin/env bash
set -euo pipefail

show_help() {
  cat <<'EOF'
Start a local OCP node with sensible defaults.

Usage:
  ./scripts/start_ocp.sh [extra server args]

Environment overrides:
  OCP_HOST            Host to bind to                (default: 127.0.0.1)
  OCP_PORT            Port to bind to                (default: 8421)
  OCP_NODE_ID         Node id                        (default: derived from hostname)
  OCP_DISPLAY_NAME    Display name                   (default: OCP Node)
  OCP_DEVICE_CLASS    Device class                   (default: full)
  OCP_FORM_FACTOR     Form factor                    (default: workstation)
  OCP_STATE_DIR       State directory                (default: ./.local/ocp)
  OCP_DB_PATH         SQLite db path                 (default: $OCP_STATE_DIR/ocp.db)
  OCP_IDENTITY_DIR    Identity directory             (default: $OCP_STATE_DIR/identity)
  OCP_WORKSPACE_ROOT  Workspace root                 (default: $OCP_STATE_DIR/workspace)

Examples:
  ./scripts/start_ocp.sh
  OCP_PORT=8521 ./scripts/start_ocp.sh
  OCP_HOST=0.0.0.0 OCP_PORT=8421 ./scripts/start_ocp.sh --display-name "Alpha"
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  show_help
  exit 0
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

slugify() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-' | sed 's/^-//; s/-$//'
}

default_host_name="$(hostname -s 2>/dev/null || hostname || printf 'ocp')"
default_node_id="$(slugify "$default_host_name")"
if [[ -z "$default_node_id" ]]; then
  default_node_id="ocp"
fi
default_node_id="${default_node_id}-node"

host="${OCP_HOST:-127.0.0.1}"
port="${OCP_PORT:-8421}"
node_id="${OCP_NODE_ID:-$default_node_id}"
display_name="${OCP_DISPLAY_NAME:-OCP Node}"
device_class="${OCP_DEVICE_CLASS:-full}"
form_factor="${OCP_FORM_FACTOR:-workstation}"
state_dir="${OCP_STATE_DIR:-$repo_root/.local/ocp}"
db_path="${OCP_DB_PATH:-$state_dir/ocp.db}"
identity_dir="${OCP_IDENTITY_DIR:-$state_dir/identity}"
workspace_root="${OCP_WORKSPACE_ROOT:-$state_dir/workspace}"

display_host="$host"
if [[ "$display_host" == "0.0.0.0" || "$display_host" == "::" ]]; then
  display_host="127.0.0.1"
fi

mkdir -p "$state_dir" "$identity_dir" "$workspace_root"

cat <<EOF
Starting The Open Compute Protocol

  repo:         $repo_root
  host:         $host
  port:         $port
  node id:      $node_id
  display name: $display_name
  device class: $device_class
  form factor:  $form_factor
  db:           $db_path
  identity:     $identity_dir
  workspace:    $workspace_root

Easy setup:
  http://$display_host:$port/

Advanced control deck:
  http://$display_host:$port/control
EOF

if [[ "$host" == "0.0.0.0" || "$host" == "::" ]]; then
  cat <<EOF

Other computers on your network:
  use this machine's LAN IP with port $port
  example: http://192.168.1.44:$port/
EOF
fi

exec python3 "$repo_root/server.py" \
  --host "$host" \
  --port "$port" \
  --db-path "$db_path" \
  --workspace-root "$workspace_root" \
  --identity-dir "$identity_dir" \
  --node-id "$node_id" \
  --display-name "$display_name" \
  --device-class "$device_class" \
  --form-factor "$form_factor" \
  "$@"
