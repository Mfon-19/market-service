#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'EOF'
Usage: export_dashboard_images.sh

Exports PNG images for Grafana dashboard panels using Grafana render endpoints.

Env vars:
  GRAFANA_URL        default: http://localhost:3000
  GRAFANA_USER       default: admin
  GRAFANA_PASSWORD   default: admin
  GRAFANA_TOKEN      optional bearer token (preferred over user/password)
  DASHBOARD_UID      default: market-performance
  DASHBOARD_SLUG     default: market-service-performance
  PANEL_IDS          default: "1 2 3 4 5"
  FROM               default: now-1h
  TO                 default: now
  WIDTH              default: 1600
  HEIGHT             default: 900
  OUT_DIR            default: dashboard_exports/<uid>_<timestamp>

Example:
  ./scripts/export_dashboard_images.sh
  FROM=now-2h TO=now PANEL_IDS="1 2" ./scripts/export_dashboard_images.sh
EOF
  exit 0
fi

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
GRAFANA_TOKEN="${GRAFANA_TOKEN:-}"
DASHBOARD_UID="${DASHBOARD_UID:-market-performance}"
DASHBOARD_SLUG="${DASHBOARD_SLUG:-market-service-performance}"
PANEL_IDS="${PANEL_IDS:-1 2 3 4 5}"
FROM="${FROM:-now-1h}"
TO="${TO:-now}"
WIDTH="${WIDTH:-1600}"
HEIGHT="${HEIGHT:-900}"
OUT_DIR="${OUT_DIR:-dashboard_exports/${DASHBOARD_UID}_$(date +%Y%m%d_%H%M%S)}"

mkdir -p "$OUT_DIR"

curl_auth=()
if [[ -n "$GRAFANA_TOKEN" ]]; then
  curl_auth=(-H "Authorization: Bearer ${GRAFANA_TOKEN}")
else
  curl_auth=(-u "${GRAFANA_USER}:${GRAFANA_PASSWORD}")
fi

for panel_id in $PANEL_IDS; do
  output_file="${OUT_DIR}/panel_${panel_id}.png"
  render_url="${GRAFANA_URL}/render/d-solo/${DASHBOARD_UID}/${DASHBOARD_SLUG}?orgId=1&panelId=${panel_id}&from=${FROM}&to=${TO}&width=${WIDTH}&height=${HEIGHT}&tz=UTC"

  status_code="$(curl -sS "${curl_auth[@]}" -o "$output_file" -w "%{http_code}" "$render_url")"
  if [[ "$status_code" != "200" ]]; then
    rm -f "$output_file"
    echo "Failed to render panel ${panel_id}. HTTP ${status_code}" >&2
    exit 1
  fi

  echo "Rendered panel ${panel_id} -> ${output_file}"
done

echo "Dashboard images exported to ${OUT_DIR}"
