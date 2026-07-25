#!/usr/bin/env bash
# Shared config for OrderForge scripts. Source this: `source "$(dirname "$0")/lib.sh"`
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGION="${AWS_REGION:-${REGION:-us-east-1}}"

# Authoritative image list = every service dir that has a Dockerfile (so the count can never drift).
mapfile -t SERVICES < <(find "$REPO_ROOT/services" -maxdepth 2 -name Dockerfile -printf '%h\n' | xargs -n1 basename | sort)

account_id() { aws sts get-caller-identity --query Account --output text; }
registry()   { echo "$(account_id).dkr.ecr.${REGION}.amazonaws.com"; }

# Node architecture the cluster runs (amd64 unless a Graviton pool). Falls back to amd64 if unreachable.
detect_node_arch() {
  local a
  a="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}' 2>/dev/null || true)"
  echo "${a:-amd64}"
}

# Image tag: short git sha if in a repo, else a timestamp passed in via TAG.
image_tag() { git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "${TAG:-dev}"; }

log() { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
