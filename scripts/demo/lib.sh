#!/usr/bin/env bash
# Shared helpers for the pattern demos. NS=orderforge.
#
# The demos are meant to be reproducible EVIDENCE, not narration: each step prints the exact command
# (run/show) before its raw output, and cites the repo code that implements the pattern (source_block).
# Anyone can copy a printed `$ ...` line and run it against the same cluster.
set -euo pipefail
NS=orderforge
ALPINE=public.ecr.aws/docker/library/alpine:3.20

# ── presentation ─────────────────────────────────────────────────────────────
hr()   { printf '\n\033[1;35m─── %s ───\033[0m\n' "$*"; }
note() { printf '\033[0;36m%s\033[0m\n' "$*"; }

# run "<cmd>": print the command exactly as it runs, then run it. The banner goes to stderr so the
# command's real stdout can still be captured with $(run "...") when a step needs the value.
run()  { printf '\033[1;33m$ %s\033[0m\n' "$1" >&2; eval "$1"; }

# show "<cmd>": print a command WITHOUT running it - for a representative command whose actual execution
# is a loop or an in-cluster helper on the following lines (keeps the transcript honest about what ran).
show() { printf '\033[1;33m$ %s\033[0m\n' "$1"; }

# source_block <"path - what">...: cite the repo files + CONTRACTS.md sections that implement this demo.
source_block() { for l in "$@"; do printf '\033[1;34msrc\033[0m  %s\n' "$l"; done; }

# ── in-cluster helpers (the demos print the real command these expand to, via run/show) ───────────────
pod_by_label() { kubectl -n "$NS" get pod -l "app=$1" -o jsonpath='{.items[0].metadata.name}'; }

# In-cluster HTTP via a throwaway alpine pod (busybox wget). 2>/dev/null drops the benign
# "couldn't attach to pod, falling back to streaming logs" race when the pod exits quickly.
incluster_get() {
  kubectl -n "$NS" run "ofget-$RANDOM" --rm -i --restart=Never --quiet \
    --image="$ALPINE" -- wget -qO- "$1" 2>/dev/null
}
incluster_post() {
  kubectl -n "$NS" run "ofpost-$RANDOM" --rm -i --restart=Never --quiet \
    --image="$ALPINE" -- wget -qO- --header='Content-Type: application/json' --post-data="$2" "$1" 2>/dev/null
}

# Place an order through the Order API (in-cluster). Prints the JSON response (order_id, task_id, availability).
place_order() {
  local cust="${1:-cust_$RANDOM}" body
  body="{\"customer_id\":\"$cust\",\"currency\":\"USD\",\"items\":[{\"sku\":\"A1\",\"qty\":2},{\"sku\":\"B2\",\"qty\":1}]}"
  incluster_post "http://order-api/orders" "$body"
}

# redis-cli against an in-cluster redis (has redis-cli). Usage: redis_cli <app-label> <args...>
redis_cli() { local app="$1"; shift; kubectl -n "$NS" exec "$(pod_by_label "$app")" -c redis -- redis-cli "$@"; }
