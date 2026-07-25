#!/usr/bin/env bash
# Tear OrderForge down. On any cluster the core is just `kubectl delete namespace orderforge` (done below).
# This script adds AWS/EKS-aware ordering on top: delete any Ingress FIRST and wait for the ALB to disappear
# (deleting the LB controller before the Ingress leaks the ALB), then delete workloads.
# ECR repo deletion is opt-in (--purge-ecr) since images are cheap and reusable.
# Usage: scripts/teardown.sh [--purge-ecr]
source "$(dirname "$0")/lib.sh"

PURGE_ECR=0; [ "${1:-}" = "--purge-ecr" ] && PURGE_ECR=1

if kubectl -n orderforge get ingress order-api >/dev/null 2>&1; then
  log "deleting Ingress first (waiting for the ALB to be released)"
  kubectl -n orderforge delete ingress order-api --wait=true || warn "ingress delete failed; check for a leaked ALB"
  sleep 20
fi

log "deleting workloads (namespace-scoped)"
kubectl delete namespace orderforge --wait=true 2>/dev/null || warn "namespace already gone"

if [ "$PURGE_ECR" = 1 ]; then
  REGION="$REGION"
  for svc in "${SERVICES[@]}"; do
    log "deleting ECR repo $svc"
    aws ecr delete-repository --repository-name "$svc" --region "$REGION" --force >/dev/null 2>&1 || true
  done
fi

log "teardown complete. NOTE: the EKS control plane, node groups, and any manually-installed ALB/NLB, KEDA,"
log "Loki, or Grafana are NOT removed here - remove those via their own lifecycle if you provisioned them."
