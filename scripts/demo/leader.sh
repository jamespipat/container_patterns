#!/usr/bin/env bash
# LEADER ELECTION demo: exactly one settlement replica leads; kill it and watch failover; show fail-closed.
source "$(dirname "$0")/lib.sh"

hr "4 · LEADER ELECTION"
source_block \
  "services/leader-elector/main.go       - client-go leaderelection.RunOrDie on a coordination.k8s.io Lease" \
  "services/settlement/app.py            - polls localhost:4040/leader; any doubt => NOT leader (fail-closed)" \
  "deploy/kustomize/base/settlement.yaml + settlement-rbac.yaml - 3 replicas, elector sidecar, Lease RBAC" \
  "docs/design/CONTRACTS.md section 7    - the /leader payload (identity, isLeader, leaseValidUntil)"

# Ask a settlement pod's elector (over its own localhost:4040) whether it is leader. The settlement
# container is python-slim (has python3) and shares the pod net namespace with the elector sidecar.
leader_status() {
  kubectl -n "$NS" exec "$1" -c settlement -- python3 -c \
    'import urllib.request as u; print(u.urlopen("http://127.0.0.1:4040/leader",timeout=2).read().decode())'
}

hr "the 3 settlement replicas"
run "kubectl -n $NS get pods -l app=settlement -o wide"

hr "the Lease is the source of truth - holderIdentity names the leader"
run "kubectl -n $NS get lease orderforge-settlement -o yaml | grep -E 'holderIdentity|renewTime|leaseDuration'"
LEADER=$(kubectl -n "$NS" get lease orderforge-settlement -o jsonpath='{.spec.holderIdentity}')

hr "each replica self-reports (expect EXACTLY ONE isLeader:true; the rest fail closed)"
show "kubectl -n $NS exec <settlement-pod> -c settlement -- python3 -c 'urlopen(\"http://127.0.0.1:4040/leader\")'"
for p in $(kubectl -n "$NS" get pods -l app=settlement -o name | sed 's#pod/##'); do
  printf '%s -> ' "$p"; leader_status "$p"
done

hr "elector log of the current leader"
run "kubectl -n $NS logs $LEADER -c leader-elector --tail=20 | grep -E 'BECAME|LOST|NEW LEADER'"

hr "FAILOVER: delete the leader pod, watch the Lease move"
run "kubectl -n $NS delete pod $LEADER --wait=false"
note "watching holderIdentity change (up to LeaseDuration=15s)..."
for i in $(seq 1 20); do
  NEW=$(kubectl -n "$NS" get lease orderforge-settlement -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)
  if [ -n "$NEW" ] && [ "$NEW" != "$LEADER" ]; then note "NEW LEADER: $NEW (was $LEADER)"; break; fi
  sleep 1
done

hr "post-failover self-reports (again exactly one leader; the rest isLeader:false)"
for p in $(kubectl -n "$NS" get pods -l app=settlement -o name | sed 's#pod/##'); do
  printf '%s -> ' "$p"; leader_status "$p" 2>/dev/null || echo '(starting)'
done
