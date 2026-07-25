#!/usr/bin/env bash
# Deploy OrderForge via the eks overlay to the current kube context, pointing every image at your ECR
# registry. This is the AWS/EKS convenience path. For a non-AWS cluster, apply deploy/kustomize/base
# directly with your own registry (see README "Deploy to any cluster").
# Renders a throwaway kustomization (keeps the repo clean, needs only kubectl's built-in kustomize).
# Usage: scripts/deploy.sh
source "$(dirname "$0")/lib.sh"

kubectl config current-context >/dev/null 2>&1 || die "no kube context. Point kubectl at your cluster first (e.g. aws eks update-kubeconfig, gcloud container clusters get-credentials, or set KUBECONFIG)."
REGISTRY="$(registry)"
TAG="$(image_tag)"
log "deploying to context '$(kubectl config current-context)'  registry=$REGISTRY  tag=$TAG"

# Create the throwaway kustomization INSIDE deploy/kustomize/ so it references the eks overlay as a
# sibling (../overlays/eks). kustomize rejects absolute resource roots, and a /tmp dir has no clean
# relative path back to the repo. The dir is hidden and removed on exit. We layer on the eks overlay
# (not raw base) so its EKS-specific config (arm64 nodeSelector, later ALB/KEDA) is included.
TMP="$(mktemp -d "${REPO_ROOT}/deploy/kustomize/.deploy-XXXXXX")"; trap 'rm -rf "$TMP"' EXIT
{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo "namespace: orderforge"
  echo "resources: [ ../overlays/eks ]"
  echo "images:"
  for svc in "${SERVICES[@]}"; do
    echo "  - name: orderforge/${svc}"
    echo "    newName: ${REGISTRY}/${svc}"
    echo "    newTag: ${TAG}"
  done
} > "$TMP/kustomization.yaml"

kubectl apply -k "$TMP"
log "applied. watch rollout:  kubectl -n orderforge get pods -w"
log "reach the API:           kubectl -n orderforge port-forward svc/order-api 32000:80"
