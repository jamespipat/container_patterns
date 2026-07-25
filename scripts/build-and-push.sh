#!/usr/bin/env bash
# Build every OrderForge service image and push to ECR. This is the AWS/EKS convenience path.
# For a non-AWS cluster, use `make build` and push the resulting orderforge/<svc>:latest images to your
# own registry (or load them into a local cluster); the manifests in deploy/kustomize/base are portable.
#   - creates each ECR repo if absent
#   - builds for the CLUSTER's node architecture (amd64 default; arm64 on Graviton)
#   - rejects any NODE_ARCH other than amd64/arm64
# Usage: scripts/build-and-push.sh            (auto-detect arch from the cluster)
#        NODE_ARCH=arm64 scripts/build-and-push.sh   (override)
source "$(dirname "$0")/lib.sh"

NODE_ARCH="${NODE_ARCH:-$(detect_node_arch)}"
case "$NODE_ARCH" in
  amd64|arm64) : ;;
  *) die "unsupported NODE_ARCH='$NODE_ARCH' (expected amd64 or arm64)";;
esac
PLATFORM="linux/${NODE_ARCH}"
REGISTRY="$(registry)"
TAG="$(image_tag)"

log "registry=$REGISTRY  platform=$PLATFORM  tag=$TAG  services=${#SERVICES[@]}"
[ "${#SERVICES[@]}" -gt 0 ] || die "no services/*/Dockerfile found"

log "ECR login"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

for svc in "${SERVICES[@]}"; do
  log "build+push $svc"
  aws ecr describe-repositories --repository-names "$svc" --region "$REGION" >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name "$svc" --region "$REGION" \
         --image-scanning-configuration scanOnPush=true >/dev/null
  docker buildx build --platform "$PLATFORM" \
    -t "${REGISTRY}/${svc}:${TAG}" -t "${REGISTRY}/${svc}:latest" \
    "${REPO_ROOT}/services/${svc}" --push
done

log "done. pushed ${#SERVICES[@]} images to $REGISTRY (tag $TAG + latest)"
