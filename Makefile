# OrderForge - six container patterns, one order pipeline, any Kubernetes cluster.
# `make build` + `kubectl apply -k deploy/kustomize/base` deploys to any cluster (see README).
# The push/deploy/destroy targets below are the AWS/EKS convenience path (ECR + the eks overlay).
SHELL := /usr/bin/env bash
SERVICES := $(notdir $(patsubst %/Dockerfile,%,$(wildcard services/*/Dockerfile)))

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: kustomize-check
kustomize-check: ## Render the manifests offline (no cluster needed)
	kubectl kustomize deploy/kustomize/base >/dev/null && echo "base OK"
	kubectl kustomize deploy/kustomize/overlays/eks >/dev/null && echo "eks overlay OK"

.PHONY: build
build: ## Locally docker-build every service image (verifies they compile)
	@for s in $(SERVICES); do echo "== build $$s =="; \
		docker build -t orderforge/$$s:latest services/$$s || exit 1; done
	@echo "built: $(SERVICES)"

.PHONY: push
push: ## Build + push all images to ECR (auto-detects node arch)
	scripts/build-and-push.sh

.PHONY: deploy
deploy: ## Deploy via the eks overlay to the current context (rewrites images to ECR; base is portable)
	scripts/deploy.sh

.PHONY: destroy
destroy: ## Tear down (Ingress-first ordering); add PURGE_ECR=1 to delete repos
	scripts/teardown.sh $(if $(PURGE_ECR),--purge-ecr,)

.PHONY: port-forward
PORT ?= 32000
port-forward: ## Forward the Order API to localhost:32000 (override: make port-forward PORT=NNNN)
	kubectl -n orderforge port-forward svc/order-api $(PORT):80

.PHONY: demo-sidecar demo-adapter demo-ambassador demo-scatter demo-workqueue demo-leader
demo-sidecar:   ## SIDECAR: app log line shows up in the central sink
	scripts/demo/sidecar.sh
demo-adapter:   ## ADAPTER: native /stats vs normalized /metrics side by side
	scripts/demo/adapter.sh
demo-ambassador: ## AMBASSADOR: keys provably split across cache shards
	scripts/demo/ambassador.sh
demo-scatter:   ## SCATTER/GATHER: fan-out + partial-failure (scale a leaf to 0)
	scripts/demo/scatter.sh
demo-workqueue: ## WORK QUEUE: enqueue, watch file handoff + done set grow
	scripts/demo/workqueue.sh
demo-leader:    ## LEADER ELECTION: kill the leader, watch failover, show fail-closed
	scripts/demo/leader.sh

.PHONY: demo-all
demo-all: demo-sidecar demo-adapter demo-ambassador demo-scatter demo-workqueue demo-leader ## Run every pattern demo
