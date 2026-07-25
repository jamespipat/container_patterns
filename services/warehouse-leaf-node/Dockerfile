# warehouse-leaf-node - OrderForge scatter/gather leaf (Node).
# Multi-stage: install (no deps today, kept for parity/future) then a slim
# non-root runtime. No external dependencies -> the app is pure Node stdlib.
FROM node:22-slim AS build
WORKDIR /app
COPY package.json ./
# --omit=dev keeps the image lean; there are no runtime deps today, so mkdir
# ensures node_modules exists for the COPY below and stays correct if a
# dependency is ever added.
RUN mkdir -p node_modules && npm install --omit=dev --no-audit --no-fund

FROM node:22-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY package.json index.js ./
# node:22-slim ships an unprivileged `node` user (uid 1000); run as it.
USER node
EXPOSE 8080
ENTRYPOINT ["node", "index.js"]
