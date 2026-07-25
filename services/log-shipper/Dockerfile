# OrderForge log-shipper (Node). No third-party deps: uses the Node 22 stdlib
# plus the built-in global fetch, so there is nothing to compile or install.
FROM node:22-slim

ENV NODE_ENV=production \
    LOG_DIR=/var/log/app \
    SINK_URL=http://log-sink:3100/loki/api/v1/push

WORKDIR /app
COPY package.json index.js ./

# node:22-slim ships a non-root "node" user (uid 1000). No listening port.
USER node

ENTRYPOINT ["node", "index.js"]
