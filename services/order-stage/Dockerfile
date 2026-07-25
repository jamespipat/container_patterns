# order-stage: work-queue user stage (shim watcher + one-shot stage).
# No dependencies beyond the Node stdlib, so a single small stage is enough.
FROM node:22-slim

ENV NODE_ENV=production
WORKDIR /app

# Manifest first for layer caching; there are no third-party deps to install.
COPY package.json ./
COPY shim.js stage.js ./

# Run as the non-root user that node:22-slim already provides.
USER node

# The shim is the long-running process; it spawns stage.js per task.
CMD ["node", "shim.js"]
