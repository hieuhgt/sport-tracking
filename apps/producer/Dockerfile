# Build context: repo root (set in docker-compose as context: .)
# This lets us access packages/schemas which producer depends on.

FROM node:20-alpine AS base
RUN corepack enable && corepack prepare pnpm@9 --activate

# ── Install stage (cached until any package.json / lockfile changes) ──────────
FROM base AS installer
WORKDIR /app
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/schemas/package.json ./packages/schemas/
COPY apps/producer/package.json     ./apps/producer/
RUN pnpm install --frozen-lockfile

# ── Build stage ───────────────────────────────────────────────────────────────
FROM base AS builder
WORKDIR /app
COPY --from=installer /app/node_modules          ./node_modules
COPY --from=installer /app/packages/schemas/node_modules ./packages/schemas/node_modules
COPY --from=installer /app/apps/producer/node_modules   ./apps/producer/node_modules
COPY packages/schemas ./packages/schemas
COPY apps/producer    ./apps/producer
# schemas must be compiled first — producer imports its types + Avro JSON exports
RUN pnpm --filter @sport-tracking/schemas  build && \
    pnpm --filter @sport-tracking/producer build

# ── Production stage ──────────────────────────────────────────────────────────
FROM base AS production
WORKDIR /app
ENV NODE_ENV=production
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/schemas/package.json ./packages/schemas/
COPY apps/producer/package.json    ./apps/producer/
# Install runtime deps only (no devDependencies)
RUN pnpm install --frozen-lockfile --prod
# Bring compiled output from builder
COPY --from=builder /app/packages/schemas/dist ./packages/schemas/dist
COPY --from=builder /app/apps/producer/dist    ./apps/producer/dist
EXPOSE 3001
CMD ["node", "apps/producer/dist/main.js"]
