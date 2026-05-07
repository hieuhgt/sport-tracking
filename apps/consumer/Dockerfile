# Build context: repo root (set in docker-compose as context: .)

FROM node:20-alpine AS base
RUN corepack enable && corepack prepare pnpm@9 --activate

# ── Install stage ─────────────────────────────────────────────────────────────
FROM base AS installer
WORKDIR /app
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/schemas/package.json ./packages/schemas/
COPY apps/consumer/package.json    ./apps/consumer/
RUN pnpm install --frozen-lockfile

# ── Build stage ───────────────────────────────────────────────────────────────
FROM base AS builder
WORKDIR /app
COPY --from=installer /app/node_modules          ./node_modules
COPY --from=installer /app/packages/schemas/node_modules ./packages/schemas/node_modules
COPY --from=installer /app/apps/consumer/node_modules   ./apps/consumer/node_modules
COPY packages/schemas ./packages/schemas
COPY apps/consumer    ./apps/consumer
RUN pnpm --filter @sport-tracking/schemas  build && \
    pnpm --filter @sport-tracking/consumer build

# ── Production stage ──────────────────────────────────────────────────────────
FROM base AS production
WORKDIR /app
ENV NODE_ENV=production
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/schemas/package.json ./packages/schemas/
COPY apps/consumer/package.json    ./apps/consumer/
RUN pnpm install --frozen-lockfile --prod
COPY --from=builder /app/packages/schemas/dist ./packages/schemas/dist
COPY --from=builder /app/apps/consumer/dist    ./apps/consumer/dist
EXPOSE 3002
CMD ["node", "apps/consumer/dist/main.js"]
