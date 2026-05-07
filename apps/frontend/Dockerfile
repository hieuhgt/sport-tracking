# Build context: repo root (set in docker-compose as context: .)

FROM node:20-alpine AS base
RUN corepack enable && corepack prepare pnpm@9 --activate

# ── Install stage ─────────────────────────────────────────────────────────────
FROM base AS installer
WORKDIR /app
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/schemas/package.json  ./packages/schemas/
COPY apps/frontend/package.json     ./apps/frontend/
RUN pnpm install --frozen-lockfile

# ── Build stage ───────────────────────────────────────────────────────────────
FROM base AS builder
WORKDIR /app
COPY --from=installer /app/node_modules          ./node_modules
COPY --from=installer /app/packages/schemas/node_modules ./packages/schemas/node_modules
COPY --from=installer /app/apps/frontend/node_modules   ./apps/frontend/node_modules
COPY packages/schemas ./packages/schemas
COPY apps/frontend    ./apps/frontend
RUN pnpm --filter @sport-tracking/schemas  build && \
    pnpm --filter @sport-tracking/frontend build

# ── Serve stage (nginx) ───────────────────────────────────────────────────────
FROM nginx:alpine AS production
COPY --from=builder /app/apps/frontend/dist /usr/share/nginx/html
COPY apps/frontend/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
