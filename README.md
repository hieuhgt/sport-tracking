# sport-tracking

Real-time sports statistics dashboard. Every goal, card, basket, and foul is published to Kafka and pushed to every connected browser within milliseconds — even when thousands of matches run simultaneously.

**Mandatory stack:** TypeScript · Apache Kafka · Docker  
**Additional stack:** NestJS · Avro + Confluent Schema Registry · Socket.io · Redis · React + Vite · Nginx · Turborepo · pnpm workspaces

---

## Table of contents

1. [Architecture overview](#1-architecture-overview)
2. [Data flow — tracing a single event](#2-data-flow--tracing-a-single-event)
3. [Infrastructure components](#3-infrastructure-components)
4. [Monorepo layout](#4-monorepo-layout)
5. [File-by-file reference](#5-file-by-file-reference)
6. [How to run the project](#6-how-to-run-the-project)
7. [API reference](#7-api-reference)
8. [Kafka concepts explained](#8-kafka-concepts-explained)
9. [Avro + Schema Registry explained](#9-avro--schema-registry-explained)
10. [How consumer scaling works](#10-how-consumer-scaling-works)
11. [How Turborepo works in this project](#11-how-turborepo-works-in-this-project)
12. [Docker build strategy](#12-docker-build-strategy)
13. [Port map](#13-port-map)

---

## 1. Architecture overview

```
                         ┌─────────────────────────────────────────────────────┐
                         │                  Docker network: sports-net          │
                         │                                                       │
  Browser                │  ┌──────────┐    ┌──────────────────────────────┐   │
  ───────  HTTP/WS ───►  │  │  Nginx   │    │     Schema Registry :8081    │   │
           port 80       │  │  :80     │    │  (stores + validates Avro     │   │
                         │  │          │    │   schemas, assigns IDs)       │   │
                         │  │ /        │    └──────────────┬───────────────┘   │
                         │  │ frontend │                   │ register           │
                         │  │          │                   │ on startup         │
                         │  │ /api/    ├──► Producer :3001 ┤                   │
                         │  │          │   (NestJS API)    │ encode → Avro     │
                         │  │ /socket  │                   ▼                   │
                         │  │   .io/   │    ┌──────────────────────────────┐   │
                         │  │          │    │      Kafka Cluster            │   │
                         │  │          │    │                               │   │
                         │  └────┬─────┘    │  broker-1  broker-2  broker-3│   │
                         │       │          │                               │   │
                         │       │ WS       │  worldcup.events (6 parts)   │   │
                         │       │          │  nba.events      (6 parts)   │   │
                         │       ▼          │                               │   │
                         │  ┌──────────┐    │  replication factor = 3      │   │
                         │  │ Consumer │◄───│  min ISR = 2                 │   │
                         │  │ :3002    │    └──────────────────────────────┘   │
                         │  │          │                                        │
                         │  │ NestJS   │    ┌──────────────────────────────┐   │
                         │  │ + WS     │    │           Redis :6379         │   │
                         │  │ Gateway  │◄──►│  • match state cache          │   │
                         │  │          │    │  • Socket.io pub/sub adapter  │   │
                         │  │ Socket.io│    └──────────────────────────────┘   │
                         │  └──────────┘                                        │
                         │   (scalable: docker compose up --scale consumer=N)   │
                         │                                                       │
                         │  ┌──────────┐   ┌──────────┐                        │
                         │  │Zookeeper │   │ Kafka UI │ ← monitoring :8090     │
                         │  └──────────┘   └──────────┘                        │
                         └─────────────────────────────────────────────────────┘
```

### Why each component exists

| Component | Role | Why it's needed |
|---|---|---|
| **Nginx** | Reverse proxy at port 80 | Single entry point; routes `/`, `/api/`, `/socket.io/` to the right service; handles WebSocket upgrade headers |
| **Producer** | NestJS REST API → Kafka | Decouples event sources (any HTTP client) from the queue; validates and Avro-encodes events |
| **Kafka** | Durable message queue | Ordered, persistent, replayable event stream; survives producer/consumer crashes |
| **Schema Registry** | Avro schema store | Enforces schema compatibility across deployments; reduces message size by removing field names from the wire |
| **Consumer** | Kafka → browser | NestJS app that reads events, maintains authoritative match state, broadcasts to all WebSocket clients |
| **Redis** | State cache + Socket.io bridge | Stores current match state so new browser tabs get data immediately; synchronises Socket.io broadcasts across scaled consumer replicas |
| **Frontend** | React SPA | Displays live scores and event feed; connects via Socket.io |
| **Zookeeper** | Kafka coordination | Manages Kafka broker leader election and partition assignments |
| **Kafka UI** | Monitoring | Visual inspection of topics, consumer lag, and Schema Registry |

---

## 2. Data flow — tracing a single event

Here is what happens, step by step, from the moment a goal is scored to the browser updating.

```
Step 1  POST /events/worldcup
        { matchId: "wc-match-1", type: "GOAL", minute: 67,
          homeTeam: { score: 2, ... }, player: { name: "Vinicius Jr" } }
        → nginx receives on port 80, proxies to producer:3001

Step 2  NestJS EventsController validates the request body (checks matchId, type, minute exist)
        → throws 400 BadRequestException if fields are missing

Step 3  EventsService calls kafkaService.publishEvent(event)
        → KafkaService calls registry.encode(schemaId=1, eventObject)
        → Schema Registry: looks up Avro schema for subject "worldcup.events-value"
        → Serialises the object to Avro binary (field names stripped, values tightly packed)
        → Prepends [0x00][0x00 0x00 0x00 0x01]  (magic byte + schema ID)
        → Returns a Buffer of ~30 bytes  (was ~200 bytes as JSON)

Step 4  KafkaService calls kafka.producer.send()
        → Topic: "worldcup.events"
        → Key:   "wc-match-1"  (Kafka hashes this → always goes to same partition)
        → Value: the 30-byte Avro Buffer
        → Kafka writes to partition leader, replicates to 2 other brokers
        → Acknowledges write only when 2 replicas (min ISR) confirm receipt

Step 5  KafkaConsumerService (in the same consumer group) has partition assigned to it
        → KafkaJS calls eachMessage() with the raw Buffer

Step 6  KafkaConsumerService calls registry.decode(message.value)
        → Reads magic byte (0x00) — confirms this is a Schema Registry message
        → Reads schema ID (1) from bytes 1-4
        → Fetches Avro schema from registry (cached after first call)
        → Deserialises Avro binary → plain JS object { matchId, type, minute, ... }

Step 7  KafkaConsumerService calls stateService.applyWorldCupEvent(event)
        → Reads current WorldCupMatchState from Redis key "wc:match:wc-match-1"
        → Updates: homeTeam.score = 2, recentEvents = [goalEvent, ...previous]
        → Writes updated state back to Redis (TTL 24h)
        → Returns the updated WorldCupMatchState object

Step 8  KafkaConsumerService calls eventsGateway.broadcastUpdate("worldcup", state)
        → Socket.io Redis adapter publishes to Redis channel
        → ALL consumer replicas receive the pub/sub message
        → EACH replica calls its own server.emit() → sends to its connected browsers

Step 9  Browser receives "worldcup:state" event via WebSocket
        → useSocket() hook calls setWorldcup(state)
        → React re-renders WorldCupDashboard with new score and event feed
        → User sees "BRA 2 – 1 GER, 67' ⚽ Vinicius Jr"
```

**Total latency:** typically 50–200 ms end-to-end on a local machine.

---

## 3. Infrastructure components

### Kafka cluster (3 brokers)

Three brokers run as separate containers (`kafka-1`, `kafka-2`, `kafka-3`). Each broker handles a subset of partition leadership. The cluster settings that matter:

| Setting | Value | Meaning |
|---|---|---|
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | 3 | Every partition is copied to all 3 brokers |
| `KAFKA_MIN_INSYNC_REPLICAS` | 2 | A write is acknowledged only when 2 brokers have it |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | false | Topics are only created by the admin client (controlled) |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | 3 | Consumer group offsets are also replicated |

**Failure tolerance:** with RF=3 and min-ISR=2, losing one broker keeps the cluster fully operational. Losing two brokers pauses writes (below min-ISR) but no data is lost.

### Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `worldcup.events` | 6 | All World Cup match events |
| `nba.events` | 6 | All NBA game events |

Six partitions means up to 6 consumer instances can work in parallel for each topic.

### Schema Registry

Stores Avro schemas indexed by a subject name and version. Subjects follow the **TopicNameStrategy**: `{topic}-value`.

| Subject | Schema | Current version |
|---|---|---|
| `worldcup.events-value` | `WorldCupEvent` | 1 |
| `nba.events-value` | `NBAEvent` | 1 |

Compatibility mode: **BACKWARD** — old consumers can read data from new producers. Concretely: you may add optional fields (with defaults), but you cannot remove fields, rename fields, or add enum symbols without changing the compatibility mode first.

### Redis

Used for two independent purposes:

1. **Match state cache** — keys like `wc:match:wc-match-1` store the full `WorldCupMatchState` as JSON with a 24-hour TTL. When a new browser tab connects, the consumer reads from Redis and sends the current state immediately.

2. **Socket.io adapter** — the `@socket.io/redis-adapter` uses two Redis connections (one publisher, one subscriber) per consumer instance to synchronise `server.emit()` calls across all replicas.

---

## 4. Monorepo layout

```
sport-tracking/
│
│  ── Root config ──────────────────────────────────────────────────────────────
├── turbo.json              Turborepo task pipeline (build order + caching rules)
├── package.json            Workspace root; declares workspaces, turbo devDep
├── pnpm-workspace.yaml     Tells pnpm which directories are workspace packages
├── tsconfig.base.json      Shared TypeScript compiler options (extended by each app)
├── .dockerignore           Files excluded from Docker build context
├── docker-compose.yml      Declares all services, networks, volumes
├── Makefile                Convenience shortcuts (make up, make simulate, ...)
│
│  ── Reverse proxy ────────────────────────────────────────────────────────────
├── nginx/
│   └── nginx.conf          Routes /, /api/, /socket.io/ to correct service
│
│  ── Shared package ───────────────────────────────────────────────────────────
└── packages/
    └── schemas/            @sport-tracking/schemas
        ├── package.json    Declares main/types/exports pointing to dist/
        ├── tsconfig.json   Extends tsconfig.base.json; compiles to commonjs
        └── src/
            ├── index.ts                Re-exports all types + Avro schema objects
            ├── types/
            │   └── events.ts           Team, Player, WorldCupEvent, NBAEvent,
            │                           SportEvent, WorldCupMatchState, NBAGameState
            └── schemas/
                ├── worldcup-event.json Avro schema for WorldCupEvent
                └── nba-event.json      Avro schema for NBAEvent

│  ── Applications ─────────────────────────────────────────────────────────────
└── apps/
    │
    ├── producer/           @sport-tracking/producer  (NestJS)
    │   ├── Dockerfile      3-stage build (installer → builder → production)
    │   ├── package.json    NestJS deps + @sport-tracking/schemas (workspace:*)
    │   ├── tsconfig.json   Extends tsconfig.base.json; emitDecoratorMetadata: true
    │   └── src/
    │       ├── main.ts                     NestJS bootstrap; enables CORS
    │       ├── app.module.ts               Root module; imports Kafka, Events, Simulator
    │       ├── kafka/
    │       │   ├── kafka.module.ts         @Global module; exports KafkaService
    │       │   └── kafka.service.ts        Topics + Schema Registry + producer lifecycle
    │       ├── events/
    │       │   ├── events.module.ts        Wires controller + service
    │       │   ├── events.controller.ts    POST /events/worldcup, POST /events/nba
    │       │   └── events.service.ts       Builds event objects, calls KafkaService
    │       └── simulator/
    │           ├── simulator.module.ts     Wires controller + service
    │           ├── simulator.controller.ts POST /simulate/start|stop, GET /simulate/status
    │           └── simulator.service.ts    Random event generator (WC + NBA game loops)
    │
    ├── consumer/           @sport-tracking/consumer  (NestJS)
    │   ├── Dockerfile      3-stage build
    │   ├── package.json    NestJS + websockets deps + @sport-tracking/schemas
    │   ├── tsconfig.json   Extends tsconfig.base.json; emitDecoratorMetadata: true
    │   └── src/
    │       ├── main.ts                     NestJS bootstrap; wires RedisIoAdapter
    │       ├── app.module.ts               Root module; imports State, Events, Kafka
    │       ├── health.controller.ts        GET /health → instance info
    │       ├── adapters/
    │       │   └── redis-io.adapter.ts     Extends IoAdapter; plugs ioredis pub/sub pair
    │       ├── kafka/
    │       │   ├── kafka.module.ts         Imports StateModule + EventsModule
    │       │   └── kafka-consumer.service.ts  Avro-decode → StateService → EventsGateway
    │       ├── state/
    │       │   ├── state.module.ts         Provides + exports StateService
    │       │   └── state.service.ts        Redis read/apply/write match state
    │       └── events/
    │           ├── events.module.ts        Imports StateModule; provides + exports EventsGateway
    │           └── events.gateway.ts       @WebSocketGateway; hydrates new clients; broadcastUpdate()
    │
    └── frontend/           @sport-tracking/frontend
        ├── Dockerfile      3-stage build (installer → builder → nginx serve)
        ├── nginx.conf      Static file serving with SPA fallback
        ├── package.json    Depends on @sport-tracking/schemas (workspace:*)
        ├── tsconfig.json   Vite/browser TypeScript config (moduleResolution: bundler)
        ├── vite.config.ts  Vite build config
        ├── index.html      SPA entry HTML
        └── src/
            ├── App.tsx                     Root component; consumes useSocket
            ├── main.tsx                    React DOM entry point
            ├── index.css                   Dark sports dashboard CSS
            ├── hooks/
            │   └── useSocket.ts            Socket.io client hook; manages connection
            └── components/
                ├── WorldCupDashboard.tsx   World Cup panel (score + events + cards)
                ├── NBADashboard.tsx        NBA panel (score + events + fouls)
                ├── MatchCard.tsx           Score display (home – away)
                └── EventFeed.tsx           Scrolling event log with icons
```

---

## 5. File-by-file reference

### Root files

#### `turbo.json`
Defines the task pipeline for Turborepo.
```json
{
  "tasks": {
    "build": { "dependsOn": ["^build"], "outputs": ["dist/**"] },
    "dev":   { "cache": false, "persistent": true }
  }
}
```
`"dependsOn": ["^build"]` means: before this package builds, build all of its dependencies first. Because producer, consumer, and frontend all depend on `@sport-tracking/schemas`, Turborepo always builds schemas before any app. The `outputs` field tells Turborepo what to cache — if inputs haven't changed, the cached `dist/` is replayed instead of recompiling.

#### `pnpm-workspace.yaml`
```yaml
packages:
  - 'apps/*'
  - 'packages/*'
```
Tells pnpm that every directory under `apps/` and `packages/` is a workspace package. pnpm creates symlinks: `node_modules/@sport-tracking/schemas` → `packages/schemas`. This lets any app do `import { ... } from '@sport-tracking/schemas'` as if it were an npm package.

#### `tsconfig.base.json`
Shared compiler options: `strict`, `esModuleInterop`, `resolveJsonModule`, `sourceMap`. Each app's `tsconfig.json` extends this with `"extends": "../../tsconfig.base.json"` and only adds what's different. NestJS apps additionally need `emitDecoratorMetadata: true` and `experimentalDecorators: true` for dependency injection to work.

#### `nginx/nginx.conf`
Three routing rules:
- `location /` → proxies to `frontend:80` (the built React SPA)
- `location /api/` → proxies to `producer:3001` (strips the `/api` prefix)
- `location /socket.io/` → proxies to `consumer:3002` with WebSocket upgrade headers (`Upgrade: websocket`, `Connection: upgrade`). Uses `ip_hash` so the same client always hits the same consumer replica (required for Socket.io long-polling fallback).

---

### `packages/schemas/`

#### `src/types/events.ts`
The single source of truth for all TypeScript interfaces used across the system. Contains:
- `Team` — `{ id, name, shortName, score }`
- `Player` — `{ id, name, number, teamId }`
- `WorldCupEvent` — event shape for football matches (`GOAL`, `YELLOW_CARD`, `SUBSTITUTION`, etc.)
- `NBAEvent` — event shape for basketball games (`BASKET_2PT`, `BASKET_3PT`, `FOUL`, etc.)
- `SportEvent` — union type `WorldCupEvent | NBAEvent`
- `WorldCupMatchState` — full state of a match (score, status, recentEvents, yellowCards, redCards)
- `NBAGameState` — full state of a game (score, status, quarter, clock, recentEvents, fouls)

Without this shared package, these types would exist in three copies (producer, consumer, frontend). A field rename would require three edits and would only be caught if all three services were type-checked together.

#### `src/schemas/worldcup-event.json` and `nba-event.json`
Avro schema definitions (JSON format). Avro schemas describe the exact binary layout of a message — field names, types, and defaults. Key Avro concepts used here:
- `"type": "record"` — a named object with fields
- `"type": "enum"` — a fixed set of string values (like `GOAL`, `YELLOW_CARD`)
- `["null", "Player"]` — a union type; field can be null or a Player record. Default must be `null`.
- Named type references — `"type": "Team"` refers to the `Team` record defined earlier in the same schema

#### `src/index.ts`
Re-exports everything from `types/events.ts` **and** imports the two Avro schema JSON files, re-exporting them as `worldcupEventSchema` and `nbaEventSchema`. This lets the producer's `KafkaService` do `import { worldcupEventSchema } from '@sport-tracking/schemas'` instead of loading JSON files at runtime.

---

### `apps/producer/src/`

The producer is a NestJS application structured around three feature modules: `KafkaModule`, `EventsModule`, and `SimulatorModule`.

#### `main.ts`
NestJS bootstrap. Creates the app, enables CORS, and calls `app.listen()`. All lifecycle hooks (`onApplicationBootstrap`) run before the server starts accepting requests, so Kafka topics and schemas are always ready before the first HTTP request arrives.

#### `app.module.ts`
Root module. Imports:
- `ConfigModule.forRoot({ isGlobal: true })` — makes `ConfigService` available everywhere without re-importing
- `KafkaModule` — marked `@Global()`, so its exported `KafkaService` is injectable in any module
- `EventsModule` — REST controllers for publishing events
- `SimulatorModule` — HTTP-controlled random event generator

#### `kafka/kafka.service.ts`
The central Kafka service. Implements `OnApplicationBootstrap` and `OnApplicationShutdown`. Startup sequence (all in `onApplicationBootstrap`):
1. Creates the KafkaJS `Kafka` client using `KAFKA_BROKERS` from env
2. Calls `ensureTopics()` — uses an `Admin` client to create `worldcup.events` and `nba.events` (6 partitions, RF 3) if they don't exist; disconnects the admin client immediately after
3. Creates a `SchemaRegistry` client and calls `initRegistry()` — registers both Avro schemas (idempotent — returns existing ID if schema unchanged); caches schema IDs in memory
4. Creates and connects the KafkaJS `Producer` with `idempotent: true` and `maxInFlightRequests: 5`

The `publishEvent(event)` method Avro-encodes the event using the cached schema ID and sends it with `key = event.matchId` (ensures all events for the same match go to the same partition, preserving order).

On `onApplicationShutdown`, disconnects the producer gracefully.

#### `events/events.controller.ts`
NestJS controller with three endpoints:
- `GET /health` — returns `{ ok: true, timestamp }`
- `POST /events/worldcup` — validates `matchId`, `type`, `minute`; delegates to `EventsService`; returns `201 { ok: true, eventId }`
- `POST /events/nba` — validates `matchId`, `type`, `quarter`, `clock`; delegates to `EventsService`

Throws `BadRequestException` (HTTP 400) if required fields are missing — NestJS automatically serialises this to `{ statusCode: 400, message: "...", error: "Bad Request" }`.

#### `events/events.service.ts`
Generates a UUID event ID, stamps the current timestamp, and calls `kafkaService.publishEvent()`. Injects `KafkaService` (available globally thanks to `@Global()` on `KafkaModule`).

#### `simulator/simulator.service.ts`
Maintains game state as instance properties (not module globals — NestJS services are singletons). Two `setInterval` loops, one for World Cup (fires every 2–4 seconds) and one for NBA (fires every 1.5–3 seconds). Each tick advances the game clock and randomly decides what event happened (goal, card, substitution, basket, foul, etc.). Stops automatically when the match reaches full time / game end.

#### `simulator/simulator.controller.ts`
Three endpoints under the `/simulate` prefix:
- `POST /simulate/start` — starts both game loops
- `POST /simulate/stop` — clears both intervals
- `GET /simulate/status` — returns `{ running: true/false }`

---

### `apps/consumer/src/`

The consumer is a NestJS application with a `@WebSocketGateway` (Socket.io) and a Redis-backed IoAdapter, structured around `KafkaModule`, `StateModule`, and `EventsModule`.

#### `main.ts`
NestJS bootstrap with an extra step: creates a `RedisIoAdapter`, calls `connectToRedis()` to establish the pub/sub connections, then passes it to `app.useWebSocketAdapter()`. This must happen before `app.listen()` so the Socket.io server uses the Redis adapter from the start.

#### `adapters/redis-io.adapter.ts`
Extends `IoAdapter` from `@nestjs/platform-socket.io`. Overrides `createIOServer()` to call `server.adapter(this.adapterConstructor)` before returning the server. The adapter constructor is created from two ioredis clients — one for publishing, one for subscribing (pub/sub clients must be separate connections).

This is the mechanism that makes `server.emit()` in any consumer replica reach clients connected to all replicas.

#### `kafka/kafka-consumer.service.ts`
Implements `OnApplicationBootstrap` and `OnApplicationShutdown`. On bootstrap:
1. Creates a KafkaJS `Kafka` client and `SchemaRegistry` client
2. Connects a `Consumer` to the `sports-consumer-group` with `partitionsConsumedConcurrently: 3`
3. Subscribes to `worldcup.events` and `nba.events`
4. Calls `consumer.run()` with `eachMessage` handler

For each message: Avro-decodes the buffer → calls `stateService.applyEvent(event)` → calls `eventsGateway.broadcastUpdate(sport, state)`.

Injects both `StateService` and `EventsGateway` directly — no event emitter indirection needed because NestJS resolves the WebSocket gateway before `onApplicationBootstrap` is called.

#### `state/state.service.ts`
Maintains match/game state in Redis. For each event:
1. **Read** — `GET wc:match:{matchId}` from Redis, parse JSON (or use sensible defaults for a new match)
2. **Apply** — switch on `event.type` to update status, scores, cards, fouls, and prepend to `recentEvents` (capped at 20)
3. **Write** — `SET wc:match:{matchId} {json} EX 86400` (24-hour TTL)
4. **Return** — the updated state object

Redis keys:
- `wc:match:{matchId}` — stores `WorldCupMatchState` JSON
- `nba:game:{gameId}` — stores `NBAGameState` JSON

Also provides `getWorldCupState()` and `getNBAState()` — called by `EventsGateway` to hydrate newly connected clients.

#### `events/events.gateway.ts`
The `@WebSocketGateway` decorated class. Implements `OnGatewayConnection` and `OnGatewayDisconnect`.

```typescript
@WebSocketGateway({ cors: { origin: '*' }, transports: ['websocket', 'polling'] })
export class EventsGateway implements OnGatewayConnection {
  @WebSocketServer()
  server: Server;

  async handleConnection(client: Socket) {
    // hydrate new client with current state from Redis
    const [wcState, nbaState] = await Promise.all([...]);
    if (wcState)  client.emit('worldcup:state', wcState);
    if (nbaState) client.emit('nba:state', nbaState);
  }

  broadcastUpdate(sport: 'worldcup' | 'nba', state: unknown) {
    this.server.emit(sport === 'worldcup' ? 'worldcup:state' : 'nba:state', state);
  }
}
```

`handleConnection` uses `client.emit()` (only this socket) to send current state to a newly connected browser. `broadcastUpdate()` uses `this.server.emit()` (all connected sockets, across all replicas via the Redis adapter).

---

### `apps/frontend/src/`

#### `hooks/useSocket.ts`
A React hook that manages the Socket.io connection lifecycle.
```typescript
const socket = io(); // connects to current origin — nginx proxies /socket.io/ to consumer
socket.on('worldcup:state', (state) => setWorldcup(state));
socket.on('nba:state',      (state) => setNba(state));
```
The state events always carry the **full current state** (not a delta patch). This simplifies the frontend: there is no merge logic, no partial updates — just replace and re-render.

#### `App.tsx`
Root component. Reads `{ worldcup, nba, status }` from `useSocket()` and renders the two dashboard panels. Shows a placeholder with instructions if state is null (before the simulator starts).

#### `components/WorldCupDashboard.tsx` and `NBADashboard.tsx`
Receive the match/game state as a prop, render `MatchCard` (the scoreboard) and the event feed. Also render card/foul stat rows (yellow cards, red cards, foul counts).

#### `components/MatchCard.tsx`
Pure display component. Shows home team name, score, separator, away team score, and a time label (minute or quarter/clock). Color-coded by status (green = LIVE, amber = HALF TIME, gray = FINISHED).

#### `components/EventFeed.tsx`
Renders the last 20 events as a scrollable list. Each event has an icon and a label. Events are mapped to emoji: ⚽ GOAL, 🟨 YELLOW_CARD, 🔥 BASKET_3PT, ✋ FOUL, etc.

---

## 6. How to run the project

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | Latest | https://docs.docker.com/get-docker/ |
| Docker Compose | v2 (bundled with Docker Desktop) | included |
| `make` | Any | pre-installed on macOS/Linux |
| pnpm | 9.x | `npm install -g pnpm` (only needed for local dev) |
| Node.js | 20.x | https://nodejs.org (only needed for local dev) |

---

### Option A — Docker (recommended)

This is the simplest way. Docker runs everything: Kafka, Redis, Schema Registry, producer, consumer, and frontend.

#### Step 1 — Clone and enter the directory

```bash
cd sport-tracking
```

#### Step 2 — Install dependencies (updates the lockfile for new NestJS deps)

```bash
pnpm install
```

#### Step 3 — Start all services

```bash
make up
# equivalent to: docker compose up -d --build
```

Docker builds all three images (producer, consumer, frontend) and starts every service. The first build takes 3–5 minutes because it downloads base images and installs dependencies.

#### Step 4 — Wait for Kafka to be ready (~30 seconds)

Kafka takes time to elect partition leaders. Watch the logs:

```bash
make logs
# or: docker compose logs -f
```

Wait until you see output similar to:
```
producer  | [Nest] LOG [KafkaService] Topics already exist
producer  | [Nest] LOG [KafkaService] worldcup.events-value → schema id=1
producer  | [Nest] LOG [KafkaService] nba.events-value      → schema id=2
producer  | [Nest] LOG [KafkaService] Kafka producer connected
producer  | [Nest] LOG [NestApplication] Nest application successfully started
consumer  | [Nest] LOG [KafkaConsumerService] Connected to Kafka
consumer  | [Nest] LOG [NestApplication] Nest application successfully started
```

#### Step 5 — Open the dashboard

```
http://localhost
```

You will see two panels (FIFA World Cup and NBA) showing "Waiting for events…"

#### Step 6 — Start the event simulator

```bash
make simulate
# equivalent to: curl -X POST http://localhost/api/simulate/start
```

Within 2–4 seconds, events start appearing in both panels. Scores update, events scroll in, cards accumulate.

#### Step 7 — Explore the monitoring tools

```bash
make kafka-ui   # opens http://localhost:8090 in your browser
```

In Kafka UI you can:
- See `worldcup.events` and `nba.events` topics and their partitions
- Watch messages arriving in real time (click a topic → click "Messages")
- See the consumer group `sports-consumer-group` and its partition assignments
- Click "Schema Registry" to see the registered Avro schemas

#### Step 8 — Stop the simulator

```bash
make stop-sim
```

#### Step 9 — Shut down everything

```bash
make down
# equivalent to: docker compose down -v   (-v removes named volumes including Redis data)
```

---

### Option B — Local development (outside Docker)

Use this when you want hot-reload of TypeScript changes without rebuilding Docker images.

#### Step 1 — Start only the infrastructure in Docker

```bash
docker compose up -d zookeeper kafka-1 kafka-2 kafka-3 schema-registry redis kafka-ui
```

Wait ~30 seconds for Kafka to be ready.

#### Step 2 — Install all workspace dependencies

```bash
pnpm install
```

pnpm installs all packages for all workspaces in one command and creates the symlink `node_modules/@sport-tracking/schemas → packages/schemas`.

#### Step 3 — Build the shared schemas package

```bash
pnpm --filter @sport-tracking/schemas build
```

This compiles `packages/schemas/src/` to `packages/schemas/dist/`. The producer and consumer import types from `dist/`, so this step must happen before starting the apps.

#### Step 4 — Set local environment variables

```bash
export KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094
export SCHEMA_REGISTRY_URL=http://localhost:8081
export REDIS_URL=redis://localhost:6379
```

Or copy `.env.example` to `.env` and adjust values.

#### Step 5 — Start the producer

```bash
cd apps/producer
pnpm dev
```

Expected output:
```
[Nest] LOG [NestFactory] Starting Nest application...
[Nest] LOG [KafkaService] Topics already exist
[Nest] LOG [KafkaService] worldcup.events-value → schema id=1
[Nest] LOG [KafkaService] nba.events-value      → schema id=2
[Nest] LOG [KafkaService] Kafka producer connected
[Nest] LOG [NestApplication] Nest application successfully started +Xms
```

#### Step 6 — Start the consumer (new terminal)

```bash
cd apps/consumer
pnpm dev
```

Expected output:
```
[Nest] LOG [NestFactory] Starting Nest application...
[Nest] LOG [KafkaConsumerService] Connected to Kafka
[Nest] LOG [NestApplication] Nest application successfully started +Xms
```

#### Step 7 — Start the frontend (new terminal)

```bash
cd apps/frontend
pnpm dev
```

Expected output:
```
  VITE v5.x.x  ready in 500ms
  ➜  Local:   http://localhost:5173/
```

Open `http://localhost:5173` in your browser.

> **Note:** In local dev the frontend connects directly to `localhost:5173`. Socket.io connects to the same origin, but there's no nginx to proxy `/socket.io/` — so you need to configure the frontend to connect directly to the consumer port. The simplest workaround: keep the Vite dev proxy or set `VITE_SOCKET_URL=http://localhost:3002`.

#### Step 8 — Start the simulator

```bash
curl -X POST http://localhost:3001/simulate/start
```

---

### Scaling consumers (Docker only)

```bash
# Scale to 3 consumer replicas
make scale-consumer N=3
# equivalent to: docker compose up -d --scale consumer=3
```

Kafka rebalances the 6 partitions across 3 consumers (2 partitions each). The Socket.io Redis adapter ensures events processed by any replica reach all browser clients regardless of which replica they're connected to.

To scale back down:
```bash
docker compose up -d --scale consumer=1
```

---

### Sending events manually

You can send events directly without the simulator:

```bash
# World Cup goal
make send-wc-goal

# NBA 3-pointer
make send-nba-basket

# Custom event
curl -X POST http://localhost/api/events/worldcup \
  -H "Content-Type: application/json" \
  -d '{
    "matchId": "wc-match-1",
    "type": "YELLOW_CARD",
    "minute": 55,
    "homeTeam": { "id": "brazil", "name": "Brazil", "shortName": "BRA", "score": 1 },
    "awayTeam": { "id": "germany", "name": "Germany", "shortName": "GER", "score": 1 },
    "player": { "id": "p4", "name": "Casemiro", "number": 5, "teamId": "brazil" }
  }'
```

---

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Producer crashes on startup | Kafka not ready | Wait 30s and check `docker compose logs kafka-1` |
| `Schema Registry not reachable` | Schema Registry not ready | Check `docker compose logs schema-registry`; ensure `depends_on` health check passes |
| Dashboard stays "Waiting for events" | Simulator not started | Run `make simulate` |
| Dashboard shows "disconnected" | Consumer not running | Check `docker compose ps` and `docker compose logs consumer` |
| Kafka UI shows no topics | Kafka still starting | Wait and refresh; topics are created by the producer on first startup |
| Port 80 already in use | Another service on port 80 | Change the nginx port in `docker-compose.yml` `ports: - '8080:80'` |
| Docker build fails with lockfile error | NestJS deps not in lockfile | Run `pnpm install` locally first to regenerate `pnpm-lock.yaml`, then rebuild |

---

## 7. API reference

Base URL: `http://localhost/api/` (via nginx) or `http://localhost:3001/` (direct)

| Method | Path | Description |
|---|---|---|
| `GET`  | `/health` | Returns `{ ok: true, timestamp }` |
| `POST` | `/events/worldcup` | Publish a World Cup event |
| `POST` | `/events/nba` | Publish an NBA event |
| `POST` | `/simulate/start` | Start the random event simulator |
| `POST` | `/simulate/stop` | Stop the simulator |
| `GET`  | `/simulate/status` | Returns `{ running: true/false }` |

### World Cup event — required fields

```json
{
  "matchId": "wc-match-1",
  "type": "GOAL",
  "minute": 67,
  "homeTeam": { "id": "brazil",  "name": "Brazil",  "shortName": "BRA", "score": 2 },
  "awayTeam": { "id": "germany", "name": "Germany", "shortName": "GER", "score": 1 }
}
```

Optional: `"player": { "id", "name", "number", "teamId" }` and `"substitute": { ... }` (for SUBSTITUTION events).

Valid `type` values:

| Type | Description |
|---|---|
| `MATCH_START` | Kick-off |
| `HALF_TIME` | End of first half |
| `FULL_TIME` | End of match |
| `GOAL` | Goal scored |
| `OWN_GOAL` | Own goal |
| `YELLOW_CARD` | Yellow card shown |
| `RED_CARD` | Red card shown (or second yellow) |
| `SUBSTITUTION` | Player substitution (requires `player` + `substitute`) |
| `PENALTY_AWARDED` | Penalty kick awarded |
| `PENALTY_SCORED` | Penalty kick converted |
| `PENALTY_MISSED` | Penalty kick missed |

### NBA event — required fields

```json
{
  "gameId":  "nba-game-1",
  "matchId": "nba-game-1",
  "type": "BASKET_3PT",
  "quarter": 3,
  "clock": "4:22",
  "homeTeam": { "id": "lakers",   "name": "Los Angeles Lakers",   "shortName": "LAL", "score": 87 },
  "awayTeam": { "id": "warriors", "name": "Golden State Warriors", "shortName": "GSW", "score": 91 }
}
```

Optional: `"player": { ... }`, `"substitute": { ... }`, `"points": 2` or `3` or `1`.

Valid `type` values:

| Type | Points | Description |
|---|---|---|
| `GAME_START` | — | Tip-off |
| `QUARTER_START` | — | Start of a quarter |
| `QUARTER_END` | — | End of a quarter |
| `GAME_END` | — | Final buzzer |
| `BASKET_2PT` | 2 | Two-point field goal |
| `BASKET_3PT` | 3 | Three-point field goal |
| `FREE_THROW` | 1 | Free throw made |
| `FOUL` | — | Personal foul |
| `SUBSTITUTION` | — | Player substitution |
| `TIMEOUT` | — | Timeout called |

---

## 8. Kafka concepts explained

### Why Kafka instead of a message queue or REST polling?

| Concern | REST polling | Simple queue | Kafka |
|---|---|---|---|
| **Durability** | None (in-memory) | In-memory | Persisted to disk |
| **Replay** | No | No | Yes — rewind to any offset |
| **Fan-out** | N×polling | 1 consumer per queue | Many independent consumer groups |
| **Ordering** | Not guaranteed | FIFO globally | Ordered per partition |
| **Throughput** | Low | Medium | Very high |
| **Scale** | Hard | Hard | Designed for it |

### Partitions and ordering

Kafka does not give you a global ordering guarantee across a whole topic — only within a single partition. By using `matchId` as the partition key, all events for the same match are guaranteed to arrive at the consumer in the correct order (goal at 30' before goal at 45').

```
worldcup.events
  Partition 0: [wc-match-42 events]  → consumer-1
  Partition 1: [wc-match-17 events]  → consumer-1
  Partition 2: [wc-match-99 events]  → consumer-2
  Partition 3: [wc-match-5  events]  → consumer-2
  Partition 4: [wc-match-88 events]  → consumer-3
  Partition 5: [wc-match-71 events]  → consumer-3
```

### Consumer groups

Every consumer that joins the same group ID (`sports-consumer-group`) shares the partition load. Kafka assigns each partition to exactly one consumer in the group at any time. If a consumer crashes, Kafka reassigns its partitions to the surviving consumers (rebalance).

If you have 6 partitions and scale to 3 consumers, each consumer gets 2 partitions. Scaling beyond 6 consumers for a 6-partition topic gives no benefit — extra consumers sit idle.

### Offsets

Kafka tracks which messages each consumer group has processed using offsets. When a consumer calls `commitOffset()`, it tells Kafka "I have processed up to this message". If the consumer restarts, it resumes from the last committed offset rather than reprocessing everything.

---

## 9. Avro + Schema Registry explained

### The problem with raw JSON on Kafka

```
Producer sends:  {"id":"abc123","sport":"worldcup","matchId":"wc-1","type":"GOAL","minute":67,...}
                  ↑ field names are repeated in every single message
                  ↑ no enforcement that "type" is a valid value
                  ↑ consumer crashes on invalid JSON with no helpful error
```

### How Avro solves it

Avro stores the schema separately (in the Schema Registry), not inside each message. The message only contains the data values in binary form. Field names are looked up by position in the schema, not stored inline.

```
Producer sends:  [0x00][0x00 0x00 0x00 0x01][Avro binary]
                   ↑      ↑ schema ID (4 bytes)  ↑ packed binary values only
                 magic
```

A typical `WorldCupEvent` in JSON: ~200 bytes. Same event in Avro binary: ~30 bytes. At 10,000 events per second across 1,000 matches, that's the difference between 2 GB/min and 300 MB/min of Kafka storage.

### How the Schema Registry works

```
┌──────────┐  1. register(schema, subject)   ┌───────────────────┐
│ Producer │ ─────────────────────────────► │  Schema Registry  │
│          │ ◄─ schema id = 1 ─────────────  │                   │
│          │                                 │  subject: worldcup│
│          │  2. encode(id=1, event)          │  .events-value    │
│          │     → [0x00][id][avro bytes]     │  version 1        │
│          │                                 │  schema: { ... }  │
└──────────┘                                 └─────────┬─────────┘
                                                       │
┌──────────┐  4. decode(buffer)                        │
│ Consumer │ ─ reads id=1 ─────────────────────────── ┘
│          │   fetches schema (cached)
│          │   deserialises binary → JS object
└──────────┘
```

### Schema compatibility

When you call `registry.register()` with a new version of a schema, the registry checks it against the configured compatibility mode before accepting it:

| Mode | What's allowed |
|---|---|
| `BACKWARD` (current) | Add optional fields with defaults; delete fields |
| `FORWARD` | Add fields; consumers must ignore unknown fields |
| `FULL` | Both backward and forward compatible |
| `NONE` | Any change allowed (dangerous) |

With `BACKWARD`: old consumers can read data written by a new producer. Practical rule: you can add `"points": ["null", "int"]` with `"default": null` without breaking old consumers.

---

## 10. How consumer scaling works

### The problem

When you scale to 3 consumer instances, each handles a subset of Kafka partitions. But Socket.io clients connect to one instance. Without coordination:

```
Partition 2 → consumer-1 → broadcasts to clients of consumer-1 only ✗
               (clients connected to consumer-2 and consumer-3 see nothing)
```

### The solution: Socket.io Redis adapter

```
Partition 2 → consumer-1 → server.emit("worldcup:state", state)
                             │
                             ▼
                          Redis pub/sub channel "socket.io#..."
                             │
                   ┌─────────┴──────────┐
                   ▼                    ▼
              consumer-2           consumer-3
              server.emit(...)     server.emit(...)
              → its clients        → its clients
```

Every `server.emit()` call is intercepted by the `RedisIoAdapter`, published to a Redis pub/sub channel. All other instances are subscribed to that channel and forward the message to their own connected clients.

### Sticky sessions

Socket.io's initial handshake uses HTTP polling before upgrading to WebSocket. During the polling phase, the client must hit the same server instance. Nginx's `ip_hash` directive ensures this — the same client IP always routes to the same upstream consumer replica.

```nginx
upstream consumer_upstream {
    ip_hash;          # same IP → same consumer replica
    server consumer:3002;
}
```

---

## 11. How Turborepo works in this project

### Workspace graph

```
@sport-tracking/producer  ──┐
                             ├──► @sport-tracking/schemas
@sport-tracking/consumer  ──┤
                             │
@sport-tracking/frontend  ──┘
```

Turborepo reads `package.json` `dependencies` to build this graph. When you run `turbo build`:
1. Builds `@sport-tracking/schemas` first (no local deps)
2. Builds producer, consumer, frontend in parallel (all depend only on schemas)

### Build caching

```bash
# First run: compiles everything
turbo build  →  compiled in 45s

# Change only producer code and run again:
turbo build  →  schemas: cache hit (0ms), consumer: cache hit, frontend: cache hit
               producer: rebuilt (8s)
```

Turbo hashes: source files + package.json + env vars. If the hash matches a previous run, it replays the cached `dist/` output without recompiling.

### Filter commands

```bash
# Build only schemas (and nothing else)
pnpm --filter @sport-tracking/schemas build

# Build producer and all its local dependencies
pnpm --filter @sport-tracking/producer... build

# Run dev for all apps simultaneously
pnpm dev    # turbo starts all persistent dev tasks in parallel
```

---

## 12. Docker build strategy

All three Dockerfiles use `context: .` (repo root) so they can access `packages/schemas`. Each has three stages:

### Stage 1: installer (cached)

```dockerfile
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/schemas/package.json ./packages/schemas/
COPY apps/producer/package.json    ./apps/producer/
RUN pnpm install --frozen-lockfile
```

Only manifest files are copied before `pnpm install`. If you change source code but not `package.json`, Docker reuses this layer from cache — no reinstalling of node_modules.

### Stage 2: builder

```dockerfile
COPY --from=installer /app/node_modules ./node_modules
COPY packages/schemas ./packages/schemas
COPY apps/producer    ./apps/producer
RUN pnpm --filter @sport-tracking/schemas build
RUN pnpm --filter @sport-tracking/producer build
```

Copies the installed `node_modules` from stage 1, then compiles schemas before the app.

### Stage 3: production

```dockerfile
RUN pnpm install --frozen-lockfile --prod   # no devDependencies
COPY --from=builder /app/packages/schemas/dist ./packages/schemas/dist
COPY --from=builder /app/apps/producer/dist    ./apps/producer/dist
CMD ["node", "apps/producer/dist/main.js"]
```

Fresh base image with only production deps and compiled `dist/`. No TypeScript compiler, no source maps in the final image. The entry point is `main.js` (NestJS bootstrap).

The frontend's stage 3 uses `nginx:alpine` instead of Node — it serves the pre-built static files:
```dockerfile
FROM nginx:alpine
COPY --from=builder /app/apps/frontend/dist /usr/share/nginx/html
```

---

## 13. Port map

| Port | Service | URL |
|---|---|---|
| `80` | Nginx — main entry point | `http://localhost` |
| `3001` | Producer — direct access | `http://localhost:3001` |
| `8081` | Schema Registry | `http://localhost:8081/subjects` |
| `8090` | Kafka UI | `http://localhost:8090` |
| `9092` | Kafka broker-1 (host access) | — |
| `9093` | Kafka broker-2 (host access) | — |
| `9094` | Kafka broker-3 (host access) | — |
| `6379` | Redis (host access) | — |
| `2181` | Zookeeper (host access) | — |

Consumer port `3002` is internal only — accessed through nginx at `/socket.io/`.
