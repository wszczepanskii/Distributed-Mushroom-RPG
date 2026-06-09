# Distributed Mushroom RPG

A university-grade **2D multiplayer top-down RPG** demonstrating distributed systems concepts:

- **Two region servers** (left / right map halves) communicating via **gRPC**
- **RabbitMQ** fanout broadcasts for real-time client synchronization
- **Ricart-Agrawala distributed mutual exclusion** for mushroom pickup (no duplicate items)
- **Transparent player handoff** when crossing the map border

## Architecture

```
┌─────────────┐     gRPC (actions)      ┌──────────────┐
│  Pygame     │◄───────────────────────►│  Server 1    │
│  Client     │                         │  (left map)  │
│             │     gRPC (actions)      └──────┬───────┘
│             │◄───────────────────────►       │ gRPC RegionSync
│             │                         ┌──────▼───────┐
│             │                         │  Server 2    │
│             │                         │  (right map) │
└──────┬──────┘                         └──────┬───────┘
       │                                       │
       │         RabbitMQ fanout               │
       └─────────────── game.updates ──────────┘
              (movement, spawns, pickups)
```

| Layer           | Technology        | Purpose                            |
| --------------- | ----------------- | ---------------------------------- |
| Client UI       | Pygame            | 2D grid rendering, input           |
| Client ↔ Server | gRPC + protobuf   | Join, move, pickup (authoritative) |
| Server ↔ Server | gRPC `RegionSync` | Handoffs, Ricart-Agrawala locks    |
| Broadcast       | RabbitMQ (`pika`) | Push state deltas to all clients   |

### Map split

- Grid: **40 × 20** tiles (`shared/config.py`)
- **Border at x = 20**: columns `0–19` → Server 1, `20–39` → Server 2
- Mushrooms within **2 tiles** of the border use **distributed locking**

### Mutual exclusion (mushroom pickup)

See `server/mutex.py` for the full Ricart-Agrawala explanation. In short:

1. Player presses **Space** → client calls `PickupMushroom` on its connected server
2. If the mushroom is near the border, server sends `RequestMushroomLock` to peer via gRPC
3. Peer applies **defer/grant** rules using Lamport timestamps
4. Winner atomically removes mushroom; both sides notified via RabbitMQ + `NotifyMushroomRemoved`

## Project structure

```
rpgGame/
├── client/              # Pygame client
├── server/              # Region servers + mutex + gRPC servicers
├── shared/              # Config, models, RabbitMQ event encoding
├── protos/              # game.proto
├── generated/           # Compiled protobuf (run compile script)
├── scripts/             # Proto compiler helper
├── docker-compose.yml   # RabbitMQ
└── requirements.txt
```

## Prerequisites

- Python 3.10+
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (for RabbitMQ)

## Setup

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts/compile_protos.py
```

## Run RabbitMQ

```powershell
docker compose up -d
```

Verify: open http://localhost:15672 (login `guest` / `guest`).

## Start servers

Use **two terminals**:

```powershell
# Terminal 1 — left region
python -m server.main --server server1

# Terminal 2 — right region
python -m server.main --server server2
```

Server 1 gRPC: `localhost:50051`
Server 2 gRPC: `localhost:50052`

## Start clients (2 players)

```powershell
# Player 1 (spawns on left)
python -m client.main --name Alice --server localhost:50051

# Player 2 (spawns on right)
python -m client.main --name Bob --server localhost:50052
```

### Controls

| Key           | Action                           |
| ------------- | -------------------------------- |
| WASD / Arrows | Move                             |
| Space         | Pick up mushroom on current tile |
| Esc           | Quit                             |

Walk across the **yellow border** to trigger a server handoff.

### Match rules

- **2-minute timer** starts when the first player joins
- **5 mushrooms** always active on the map
- Each pickup instantly spawns replacements until the count is back to 5 (random server/location)
- When time runs out, the player with the **most mushrooms wins** (draw on tie)

## Configuration

Edit `shared/config.py` for map size, ports, RabbitMQ credentials, and border lock radius.

## Stopping

```powershell
docker compose down
```

## Files to highlight for your professor

| File                          | Why                                 |
| ----------------------------- | ----------------------------------- |
| `server/mutex.py`             | Ricart-Agrawala + Lamport clocks    |
| `server/grpc_servicer.py`     | Pickup critical section, handoff    |
| `protos/game.proto`           | Client + inter-server RPC contracts |
| `server/rabbitmq_client.py`   | Fanout broadcast pattern            |
| `client/rabbitmq_consumer.py` | Event-driven client state sync      |
