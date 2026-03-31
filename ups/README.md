# Mini-UPS: A Scalable Package Delivery Simulation System
A distributed logistics backend that simulates a UPS-like package delivery service. The system coordinates trucks, warehouses, and packages by communicating with a **World Simulator** (via TCP/Protocol Buffers) and an **Amazon frontend** (via REST/JSON), while providing users with a web interface to track, manage, and redirect their deliveries in real time.
**Course:** ECE 568 — Engineering Robust Server Software (Duke University)  
**Team:** cg387, xl487
---
## Architecture Overview
┌──────────────┐ TCP / Protobuf ┌──────────────────┐ HTTP / JSON ┌──────────────┐ │ World │◄─────────────────►│ UPS Backend │◄──────────────►│ Amazon │ │ Simulator │ │ (Django + Daemon) │ │ Service │ └──────────────┘ └────────┬───────────┘ └──────────────┘ │ ┌────────┴───────────┐ │ PostgreSQL 13 │ │ (trucks, packages,│ │ warehouses, etc.) │ └────────────────────┘ │ ┌────────┴───────────┐ │ Web UI (Django │ │ Templates + AJAX) │ └────────────────────┘

The system consists of three main runtime components:
1. **UPS Daemon** — A multithreaded background process that manages truck assignments, world communication, and Amazon message queuing.
2. **World Connection** — Maintains a persistent TCP socket to the world simulator, sending/receiving length-prefixed protobuf messages (pickups, deliveries, truck queries, acks).
3. **Amazon Communication** — Handles bidirectional HTTP/JSON with the Amazon service: receives pickup requests inbound, sends truck arrivals and delivery updates outbound via a database-backed message queue.
---
## Features
- **Real-time package tracking** — Users can look up any package by tracking number and see live status updates.
- **Interactive delivery map** — A map view showing warehouses, trucks, and in-flight packages with live AJAX polling.
- **Package redirection** — Authenticated users can change a package's delivery destination before it enters the final delivery phase.
- **Email & in-app notifications** — Configurable per-user notification preferences (delivery, pickup, truck arrival, system) with email delivery via SMTP.
- **Admin dashboard** — Overview of all trucks and recent packages for system operators.
- **World control panel** — View and manage the active world simulator connection.
- **Reliable message delivery** — Sequence-number-based ack tracking with the world simulator; database-backed outbox pattern with retry logic for Amazon messages.
- **Automatic truck assignment** — Background loop continuously matches idle trucks to packages waiting for pickup.
- **Stale truck monitoring** — Periodically queries the world simulator for trucks that haven't reported status updates.
---
## Tech Stack
| Layer         | Technology                                      |
|---------------|--------------------------------------------------|
| Language      | Python 3.10                                       |
| Web Framework | Django 3.2                                        |
| Database      | PostgreSQL 13                                     |
| Serialization | Protocol Buffers (proto2)                         |
| HTTP Client   | `requests`                                        |
| DB Driver     | `psycopg2-binary` (ORM + raw SQL where needed)   |
| Frontend      | Django templates, AJAX/JSON APIs                  |
| Email         | Django SMTP (Gmail)                               |
| Containers    | Docker, Docker Compose                            |
---
## Database Schema
The system uses Django ORM models mapped to the following tables:
| Table                    | Purpose                                                    |
|--------------------------|------------------------------------------------------------|
| `trucks`                 | Truck ID, status (idle/traveling/loading/delivering/arrive_warehouse), position, world ID |
| `warehouses`             | Warehouse ID and coordinates                               |
| `packages`               | Tracking ID (PK), user, truck, warehouse, status, destination, timestamps |
| `items`                  | Line items belonging to a package                          |
| `notifications`          | Per-user notification messages (read/unread)               |
| `notification_preferences` | Per-user toggles for notification types                 |
| `world_state`            | Active world connection metadata                           |
| `sequence_num`           | Sequence/ack tracking for world protobuf protocol          |
| `amazon_messages`        | Outbox queue for messages to Amazon (pending/sent/failed)  |
| `command_logs`           | Logs of commands received from Amazon                      |
| `command_retry_queue`    | Retry queue for failed world commands                      |
| `error_logs`             | World simulator error messages                             |
---
## Getting Started
### Prerequisites
- Docker & Docker Compose
- Access to a running **World Simulator** instance
- A paired **Amazon** service endpoint
### Configuration
Create a `.env` file in the project root:
```env
# World Simulator
WORLD_HOST=<world-simulator-host>
WORLD_PORT=12345
# Amazon Service
AMAZON_URL=http://<amazon-host>:<port>
# PostgreSQL (used by the container)
POSTGRES_DB=ups_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres_password
# Django DB connection
DB_HOST=ups-db
DB_PORT=5432
DB_NAME=ups_db
DB_USER=postgres
DB_PASSWORD=postgres_password
Run with Docker Compose
docker-compose up --build
This will:

Start a PostgreSQL 13 container (ups-db) on host port 5433.
Build and start the UPS server container (ups-server) on host port 8000.
Automatically run Django migrations and launch the development server.
The UPS daemon starts as a background thread within the Django process, connecting to the world simulator and Amazon service.
Access the Web UI
Home: http://localhost:8000/
Register/Login: http://localhost:8000/register/ or http://localhost:8000/login/
Dashboard: http://localhost:8000/dashboard/
Track a Package: http://localhost:8000/track/
Delivery Map: http://localhost:8000/delivery-map/
Admin Dashboard: http://localhost:8000/admin/dashboard/
World Control: http://localhost:8000/admin/world/
API Endpoints
Endpoint	Method	Description
/api/amazon/	POST	Inbound API for Amazon service
/api/truck/<id>/	GET	Truck status (AJAX)
/api/package/<tracking>/	GET	Package status (AJAX)
/api/map-data/	GET	Real-time map data (trucks, packages, warehouses)
/api/notifications/	GET	User notifications
Protocol
UPS ↔ World Simulator (TCP + Protobuf)
Uses a custom proto2 schema (world_ups-1.proto) with varint length-delimited framing:

Connect: UConnect / UConnected (with truck initialization)
Commands (UCommands): UGoPickup, UGoDeliver, UQuery, acks, simspeed, disconnect
Responses (UResponses): UFinished (pickup complete), UDeliveryMade, UTruck (status query result), UErr, acks
UPS ↔ Amazon (HTTP/JSON)
Inbound (Amazon → UPS): request_pickup, package_ready, loading_package, package_loaded, query_status, heartbeat
Outbound (UPS → Amazon): truck_arrived, package_loaded, delivery_started, package_delivered, redirect_package, query_status
Messages are queued in the amazon_messages table and processed by a background thread with retry logic.

Project Structure
.
├── docker-compose.yml          # Container orchestration
├── Dockerfile                  # Python 3.10 image with dependencies
├── wait-for-postgres.sh        # DB readiness check script
├── .env                        # Environment configuration
└── ups/
    ├── manage.py               # Django management
    ├── requirements.txt        # Python dependencies
    ├── config/
    │   ├── settings.py         # Django settings
    │   ├── urls.py             # URL routing
    │   ├── wsgi.py
    │   └── asgi.py
    ├── core/
    │   ├── models.py           # Database models
    │   ├── views.py            # Web views and API endpoints
    │   ├── apps.py             # App config (daemon auto-start)
    │   ├── admin.py
    │   ├── global_context.py   # Shared runtime references
    │   ├── migrations/         # Django migrations
    │   └── templates/core/     # HTML templates
    ├── ups_daemon.py           # Main daemon (truck assignment, monitoring)
    ├── world_connection.py     # World simulator TCP/protobuf client
    ├── amazon_communication.py # Amazon HTTP/JSON communication
    ├── notification_manager.py # Email and in-app notification system
    └── world_ups_1_pb2.py      # Generated protobuf Python code
