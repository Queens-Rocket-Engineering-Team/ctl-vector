# Deploying VECTOR

This document provides a step-by-step guide for deploying VECTOR (Vehicle Event, Control, Telemetry, and Operations Router) on a fresh Linux server using Docker Compose. The guide covers prerequisites, configuration, deployment, verification, and troubleshooting.

## Table of Contents
1. [Prerequisites](#1-prerequisites)
2. [Checkout](#2-checkout)
3. [Production Configuration](#3-production-configuration)
4. [Recording Storage and Permissions](#4-recording-storage-and-permissions)
5. [Start the Production Stack](#5-start-the-production-stack)
6. [Verify VECTOR](#6-verify-vector)
7. [Network Requirements](#7-network-requirements)
8. [Troubleshooting](#8-troubleshooting)
9. [Operational/Safety Notes](#9-operational/safety-notes)
10. [Maintenance/Update Procedure](#10-maintenance/update-procedure)

---

## 1. Prerequisites

Before deploying VECTOR, ensure your Linux server has:

- **Docker Engine** and **Docker Compose** installed (Docker version 20.10+, Compose v2+)
- **Git** installed for cloning the repository
- **Network access** to:
  - GitHub Container Registry (`ghcr.io`) for pulling container images
  - Docker Hub (`hub.docker.com`) for MediaMTX and Mumble images
- **Sufficient disk space** for recordings (plan based on expected session duration and frequency)
- **Administrator/sudo privileges** on the server for initial setup

No specific Linux distribution is required, but the server must support Docker containers.

## 2. Checkout

Obtain the VECTOR repository on your server:

```bash
# Clone the repository
git clone https://github.com/Queens-Rocket-Engineering-Team/ctl-vector.git

# Change to the repository directory
cd ctl-vector

# Initialize submodules (required for QLCP protocol library)
git submodule update --init --recursive
```

The submodule initialization is necessary because VECTOR depends on the `ctl-qlcp-lib` submodule for its native protocol implementation.

## 3. Production Configuration

Review and edit the following configuration files before deployment:

### config.yaml
This file controls VECTOR's behavior. Key sections to review:

- **accounts.camera**: ONVIF credentials for camera access
  ```yaml
  accounts:
    camera:
      username: <your-username>      # Replace with actual username
      password: <your-password>      # Replace with actual password
  ```

- **cameras**: Manual IP addresses for cameras that don't respond to discovery
  ```yaml
  cameras:
    - ip: 192.168.1.100              # Example: replace with actual camera IP
      onvif_port: 2020
  ```

- **services.discovery**: ONVIF discovery settings
  ```yaml
  services:
    discovery:
      enabled: true                  # Set false to disable discovery
      onvif_port: 2020               # Default ONVIF port
      timeout: 5.0                   # Seconds to wait for responses
      max_retries: 3                 # Discovery attempts
  ```

- **services.recordings**: Recordings directory configuration
  ```yaml
  services:
    recordings:
      root: ./recordings             # Host path (relative to ctl-vector/)
      mediamtx_container_root: /recordings  # Container path
  ```

### murmur.ini
Mumble server configuration. Review:
```ini
welcometext=Launch Control
port=64738                           # Default port for Mumble
serverpassword=propteambestteam      # Shared with all clients (change if needed)
bandwidth=72000                      # ~72kbps voice bandwidth
users=20                             # Maximum concurrent users
allowping=true
```

### .env (Optional)
Create a `.env` file in the repository root to override default values:
```env
# Image tags (default: latest for server, v1.0.0 for GUI)
IMAGE_TAG=latest
GUI_TAG=v1.0.0

# Docker UID/GID (default: 1000:1000)
DOCKER_UID=1000
DOCKER_GID=1000

# Server IP for GUI (optional)
PROP_SERVER_IP=
```

**Important**: Never commit real credentials to version control. Use environment variables or Docker secrets for production deployments if available.

## 4. Recording Storage and Permissions

VECTOR and MediaMTX must access the same recordings directory:

- **Why recordings/ is required**: Stores session telemetry.csv, session.json, camera video, and Mumble audio
- **Shared access**: Both VECTOR and MediaMTX containers mount the same host directory
- **Container paths**:
  - VECTOR container: `/app/recordings` (from `services.recordings.root`)
  - MediaMTX container: `/recordings` (from `mediamtx_container_root`)
- **Ownership matters**: Containers run as `DOCKER_UID:DOCKER_GID` (default 1000:1000)
- **Correcting ownership**: If the directory has incorrect ownership:
  ```bash
  sudo chown -R "$(id -u):$(id -g)" recordings
  ```
  Replace `$(id -u):$(id -g)` with your desired UID:GID if not using the current user.

Create the recordings directory if it doesn't exist:
```bash
mkdir -p recordings
```

## 5. Start the Production Stack

Deploy VECTOR using the production Compose file:

```bash
# Start all services in detached mode
docker compose -f compose.prod.yml up -d
```

This command will:
- Pull the latest images from GitHub Container Registry and Docker Hub
- Start four services: server (VECTOR), media (MediaMTX), mumble, and gui (HELM tablet build)
- Configure host networking for SSDP/SSDP discovery (required for QLCP)

### Managing the Stack

| Action | Command |
|--------|---------|
| View logs (all services) | `docker compose -f compose.prod.yml logs -f` |
| View logs (VECTOR only) | `docker compose -f compose.prod.yml logs -f server` |
| Inspect running containers | `docker compose -f compose.prod.yml ps` |
| Restart services | `docker compose -f compose.prod.yml restart` |
| Stop the stack | `docker compose -f compose.prod.yml down` |
| Stop and remove volumes | `docker compose -f compose.prod.yml down -v` |

### Startup Verification

Containers should show as "Up" in `docker compose ps`. Check logs for:
- Server: "Device [name] registered from [address]" when nodes connect
- MediaMTX: "RTSP server listening on [address]:[port]"
- Mumble: Server start confirmation
- No repeated restart loops or error messages

## 6. Verify VECTOR

### Server-only Verification (no hardware required)

1. **Check container status**:
   ```bash
   docker compose -f compose.prod.yml ps
   ```
   All services should show `State: Up`

2. **Inspect VECTOR logs**:
   ```bash
   docker compose -f compose.prod.yml logs -f server
   ```
   Look for startup completion and readiness indicators

3. **Access the API**:
   - Open `http://<server-ip>:8000/docs` in a browser
   - Should show the FastAPI interactive documentation
   - Alternative: `curl http://<server-ip>:8000/docs`

4. **Check system state**:
   ```bash
   curl http://<server-ip>:8000/v1/state
   ```
   Should return JSON with state_version, devices, kasa, commands, tares, and session

5. **Verify recordings directory access**:
   ```bash
   ls -la recordings/
   ```
   Should be readable/writable by the Docker user

### Hardware-dependent Verification (requires Control Nodes/cameras)

These steps require physical hardware on the network:

1. **Node discovery/connection**:
   - Check VECTOR logs for "Device [name] registered from [address]"
   - Verify in `/v1/state` under `devices` array

2. **Camera verification** (if configured):
   - Check `/v1/cameras` endpoint returns configured/discovered cameras
   - Verify MediaMTX is receiving streams (check MediaMTX logs)

3. **Mumble verification**:
   - Connect a Mumble client to `<server-ip>:64738`
   - Use password from `murmur.ini` (`serverpassword`)

4. **GUI verification**:
   - Tablet/build: Access `http://<server-ip>:8080` for static HELM tablet build
   - Desktop HELM: Point to `<server-ip>:8000` for REST/WebSocket

## 7. Network Requirements

VECTOR uses host networking for services requiring multicast/broadcast:

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| VECTOR API | 8000 | TCP | REST/WebSocket for HELM/clients |
| VECTOR QLCP TCP | 50000 | TCP | Control Node command/response |
| VECTOR QLCP UDP | 50001 | UDP | Telemetry ingestion |
| VECTOR Discovery | 239.100.0.1:10000 | UDP multicast | SSDP for Control Node discovery |
| MediaMTX RTSP | 8558 | TCP | Internal camera video pull (not exposed externally) |
| MediaMTX API | 9997 | TCP | MediaMTX configuration (used by VECTOR) |
| MediaMTX WebRTC | 8889 | TCP/UDP | WebRTC signaling for HELM/clients |
| Mumble | 64738 | TCP/UDP | Voice communications |
| GUI (tablet) | 8080 | TCP | Static file server for HELM tablet build |

**Host networking implications**:
- Services bind directly to host interfaces
- No port translation occurs (`8000` on container = `8000` on host)
- Ensure no other services conflict with these ports on the host
- Multicast/broadcast works without additional Docker configuration

**Firewall considerations** (if applicable):
- Allow inbound TCP 8000 (API) from control point/network
- Allow inbound TCP/UDP 64738 (Mumble) if using voice
- Allow inbound TCP 8080 (GUI tablet) if accessing from other devices
- Ensure outbound access to Docker registries for image pulls
- Multicast (239.100.0.1:10000) should be allowed on local subnet for discovery

## 8. Troubleshooting

### 8.1 Docker Service Issues

**Symptom**: `docker compose` command not found or Docker daemon not running
- **Check**: `docker version`, `systemctl status docker`
- **Likely cause**: Docker not installed or service not started
- **Action**: Install Docker Engine, start/enable docker service

### 8.2 Permission Denied on Recordings

**Symptom**: Containers restarting or failing to write recordings
- **Check**: `docker compose logs server` for permission errors
  - Look for: "Permission denied", "Operation not permitted"
- **Likely cause**: Mismatch between `DOCKER_UID:DOCKER_GID` and recordings directory ownership
- **Action**: 
  ```bash
  # Check directory ownership
  ls -ld recordings
  
  # Fix ownership (example for UID/GID 1000)
  sudo chown -R 1000:1000 recordings
  ```

### 8.3 Containers Repeatedly Restarting

**Symptom**: `docker compose ps` shows `Restarting` state
- **Check**: `docker compose logs <service-name>` for the failing service
- **Likely causes**:
  - **SERVER**: Invalid `config.yaml`, missing QLCP protocol build
  - **MEDIA**: MediaMTX configuration conflict, port already in use
  - **MUMBLE**: Invalid `murmur.ini`, port conflict
  - **GUI**: Image pull failure, network issue
- **Action**: Fix the underlying issue identified in logs

### 8.4 VECTOR Cannot Read config.yaml

**Symptom**: Server container exits immediately or logs show YAML parsing errors
- **Check**: `docker compose logs server`
- **Likely causes**:
  - File not found (incorrect volume mount)
  - Syntax error in YAML
  - File not readable by container user
- **Action**:
  - Verify `./config.yaml:/app/config.yaml` mount in `compose.prod.yml`
  - Validate YAML syntax: `yamllint config.yaml` or online validator
  - Check file permissions: `ls -l config.yaml`

### 8.5 MediaMTX Problems

**Symptom**: No video, cameras not showing in GUI, recording failures
- **Check**: `docker compose logs media`
- **Likely causes**:
  - RTSP port conflict (should be internal at :8558)
  - Recording path mismatch between VECTOR and MediaMTX
  - Missing dependencies (older versions)
- **Action**:
  - Verify `services.recordings.root` and `mediamtx_container_root` match
  - Check MediaMTX version in `compose.prod.yml` (currently 1.20.0)
  - Ensure recordings directory is writable

### 8.6 Camera Connectivity Problems

**Symptom**: No cameras discovered or configured, `/v1/cameras` empty
- **Check**:
  - `docker compose logs server` for discovery/manual config messages
  - Network connectivity to camera IPs
  - ONVIF port accessibility (default 2020)
- **Likely causes**:
  - Cameras on different subnet (multicast doesn't route)
  - Incorrect credentials in `accounts.camera`
  - Cameras not ONVIF-compliant or ONVIF disabled
  - Firewall blocking UDP 3702 (WS-Discovery) or TCP 2020 (ONVIF)
- **Action**:
  - For different subnets: Use manual `ip` entries in `cameras:` list
  - Verify credentials work with ONVIF client tools
  - Test ONVIF port connectivity: `nc -vz <camera-ip> 2020`

### 8.7 Node Discovery/QLCP Connectivity Problems

**Symptom**: No nodes appearing in `/v1/state` or VECTOR logs
- **Check**: `docker compose logs server` for registration messages
- **Likely causes**:
  - Multicast blocked (UDP 239.100.0.1:10000 for discovery)
  - TCP 50000 blocked (QLCP command/response)
  - UDP 50001 blocked (telemetry)
  - Nodes not powered on or not connected to network
  - Node firmware issues
- **Action**:
  - Verify multicast works on network segment
  - Check firewall settings for required ports
  - Confirm nodes are powered and on same LAN
  - Validate node firmware is functioning

### 8.8 API Unavailable on Port 8000

**Symptom**: Cannot access `http://<server-ip>:8000/docs`
- **Check**: `docker compose logs server` for startup errors
- **Likely causes**:
  - Port 8000 already in use on host
  - Server container failed to start
  - Container not actually listening on 0.0.0.0:8000
- **Action**:
  - Check for port conflicts: `sudo ss -tlnp | grep :8000`
  - Restart Docker stack: `docker compose -f compose.prod.yml restart server`
  - Check container ports: `docker compose port server 8000`

### 8.9 Wrong Image/Tag

**Symptom**: Pull errors, unexpected behavior, version mismatches
- **Check**: `docker compose logs` for pull/failure messages
- **Likely causes**:
  - Invalid `IMAGE_TAG` or `GUI_TAG` in `.env` or environment
  - Image doesn't exist in registry
  - Network issues preventing pull
- **Action**:
  - Verify image exists: `docker pull ghcr.io/queens-rocket-engineering-team/ctl-vector:latest`
  - Check available tags via GitHub Packages web interface
  - Correct `.env` or environment variables

### 8.10 GUI Unable to Communicate with VECTOR

**Symptom**: HELM tablet shows connection errors, no state updates
- **Check**:
  - VECTOR logs for WebSocket connection attempts
  - Network connectivity between GUI device and server
  - Server API accessibility from GUI device
- **Likely causes**:
  - Network routing issues between control point and pad LAN
  - Firewall blocking TCP 8000 between networks
  - Incorrect `PROP_SERVER_IP` in `.env` (if set)
  - GUI build version incompatibility
- **Action**:
  - Test connectivity: `curl http://<server-ip>:8000/v1/state` from GUI device
  - Verify wireless link functionality
  - Ensure GUI can reach server IP:port

### 8.11 Recordings Not Being Created

**Symptom**: Sessions start but no files appear in recordings/
- **Check**:
  - `docker compose logs server` for session start/stop messages
  - `docker compose logs media` for recording errors
  - Directory permissions and available space
- **Likely causes**:
  - Recordings directory not writable by container user
  - Insufficient disk space
  - MediaMTX configuration issues
  - Session not actually starting (check API response)
- **Action**:
  - Fix ownership/permissions on recordings/
  - Check disk space: `df -h .`
  - Verify session start via `/v1/sessions` API
  - Check MediaMTX logs for recording path errors

### 8.12 Stale/Root-owned Recording Files

**Symptom**: Permission errors after sudo operations, files owned by root
- **Check**: `ls -la recordings/` for root-owned files
- **Likely cause**: Running commands with sudo that created files as root
- **Action**:
  ```bash
  # Restore ownership to Docker user (example: 1000:1000)
  sudo chown -R 1000:1000 recordings
  ```

## 9. Operational/Safety Notes

### Docker/Server Availability vs. System Safety

- **Docker/server availability**: Containers running and responsive does **not** indicate the physical system is safe
- **VECTOR command success**: A `sent` REST response only means transmission to selected nodes, not physical actuation
- **Node acknowledgement**: A reported STATUS response indicates node received and processed command
- **Actual physical safe state**: Determined by node hardware defaults and firmware watchdogs

### Key Safety Behaviors

1. **Node firmware watchdogs**: 
   - Nodes reset controls to configured defaults after 5 minutes without server packets
   - This operates independently of VECTOR and requires no server communication

2. **VECTOR GUI watchdog**:
   - Sends QLCP ESTOP to registered nodes after 10 minutes with no `/ws/state` clients
   - Any open `/ws/state` connection (including tablets) prevents watchdog from tripping
   - Field tablets are expected to be disconnected during armed operations

3. **Hardware power-loss behavior**:
   - Solenoids de-energize; actuated valves return to spring-biased defaults
   - This requires no software or network command

**Important**: A successful ESTOP send means only that the TCP write succeeded. Device-reported states and physical feedback are separate observations. Reconnecting, sending ESTOP, or receiving an acknowledgement alone does **not** establish that the complete system is safe to approach.

## 10. Maintenance/Update Procedure

### Obtaining Latest Changes

```bash
# Fetch latest repository changes
git pull origin main

# Update submodules if needed
git submodule update --init --recursive
```

### Reviewing Configuration Changes

1. Check for new/changed configuration options in:
   - `config.yaml` (compare with your customized version)
   - `murmur.ini`
   - `compose.prod.yml` (for image tag changes)
2. Review `README.md` and subsystem docs for new features or requirements

### Updating Production Containers

```bash
# Pull latest images and recreate containers
docker compose -f compose.prod.yml pull
docker compose -f compose.prod.yml up -d
```

Alternatively, for a one-step update:
```bash
docker compose -f compose.prod.yml up -d --build
```

### Post-update Verification

1. Check container status: `docker compose -f compose.prod.yml ps`
2. Review logs for startup errors: `docker compose -f compose.prod.yml logs`
3. Verify API accessibility: `curl http://<server-ip>:8000/v1/state`
4. Confirm recordings directory accessibility and permissions
5. If hardware is available, verify node discovery and camera connectivity

### Reverting to Previous Version

If an update causes issues:
```bash
# Stop current stack
docker compose -f compose.prod.yml down

# Checkout previous version (if using git tags)
git checkout <previous-tag>
git submodule update --init --recursive

# Or reset to previous commit
git reset --hard <previous-commit>
git submodule update --init --recursive

# Restart with previous images
docker compose -f compose.prod.yml up -d
```

---

**Note**: This documentation reflects the current repository state. For the most accurate information, always refer to:
- `compose.prod.yml` (source of truth for production container configuration)
- `README.md` (general usage and development information)
- Subsystem documentation in `docs/` for detailed behavior explanations