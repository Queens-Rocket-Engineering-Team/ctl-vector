# Deploying VECTOR

Sets up VECTOR on a Linux server using the published Docker images. To run from source, see the [README](README.md#development).

## Prerequisites

- 64-bit Debian-based Linux (Raspberry Pi OS 64-bit, Ubuntu, Debian) on the pad LAN, with `sudo` and internet access during setup
- The machine's first user account (`id -u` prints `1000`)
- A fixed IP on the pad LAN (static or DHCP reservation)
- Camera IPs and login, if using cameras

## Setup

1. **Install Docker**, then log out and back in:
   ```bash
   sudo apt update && sudo apt install -y git curl
   curl -fsSL https://get.docker.com | sudo sh
   sudo usermod -aG docker "$USER"
   ```

2. **Clone the repo:**
   ```bash
   git clone https://github.com/Queens-Rocket-Engineering-Team/ctl-vector.git
   cd ctl-vector
   ```

3. **Edit `config.yaml`:** set `accounts.camera` and the `cameras` list (`cameras: []` for none). Leave `services` as is.

4. **Start it.** Create `recordings/` first, or Docker creates it as root and recording fails:
   ```bash
   mkdir -p recordings
   docker compose -f compose.prod.yml pull
   docker compose -f compose.prod.yml up -d
   ```
   Containers restart automatically after reboots.

5. **Check it:**
   - `docker compose -f compose.prod.yml ps`: all four services are `running`.
   - `http://<server-ip>:8000/docs` and `http://<server-ip>:8080` load from another machine.
   - Nodes appear in the server logs, and HELM shows data and video.

## Operating

Run these from the `ctl-vector` folder. Always include `-f compose.prod.yml`; without it, Compose uses the development stack.

| Task | Command |
|---|---|
| Logs | `docker compose -f compose.prod.yml logs -f server` |
| VECTOR terminal | `docker attach ctl-vector-server-1`. Detach with **Ctrl-P Ctrl-Q**. Ctrl-C stops the server. |
| Stop / start | `docker compose -f compose.prod.yml down` / `up -d` |
| Update | `git pull && docker compose -f compose.prod.yml pull && docker compose -f compose.prod.yml up -d` |

- **If no HELM or tablet connects within 10 minutes of startup**, VECTOR ESTOPs all nodes. See [SAFETY.md](docs/SAFETY.md).
- **Servers set up before the rename:** run `docker compose -p prop-teststand -f compose.prod.yml down` once before starting.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `permission denied ... docker.sock` | Log out and back in after installing Docker. |
| Compose tries to build from source | You left out `-f compose.prod.yml`. |
| `server` keeps restarting; port in use | An old `prop-teststand` stack or a `uv run` VECTOR is still running. Find it with `docker ps` and stop it. |
