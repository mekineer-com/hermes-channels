<p align="center">
  <img src="assets/banner.png" alt="hermes-channels" width="100%">
</p>

# hermes-channels

WhatsApp communications layer for [OpenAlma](https://github.com/mekineer-com/OpenAlma). Handles live message delivery, history capture, and the bridge between WhatsApp and the memU memory server.

Part of the OpenAlma stack — not a standalone WhatsApp bot framework.

## Lineage

hermes-channels is extracted from [hermes-agent](https://github.com/NousResearch/hermes-agent) by [Nous Research](https://nousresearch.com) — an open-source self-improving AI agent framework. We kept only the WhatsApp protocol and gateway layer; the soul turn, memory, and policy logic moved to [mcp-memu-server](https://github.com/mekineer-com/mcp-memu-server). The Baileys bridge and whatsapp-web.js web-source remain close to their hermes-agent originals.

## Architecture

```
WhatsApp ──► Baileys bridge (Node.js)  ──► gateway/daemon.py ──► mcp-memu-server
         └── wwebjs web-source (Node.js) ──┘                       (memU HTTP API)
```

- **`bridge/`** — Baileys live-message bridge: ingest, WAL, history, known contacts, media retry
- **`web-source/`** — whatsapp-web.js history/source daemon: captures reconciled history and metadata
- **`gateway/daemon.py`** — Python controller: starts bridge and web-source, polls messages, builds turn events, calls memU, delivers responses
- **`gateway/state_db.py`** — SQLite state: messages, sessions, WAL, arrivals, outbounds

## Configuration

Config file: `data/config.json` (created on first run). Override with environment variables:

| Env var | Default | Purpose |
|---------|---------|---------|
| `CHANNELS_MEMU_BASE_URL` | `http://127.0.0.1:8099` | memU server URL |
| `CHANNELS_SOUL_ID` | `default` | Soul identity |
| `CHANNELS_USER_ID` | `marcos` | User scope |
| `CHANNELS_BRIDGE_PORT` | `3000` | Baileys bridge port |
| `CHANNELS_HOME` | platform default | Data directory |

Channel policy (which conversations the soul responds to) lives in `data/memu.json`.

## Requirements

- Python 3.11+
- Node.js 18+

```bash
pip install -e .
cd bridge && npm install
cd web-source && npm install
```

## Running

Managed by [OpenAlma](https://github.com/mekineer-com/OpenAlma) launcher. To run standalone:

```bash
python -m gateway.daemon
```

## Tests

```bash
# Python gateway tests
python -m pytest -q tests/

# Bridge tests
cd bridge && npm test

# Web-source tests
cd web-source && npm test
```

## License

GPLv3. The upstream hermes-agent bridge code is MIT (Nous Research) — GPLv3 is compatible.
