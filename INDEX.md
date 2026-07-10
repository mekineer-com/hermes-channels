# hermes-channels Index

## Purpose

`hermes-channels` is the standalone communications layer for OpenAlma. Today it
owns WhatsApp live delivery, WhatsApp history/web-source capture, channel policy
metadata, and the bridge between external messages and `mcp-memu-server`.

This repo was extracted from `hermes-agent`, but it is not meant to recreate the
old Hermes framework. The intended shape is smaller:

- WhatsApp protocol handling stays here.
- memU/OpenAlma owns the soul turn, memory, policy, and server-side history.
- The daemon talks to memU over HTTP instead of running the old in-process agent
  stack.
- Bridges/web-source remain close copies of working Hermes code, with only path,
  config, and process-boundary seams changed.

## Main Components

- `bridge/`: Baileys live-message bridge. Durable queue, message ingest,
  history ingest, known contacts/chats, sent-message echo tracking, media retry.
  This should remain copy-equivalent to final working `hermes-agent` bridge code
  unless a Channels-specific path/config seam requires a small delta.
- `web-source/`: whatsapp-web.js history/source daemon. Captures reconciled
  WhatsApp history and metadata for source reads.
- `gateway/daemon.py`: standalone Python controller. Starts/stops bridge and
  web-source, polls bridge messages, owns WAL/replay, builds `MessageEvent`s,
  calls memU, sends responses, and persists transcript rows in Channels state.
- `gateway/state_db.py`: small raw-SQL state store for just Channels tables:
  messages, sessions, processed source keys, arrivals, outbounds, souls.
- `gateway/contact_store.py`, `whatsapp_identity.py`, `whatsapp_seam.py`,
  `whatsapp_known_contacts.py`: WhatsApp identity/contact evidence and display
  seams.
- `gateway/memu_client.py`: HTTP client for memU turn calls and outbound queue
  claim/mark operations.
- `gateway/memu_policy.py`: channel policy lookup from `data/memu.json`.

## Why This Shape Exists

Old Hermes bundled platform runtime, agent runtime, profile machinery, plugins,
terminal/browser behavior, and WhatsApp protocol code in one gateway. OpenAlma
only needs the channel/runtime piece here. The extraction intentionally removed
old framework complexity:

- no generic Hermes plugin hook layer
- no broad multi-profile gateway orchestration
- no terminal/browser agent runtime
- no old in-process soul-mode stack
- no generic auth/pairing flows beyond OpenAlma WhatsApp needs

That simplification should stay. The rule is not "restore Hermes". The rule is:
keep the smaller controller, but preserve final working Hermes WhatsApp message
semantics.

## Porting Rule

If behavior already exists in `hermes-agent`, do not reconstruct it from memory.
Use final `hermes-agent` HEAD as the behavior reference, check `git log` on the
source file for later fixes, then hunk/copy the behavior and adapt only the seam
names/imports required by Channels.

Never use `hermes-agent-main/` as proof that something worked before; it is a
separate upstream/latest reference, not the OpenAlma working baseline.

## Behavior Decisions To Preserve

These are deliberate Channels/OpenAlma decisions, not bugs to "fix" back to old
Hermes:

- WhatsApp group sessions are shared by group chat, not split per participant.
  Sender identity remains metadata, not conversation identity.
- `whatsapp_message_arrivals` is evidence only. It records history/live arrivals
  but is not a durable response claim and must not become a trigger/skip gate.
- `_active_source_keys` is load-bearing. It covers the in-flight window after a
  source key leaves batching and before `processed_source_keys` is written.
- `_is_history_live_text_copy` is load-bearing. It prevents history+live copies
  from merging duplicate text before the first flush.
- Do not restore bare `message_source_key_exists` as a duplicate skip. History
  rows can exist before the live turn has been answered; only processed ledger,
  response-exists fallback, and active source key should suppress response.
- Do not add a blanket bot-mode `fromMe` drop. Final working Hermes allows
  phone-originated `fromMe` messages through; bot echoes are filtered through
  sent-message echo tracking.
- History may trigger a turn when it is the first new-enough arrival. That is a
  July 2026 OpenAlma behavior, not old Hermes parity.
- Pending WhatsApp follow-ups are newest-wins. One reply per busy chat is the
  intended behavior; background/web-source history is responsible for content
  recovery, not the live turn payload.
- WAL marked processed on failure is intentional to avoid retry storms. The user
  receives an error notice and the message remains persisted.
- `_drop_orphan_pending` clears active source keys but leaves WAL unmarked for
  replay.
- `whatsapp_wal.append()` must increment WAL seq only after durable append.

## Important Edge Cases

- Staleness uses the WhatsApp message timestamp, not daemon receipt time.
- Live stale rows must mark WAL processed before returning.
- `deliveryMode` controls live vs persist-only vs revoke. Missing/invalid values
  are treated as non-live and should log a warning.
- Response delivery source-key stamping is best-effort after send. A DB stamp
  failure must not convert an already-delivered WhatsApp reply into a failed
  turn.
- Private/free-turn replies route to the operator self-DM. Public replies route
  to the origin chat. Outbound claims use `claimed_by="channels"`.
- Session rotation metadata matters because server readers chain through
  `parent_session_id`. A crash between `sessions.json` and DB session row write
  is a metadata hole, not message loss, but avoid worsening that window.
- Bridge-seq WAL dedup assumes the bridge queue directory is durable. If re-pair
  or queue wipe bugs appear, check for new bridge seq collisions against old
  daemon WAL rows before changing dispatch semantics.

## Common Workflows

### Run Python Tests

```bash
python -m pytest -q tests
```

### Run Bridge Tests

```bash
cd bridge
npm test
```

### Run Web-Source Tests

```bash
cd web-source
npm test
python -m pytest -q test_store.py
```

## Before Changing WhatsApp Semantics

1. Check `PLAN_hermes_channels_parity_map.md` in the workspace root if present.
2. Search memory for `project=apps-codex--hermes-channels`.
3. Compare against final `hermes-agent` HEAD, not archived/runtime data.
4. Classify the difference as one of:
   - required Channels glue
   - intentional OpenAlma behavior
   - accidental rewrite drift
   - unclear product decision
5. Only code accidental drift. For unclear product decisions, ask Marcos.
6. Add a focused test at the first wrong boundary.

## Files That Are Usually The First Boundary

- Live bridge payload wrong: `bridge/message_ingest.js`, `bridge/history_ingest.js`, `bridge/known_state.js`
- Event construction wrong: `gateway/daemon.py::_build_message_event`
- Dispatch/dedup/WAL wrong: `gateway/daemon.py::_dispatch_built_message_event`, `_is_duplicate_source_message`, `on_processing_complete`
- State persistence wrong: `gateway/state_db.py`, `_persist_history_event`, `_persist_exception_turn`
- Outbound delivery wrong: `gateway/daemon.py::drain_outbounds`, `_deliver_outbound`, `_handle_response_delivery`
- Policy/display wrong: `gateway/memu_policy.py`, `gateway/channel_directory.py`, `gateway/contact_store.py`
