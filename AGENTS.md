# Hermes Channels

Keep README.md for people choosing, installing and using this project. Put durable
working guidance here, code navigation in an existing index, and task history in
the relevant plan or audit; do not turn this file into a session log.

Read [INDEX.md](INDEX.md) first: it owns the component map, WhatsApp behavior
decisions, parity guardrails and test guidance.

Channels owns WhatsApp transport, arrival/session bookkeeping and delivery.
MCP owns Soul turns and memory; the canonical owner and existing Souls come
from MCP. Preserve working Hermes bridge/web-source behavior by hunk-porting,
not reconstructing it. Trace both live arrival and history before changing
message dispatch or identity.
