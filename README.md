# Ditto Google Messages bridge

An independent AGPL-3.0 service that adapts
[`mautrix/gmessages`' libgm](https://github.com/mautrix/gmessages) to Ditto's
small HTTPS connector contract. It pairs a Google Messages web session with an
Android phone, reads SMS/RCS history, keeps an incremental cursor, and sends
text messages through the paired phone. New connections use Google Messages'
linked-device QR flow, so users never need to copy Google session cookies into
Ditto. The older Google-account/cookie flow remains available only as a
compatibility fallback for deployments where QR pairing is unavailable.

The service is separate from Ditto's private backend because libgm is
AGPL-3.0. It runs as one always-on instance: pairing and the unofficial
Google Messages web protocol both require a live, stateful connection.

## Configuration

- `BRIDGE_TOKEN` (required): at least 32 characters; every `/v1/*` request must
  use it as a bearer token.
- `PORT` (optional): HTTP port, default `8080`.
- `LOG_LEVEL` (optional): `debug`, `info`, `warn`, or `error`; default `info`.

`GET /health` is unauthenticated and contains no account data. The three
authenticated endpoints are `POST /v1/connect`, `POST /v1/sync`, and
`POST /v1/send`; their wire format is documented in Ditto backend's
`docs/google-messages-bridge-api.md`.

## Local development

```sh
export BRIDGE_TOKEN="$(openssl rand -hex 32)"
go test ./...
go run .
```

Do not expose a local instance without TLS. Production should terminate HTTPS
at the platform edge, keep exactly one instance, disable CPU throttling, and
store `BRIDGE_TOKEN` in a managed secret store.

## Operational limits

- Pairing flows expire after five minutes.
- Request bodies are capped at 1 MiB.
- Initial history is bounded to 30 days and 200 conversations.
- Sync responses contain at most the caller's requested limit (maximum 500).
- Sessions idle for 30 minutes are disconnected and can be reconstructed from
  the encrypted session returned to Ditto.

Google Messages web is an unofficial protocol. Pairing can expire, the Android
phone must remain online, and Google can change or revoke the flow at any time.
Google has removed QR pairing for some accounts and regions; those users cannot
use the QR-first Ditto flow until a secure first-party Google-account handoff is
available. A normal Google OAuth token does not grant access to personal
Messages/RCS, so the bridge does not present OAuth as if it did.
