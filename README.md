# ai-systems-conversation-hub

The Conversation service of ai-systems (ADR 0021). It keeps each chat
thread as an event-sourced stream `conversation-<conversationId>` in
MongoDB. It gives each owner message an `intent` from the state of the
tasks of the thread, and it makes the agent replies from orchestration
events. The `events` collection is the outbox: a relay publishes each
event to the NATS JetStream stream `CONVERSATION`. The service is
internal: only api-gateway (commands) and projections (replay) call it.
It has no Ingress.

```
api-gateway --POST /internal/commands/post-owner-message--> conversation-hub
                                                              | one transaction
                                                              v
                                      MongoDB conversation_hub: events, commands
                                                              | outbox relay
                                                              v
                                      NATS stream CONVERSATION (conversation.>)
                                        |
                                        +--> orchestration reads only conversation.OwnerMessagePosted

NATS stream ORCHESTRATION --durable "conversation-hub", filter orchestration.>, ordered-->
  conversation-hub: tasks (task state) and conversation.AgentReplyPosted
projections --GET /internal/events--> conversation-hub (rebuild only)
```

Architecture and reasons: `ai-systems-docs` (`ARCHITECTURE.md` 3, 5 to 8,
ADR 0006, 0007, 0021, 0023). Schemas: `@famashines/ai-systems-contracts`
v0.3.0.

## Events

| Event                              | When                                                    |
| ---------------------------------- | ------------------------------------------------------- |
| `conversation.ConversationStarted` | The first message to a new `conversationId`             |
| `conversation.OwnerMessagePosted`  | Each owner message, with its `intent`                   |
| `conversation.AgentReplyPosted`    | An orchestration event that the owner must read (below) |

All have schema version 1. There is no `StartConversation` command: the
first message to a new id starts the thread.

## Intent rules

conversation-hub keeps the state of each task of a thread in the
collection `tasks`, from the orchestration events:

| Orchestration event                     | Task state |
| --------------------------------------- | ---------- |
| `TaskSubmitted` with a `conversationId` | `running`  |
| `RunStarted`                            | `running`  |
| `RunBlocked`                            | `blocked`  |
| `RunCompleted`, `RunFailed`             | `ended`    |

`ended` is terminal: no later or replayed event changes it. Each state
change and the kit `processed` marker of its event are in one MongoDB
transaction, so a replay of `ORCHESTRATION` (for example after the
durable consumer is made again) does not change the state.

`MessageRefused` changes no task state: orchestration can refuse a
message for a task that is still live (for example a blocked task while
the cap is still reached), and the owner must be able to resume it.

A `TaskSubmitted` with no `conversationId` (a task from `POST /api/tasks`)
is not a task of a thread. Its later events give no state and no reply.

The rules, in this order:

| Message                                      | Intent                                 |
| -------------------------------------------- | -------------------------------------- |
| `newTask: true`                              | `new_task` (no `taskId`)               |
| `taskId` of a `running` or `blocked` task    | `steer` with that `taskId`             |
| `taskId` of an `ended` task                  | `new_task`                             |
| `taskId` that is not a task of the thread    | 409 `conflict`, no event               |
| no `taskId`, the thread has a `running` task | `steer` with the latest `running` task |
| no `taskId`, no `running` task               | `new_task`                             |

The latest task is the one with the latest `TaskSubmitted` time. A
`blocked` task is a `steer` target only when the owner replies to it with
its `taskId`, so a blocked task does not take all later messages of the
thread. The contract refuses `taskId` together with `newTask: true` (400).

Orchestration checks the intent against its own task state. When it
refuses a `steer`, it sends `orchestration.MessageRefused`, and the reason
comes in the thread as a reply.

## Replies

| Orchestration event | Reply text | `runId` |
| ------------------- | ---------- | ------- |
| `RunCompleted`      | `summary`  | yes     |
| `RunFailed`         | `reason`   | yes     |
| `RunBlocked`        | `reason`   | yes     |
| `MessageRefused`    | `reason`   | no      |

The service removes NUL characters from the text. A `reason` can be
valid and still have no text that is not white space (for example
`" "`). Then the reply text is `No reason was given.`, for all source
events, so the reply is always a valid `ReplyText`.

- The reply has `sourceEventId` (the orchestration `eventId`), a new
  `messageId`, `causationId` = the source event id, the `correlationId` of
  the source event and the actor `{ kind: "system", id: "conversation-hub" }`.
- Once only: the reply, the task state and the kit `processed` marker of
  the source event id are in one MongoDB transaction. A redelivery of the
  source event is acked with no new reply.
- A reply never reaches orchestration: conversation-hub publishes only
  `conversation.*` events, and orchestration reads only
  `conversation.OwnerMessagePosted`. The integration test checks this with
  the orchestration grants and its durable consumer.
- The consumer is `ordered` (kit): one message at a time, so a
  `TaskSubmitted` is always applied before the later events of its task.
  An event that fails 5 times goes to `dlq.conversation-hub.<subject>`.

## API

| Method | Path                                           | Does                                                                                                                                                                                                                            |
| ------ | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| POST   | `/internal/commands/post-owner-message`        | Post an owner message. Needs an owner bearer token.                                                                                                                                                                             |
| GET    | `/internal/events?afterPosition=<n>&limit=<m>` | Replay API: all events in position order. No token.                                                                                                                                                                             |
| GET    | `/health`                                      | Liveness: 200 while the process runs                                                                                                                                                                                            |
| GET    | `/ready`                                       | Readiness: 200 when MongoDB and NATS answer, the migrations are done, the relay sees `CONVERSATION` and the consumer exists, else 503 with the names of the failing checks (`mongo`, `nats`, `relay`, `consumer`, `migrations`) |
| GET    | `/metrics`                                     | Prometheus metrics                                                                                                                                                                                                              |

Errors are RFC 9457 problem JSON with a `code` (`@famashines/ai-systems-kit/http`).

### Callers and trust

A NetworkPolicy is L4: it limits pod and port, not the caller. So the
service does not trust `actor.kind` `owner` in the body (ADR 0028,
M2-17). The command handler checks the token first, with `bearerHook`
of `kit/auth`, as budget does: `Authorization: Bearer <access token>`
with the role `owner`, issuer `OIDC_ISSUER`, audience `OIDC_AUDIENCE` and
`exp`. The check comes before the body is read, so a caller with no token
gets 401 and no validation errors. The check is in the handler, not on a
path prefix, so it also applies to a path with empty segments (for
example `//internal/commands/post-owner-message`, which the kit router
matches). Then the `actor.id` of the command must be the `sub` of the
token. A new command route must call the same check. The read and replay
routes have no check and stay open to the callers that the network lets
in.

| Caller      | Paths                                   | Token                    |
| ----------- | --------------------------------------- | ------------------------ |
| api-gateway | `/internal/commands/post-owner-message` | Forwards the owner token |
| projections | `/internal/events`                      | None                     |
| Prometheus  | `/metrics`                              | None                     |

### `POST /internal/commands/post-owner-message`

The request needs an owner bearer token (see "Callers and trust"). The
body is a `CommandEnvelopeV1` with a
`PostOwnerMessageCommandPayloadV1`:

```json
{
  "commandId": "6f1c2b9e-1d1a-4c55-9b0e-3a7f0d5e8c21",
  "type": "conversation.PostOwnerMessage",
  "actor": { "kind": "owner", "id": "<sub of the token>" },
  "payload": {
    "conversationId": "0b6f9c1e-6a43-4d2b-8f3e-2b8f1a9d7c10",
    "text": "Write a poem",
    "newTask": false
  }
}
```

- `commandId` is the idempotency key: a lower-case UUID v4. api-gateway
  takes it from the `Idempotency-Key` header.
- `type` must be `conversation.PostOwnerMessage`. `expectedVersion` is
  refused. `actor.kind` must be `owner`.
- `taskId` is optional; `newTask` is `false` by default.

| Status   | When                                                             | Body                                                                                                                                                                |
| -------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 201      | A new message                                                    | `{ conversationId, messageId, intent, taskId? }`                                                                                                                    |
| 200      | The same `commandId` came before                                 | The first answer again. No new event.                                                                                                                               |
| 400      | The body is not valid                                            | Problem JSON with `errors` (paths, never values)                                                                                                                    |
| 401      | No token, or a token that is not valid                           | Problem JSON                                                                                                                                                        |
| 403      | No `owner` role, or `actor.id` is not the `sub` of the token     | Problem JSON                                                                                                                                                        |
| 409      | The `taskId` is not a task of the thread, or a concurrent append | Problem JSON, `code` `conflict`                                                                                                                                     |
| 413, 415 | The body is too large, or not JSON                               | Problem JSON                                                                                                                                                        |
| 503      | MongoDB does not answer in 5 s, or Keycloak cannot be read       | Problem JSON, `code` `unavailable`, with a fixed `detail`: `The database is not available` or `The keys of the identity provider are not available`. No host names. |

The service makes the `messageId` (UUID v4). The events have
`correlationId` = the `messageId`, `causationId` = the `commandId`, and
the `actor` of the command. The service does not read the stream. It
appends `ConversationStarted` and `OwnerMessagePosted` with
`expectedVersion: 0`. If the stream exists (`ConcurrencyError`), it
appends only `OwnerMessagePosted` with no version check. Both appends
store the `commandId`, so a replay gives the first answer. A 409 is
possible only in a rare race: two appends to the same stream that get
the same version in the same moment. Send the message again with the same
`commandId`.

### `GET /internal/events`

The same as orchestration: `afterPosition` (default `0`) and `limit`
(1 to 1000, default `100`), digits only. The answer is
`{ "events": [ ... ] }`, each an `EventEnvelopeV1` with its `position`.
Take the `position` of the last item as the next `afterPosition`.

## Data

| Collection                                     | Content                                                                           |
| ---------------------------------------------- | --------------------------------------------------------------------------------- |
| `events`, `commands`, `counters`, `migrations` | kit event store and outbox                                                        |
| `processed`                                    | kit consumer markers (source event ids), 45 days                                  |
| `tasks`                                        | `{ _id: taskId, conversationId, submittedAt, status }`, index on `conversationId` |

`tasks` is state that conversation-hub needs for the intent. Its source is
the `ORCHESTRATION` stream, not the `conversation-*` streams.

## Configuration

Environment variables (`.env.example` has all of them):

| Variable                                                            | Default                                         | Content                                      |
| ------------------------------------------------------------------- | ----------------------------------------------- | -------------------------------------------- |
| `MONGODB_URI`                                                       | required                                        | MongoDB connection string (replica set)      |
| `MONGODB_DATABASE`                                                  | `conversation_hub`                              | Database name                                |
| `NATS_SERVERS`                                                      | required                                        | NATS URLs, comma-separated; at least one     |
| `NATS_USER`                                                         | required                                        | `ai-systems-conversation-hub` on the cluster |
| `NATS_PASSWORD`                                                     | required                                        | From the SealedSecret                        |
| `OIDC_ISSUER`                                                       | `https://auth.famashines.com/realms/ai-systems` | Keycloak realm                               |
| `OIDC_JWKS_URL`                                                     | `<issuer>/protocol/openid-connect/certs`        | JWKS inside the cluster                      |
| `OIDC_AUDIENCE`                                                     | `ai-systems-app`                                | Audience and `azp` of the token              |
| `PORT`                                                              | `8080`                                          | HTTP port                                    |
| `LOG_LEVEL`                                                         | `info`                                          | pino level                                   |
| `RELEASE`                                                           | `local`                                         | Release id in logs and metrics               |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | not set                                         | Traces go over OTLP only when one is set     |

The kit service name is fixed to `conversation-hub`. So the NATS inbox
prefix is `_INBOX_conversation-hub`, the durable consumer on
`ORCHESTRATION` is `conversation-hub`, and the DLQ subjects are
`dlq.conversation-hub.>`. These are the gitops grants (ADR 0023).

Deploy: set `OIDC_JWKS_URL` to the Keycloak service inside the cluster,
and add a NetworkPolicy egress from conversation-hub to Keycloak on port
8080 for the JWKS, as for budget. Keycloak is not a readiness check:
when the JWKS cannot be read, a command gets 503.

NATS, the relay and the consumer are readiness checks, as in
projections: the service is not ready while it cannot read
`ORCHESTRATION` or publish to `CONVERSATION`. A stuck relay shows in `outbox_oldest_pending_seconds`, a stuck
consumer in `consumer_oldest_held_seconds`. Put an alert on both. NATS
must be available at start.

## Run locally

Node 26 (`nvm use`), a MongoDB replica set, and NATS with JetStream and
the streams `CONVERSATION` and `ORCHESTRATION`.

```sh
npm ci
cp .env.example .env
node --env-file=.env src/main.ts
```

Fill in the required values in `.env` and delete the lines that you do
not set. The service refuses an empty value.

## Container image

```sh
docker build --build-arg RELEASE=$(git rev-parse HEAD) -t ai-systems-conversation-hub .
```

The same image rules as orchestration: Node 26 slim by digest, no npm and
no npx, `node src/main.ts` as uid 1000, a read-only root file system
works, no `HEALTHCHECK`.

Release: when `ci` passes on a push to `main`, `.github/workflows/release.yml`
builds, scans and pushes `ghcr.io/famashines/ai-systems-conversation-hub:sha-<commit>`
and opens a pull request from `release/ai-systems-conversation-hub-prod`
in `famashines/gitops`. It needs the org secret `GITOPS_TOKEN` with access
to this repo, and the file
`kubernetes/ai-systems/ai-systems-conversation-hub/prod/kustomization.yaml`
in gitops. Neither is there yet (the deploy is task M2-09).

## Deploy order

conversation-hub must have a contracts version before orchestration
sends an event type that is new in that version. The consumer reads all
of `orchestration.>`, and an event that the contracts do not know fails
the parse and goes to the DLQ. So: release contracts, then
conversation-hub (and the other readers of `ORCHESTRATION`), then
orchestration.

api-gateway must forward the `Authorization` header of the owner on
`POST /api/conversations/:id/messages` before this service checks the
token (M2-17). Deploy that api-gateway first. Else each owner message
gets 401 here, and api-gateway gives 502.

## Known limits

- The token is checked twice: at api-gateway and here. A command is
  refused when the token ends between the two checks (`exp`, with the
  clock tolerance of `kit/auth`). Send it again with a new token.
- The JWKS comes over plain HTTP inside the cluster (ARCHITECTURE 11.1,
  accepted).

- The intent `answer` is not made. Contracts v0.3.0 has no event that
  says a task waits for answers to clarifying questions. It comes with
  the clarifying events (contracts v0.4.0, M3).
- A message that comes before conversation-hub has the `TaskSubmitted` of
  the first message gives `new_task`, not `steer`: the task id is not
  known yet.
- A chat task with no terminal event stays `running`, and it takes the
  later messages with no `taskId`. A refusal does not end it. So
  orchestration (M2-06) must always end a task with `RunCompleted` or
  `RunFailed`.
- The decision reads `tasks` outside the append transaction. If a task
  ends between the read and the append, the message steers an ended task,
  and orchestration answers with `MessageRefused`.

## Test

| Command                    | Does                                                 |
| -------------------------- | ---------------------------------------------------- |
| `npm run verify`           | type check, lint, unit tests with the coverage gates |
| `npm run test:integration` | MongoDB and NATS in Testcontainers (needs Docker)    |

Commands for agents: `CLAUDE.md`.
