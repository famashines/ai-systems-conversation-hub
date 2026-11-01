# Changelog

## Unreleased

- `/internal/commands/*` needs an owner bearer token (M2-17, ADR 0028).
  A path-scoped `kit/auth` hook checks the role `owner`, the issuer, the
  audience and `exp`: no token or a bad token gives 401, no `owner` role
  gives 403. The `actor.id` of the command must be the `sub` of the token,
  else 403. The replay API, `/health`, `/ready` and `/metrics` need no
  token.
- New env: `OIDC_ISSUER`, `OIDC_JWKS_URL`, `OIDC_AUDIENCE` (the same as
  budget).

## 0.1.0

- Conversation aggregate on the stream `conversation-<conversationId>`:
  `PostOwnerMessage` gives `conversation.ConversationStarted` (first
  message only) and `conversation.OwnerMessagePosted` with an intent
  (`new_task` or `steer`), all schema version 1 (contracts v0.3.0).
- `POST /internal/commands/post-owner-message` with command idempotency by
  `commandId`: 201, 200 on a replay, 400, 409 for a `taskId` that is not
  in the thread, 503 when MongoDB is down.
- Durable ordered consumer `conversation-hub` on `ORCHESTRATION`
  (`orchestration.>`): task state in the collection `tasks`, and
  `conversation.AgentReplyPosted` from `RunCompleted`, `RunFailed`,
  `RunBlocked` and `MessageRefused`, once for each source event id.
- `GET /internal/events`: the replay API.
- `/health`, `/ready` (MongoDB and migrations), `/metrics`.
- Outbox relay to `CONVERSATION`, graceful shutdown.
- Review fixes: task state changes have a `processed` marker and `ended`
  is terminal, so a replay does not make an ended task `running` again.
  A reason with no text gives the fixed reply text `No reason was
given.`. `MessageRefused` gives a reply and changes no task state. Readiness has `nats`, `relay` and `consumer`. An
  owner message does not read the stream. Deploy order in the README.
- Container image, CI (gitleaks, verify, audit, integration, image) and
  the release workflow.
