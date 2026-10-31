# CLAUDE.md for ai-systems-conversation-hub

Rules for all ai-systems repos: `ai-systems-docs/CLAUDE.md`
(local: `../../docs/CLAUDE.md`). What the service does: `README.md`.

## Commands

| Command                    | Does                                                             |
| -------------------------- | ---------------------------------------------------------------- |
| `npm ci`                   | install from the lock file (builds kit and contracts on install) |
| `npm run check`            | `tsc --noEmit` (TypeScript 7)                                    |
| `npm run lint`             | ESLint (strict type-checked) and Prettier check                  |
| `npm test`                 | unit tests, two coverage gates (see below)                       |
| `npm run verify`           | check, lint and test                                             |
| `npm run test:integration` | `test/integration` with Testcontainers (needs Docker)            |
| `npm start`                | `node src/main.ts`                                               |

## Layout

```
src
├── domain/conversation.ts             decide, evolve, intent rules, reply and status of an event; no I/O
├── application/post-owner-message.ts  the command handler
├── adapters/http.ts                   routes, zod schemas of the bodies and queries
├── adapters/orchestration.ts          the ORCHESTRATION handler, the tasks collection, 001-tasks
└── main.ts                            config, wiring by constructor, start, shutdown
test
├── unit                               node:test, kit in-memory event store
└── integration                        MongoDB and NATS in Testcontainers
```

## Rules

- conversation-hub publishes only `conversation.*` events. Never publish
  or append an event that orchestration reads as an input
  (`conversation.OwnerMessagePosted`) from the consumer path: a reply must
  never start a task.
- The consumer filter is `orchestration.>` and the consumer is `ordered`.
  The gitops grant pins the filter. Other event types are acked with no
  effect.
- Each handler effect and `markProcessed` are in one transaction (`also`
  of `append`, or `session.withTransaction`). The task state `ended` is
  terminal in the update filter. A replay of `ORCHESTRATION` changes
  nothing.
- Readers before writers: conversation-hub takes a new contracts version
  before orchestration sends new event types (README "Deploy order").
- Pass `parse: parseEvent` from contracts v0.3.0 to the store and the
  consumer.

## Coverage gates (ADR 0017)

- `test:domain`: `src/domain`, 90% lines and 85% branches.
- `test:unit`: `src` without `main.ts`, 70% lines.

`main.ts` and `adapters/orchestration.ts` are covered by the integration
test.

## Integration tests

The NATS config in the test has the grants of `ai-systems-conversation-hub`,
`ai-systems-orchestration` and `ai-systems-admin` from gitops
(`kubernetes/infrastructure/nats/values.yaml`). Change them together with
gitops. Wait with `waitFor`, never with a fixed sleep. The CI job
`integration` runs on `ubuntu-22.04` (see the kit `CLAUDE.md`).

## Dependencies

- Kit and contracts come from git tags. A new tag needs a new
  `allowScripts` entry with the commit of the tag
  (`npm install-scripts approve <package>`). The kit asks for contracts
  v0.1.0, and npm dedupes it to the v0.3.0 copy of this service. Check
  with `npm ls @famashines/ai-systems-contracts`.
- `mongodb` and `zod` have the exact versions that the kit pins. Check
  with `npm ls zod mongodb`.
- TypeScript 6 (`typescript`, for ESLint) and 7 (`typescript-7`, for
  `check`): ADR 0022. Never run `npx tsc`.
