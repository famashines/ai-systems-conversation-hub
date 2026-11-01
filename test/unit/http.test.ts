import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import type { JwtVerifier } from "@famashines/ai-systems-kit/auth";
import { createService, ForbiddenError, UnauthorizedError } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { createMemoryEventStore, fakeClock, fakeIds } from "@famashines/ai-systems-kit/testing";
import { routes } from "../../src/adapters/http.ts";
import type { ThreadTask } from "../../src/domain/conversation.ts";

const clock = fakeClock();
const store = createMemoryEventStore({ now: clock.now, newId: fakeIds(1) });
const messageIds = fakeIds(2);
const tasks = new Map<string, ThreadTask[]>();
const tokens: Record<string, JwtVerifier> = {
  owner: () => Promise.resolve({ sub: "owner-1", roles: ["owner"], exp: 0 }),
  guest: () => Promise.reject(new ForbiddenError("The role is missing")),
};
const verify: JwtVerifier = (token) =>
  (tokens[token] ?? (() => Promise.reject(new UnauthorizedError())))(token);
const app = createService({
  logger: createLogger({ service: "conversation-hub", release: "test", level: "silent" }),
  routes: routes({
    store,
    newId: messageIds,
    threadTasks: (conversationId) => Promise.resolve(tasks.get(conversationId) ?? []),
    verify,
  }),
  drainDelayMs: 0,
});
let base: string;

before(async () => {
  const { port } = await app.listen(0, "127.0.0.1");
  base = `http://127.0.0.1:${String(port)}`;
});

after(async () => {
  await app.close();
});

const ids = fakeIds(3);
const owner = { kind: "owner", id: "owner-1" } as const;

const envelope = (payload: Record<string, unknown>, overrides: Record<string, unknown> = {}) => ({
  commandId: ids(),
  type: "conversation.PostOwnerMessage",
  actor: owner,
  payload: { text: "Write a poem", ...payload },
  ...overrides,
});

const send = (body: unknown, authorization: string | null = "Bearer owner") =>
  fetch(`${base}/internal/commands/post-owner-message`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(authorization === null ? {} : { authorization }),
    },
    body: JSON.stringify(body),
  });

const problem = async (response: Response) => ({
  status: response.status,
  code: ((await response.json()) as { code: string }).code,
});

const answerOf = async (response: Response) =>
  (await response.json()) as { conversationId: string; messageId: string; intent: string };

test("the first message gives 201 new_task and starts the thread", async () => {
  const conversationId = ids();
  const command = envelope({ conversationId });
  const response = await send(command);
  assert.equal(response.status, 201);
  const answer = await answerOf(response);
  assert.deepEqual(answer, { conversationId, messageId: answer.messageId, intent: "new_task" });
  const stream = await store.readStream(`conversation-${conversationId}`);
  assert.deepEqual(
    stream.map((event) => [event.type, event.version, event.causationId, event.correlationId]),
    [
      ["conversation.ConversationStarted", 1, command.commandId, answer.messageId],
      ["conversation.OwnerMessagePosted", 2, command.commandId, answer.messageId],
    ],
  );
  assert.deepEqual(stream[1]?.payload, {
    conversationId,
    messageId: answer.messageId,
    text: "Write a poem",
    intent: "new_task",
  });
  assert.deepEqual(stream[1].actor, owner);
});

test("a message while a task runs gives steer with its taskId", async () => {
  const conversationId = ids();
  const taskId = ids();
  await send(envelope({ conversationId }));
  tasks.set(conversationId, [
    { taskId, status: "running", submittedAt: clock.now().toISOString() },
  ]);
  const response = await send(envelope({ conversationId, text: "Shorter" }));
  assert.equal(response.status, 201);
  const answer = await answerOf(response);
  assert.deepEqual(answer, {
    conversationId,
    messageId: answer.messageId,
    intent: "steer",
    taskId,
  });
  const stream = await store.readStream(`conversation-${conversationId}`);
  assert.equal(stream.length, 3);
  assert.equal(stream[2]?.type, "conversation.OwnerMessagePosted");
  assert.deepEqual(stream[2].payload, {
    conversationId,
    messageId: answer.messageId,
    text: "Shorter",
    intent: "steer",
    taskId,
  });
});

test("a new task keeps its mode; a message for a clarifying task gives answer", async () => {
  const conversationId = ids();
  const taskId = ids();
  const first = await answerOf(await send(envelope({ conversationId, mode: "auto" })));
  tasks.set(conversationId, [
    { taskId, status: "clarifying", submittedAt: clock.now().toISOString() },
  ]);
  const response = await send(envelope({ conversationId, text: "For kids" }));
  assert.equal(response.status, 201);
  const answer = await answerOf(response);
  assert.deepEqual(answer, {
    conversationId,
    messageId: answer.messageId,
    intent: "answer",
    taskId,
  });
  const stream = await store.readStream(`conversation-${conversationId}`);
  assert.deepEqual(
    stream.slice(1).map((event) => event.payload),
    [
      {
        conversationId,
        messageId: first.messageId,
        text: "Write a poem",
        intent: "new_task",
        mode: "auto",
      },
      { conversationId, messageId: answer.messageId, text: "For kids", intent: "answer", taskId },
    ],
  );
});

test("the same commandId gives 200 with the same answer and no new event", async () => {
  const command = envelope({ conversationId: ids() });
  const first = await send(command);
  const count = (await store.readAll({ limit: 1000 })).length;
  const second = await send(command);
  assert.equal(first.status, 201);
  assert.equal(second.status, 200);
  assert.deepEqual(await second.json(), await first.json());
  assert.equal((await store.readAll({ limit: 1000 })).length, count);
});

test("a taskId that is not in the thread gives 409", async () => {
  const response = await send(envelope({ conversationId: ids(), taskId: ids() }));
  assert.equal(response.status, 409);
  assert.equal(response.headers.get("content-type"), "application/problem+json");
  assert.equal(((await response.json()) as { code: string }).code, "conflict");
});

test("no token or a bad token gives 401, a token with no owner role gives 403", async () => {
  const conversationId = ids();
  const body = envelope({ conversationId });
  for (const authorization of [null, "Bearer bad", "Basic YTpi"]) {
    assert.deepEqual(await problem(await send(body, authorization)), {
      status: 401,
      code: "unauthorized",
    });
  }
  assert.deepEqual(await problem(await send(body, "Bearer guest")), {
    status: 403,
    code: "forbidden",
  });
  assert.equal((await store.readStream(`conversation-${conversationId}`)).length, 0);
});

test("no token gives 401 before the body is read, also on a path with empty segments", async () => {
  const count = (await store.readAll({ limit: 1000 })).length;
  for (const path of [
    "/internal/commands/post-owner-message",
    "//internal/commands/post-owner-message",
    "/internal//commands/post-owner-message",
  ]) {
    for (const body of ["{}", "not json", JSON.stringify(envelope({ conversationId: ids() }))]) {
      const response = await fetch(`${base}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
      });
      assert.deepEqual(await problem(response), { status: 401, code: "unauthorized" }, path);
    }
  }
  assert.equal((await store.readAll({ limit: 1000 })).length, count);
});

test("an actor that is not the subject of the token gives 403", async () => {
  const conversationId = ids();
  const body = envelope({ conversationId }, { actor: { kind: "owner", id: "owner-2" } });
  assert.deepEqual(await problem(await send(body)), { status: 403, code: "forbidden" });
  assert.equal((await store.readStream(`conversation-${conversationId}`)).length, 0);
});

test("the replay API, health and ready need no token", async () => {
  for (const path of ["/internal/events", "/health", "/ready"]) {
    assert.equal((await fetch(`${base}${path}`)).status, 200, path);
  }
});

const conversationId = "00000000-0000-4000-8000-0000000000c1";

for (const [name, body, path] of [
  ["no text", envelope({ conversationId, text: undefined }), "body.payload.text"],
  ["a blank text", envelope({ conversationId, text: " \n " }), "body.payload.text"],
  [
    "a text over 20000 characters",
    envelope({ conversationId, text: "a".repeat(20001) }),
    "body.payload.text",
  ],
  ["no conversationId", envelope({}), "body.payload.conversationId"],
  ["a bad conversationId", envelope({ conversationId: "x" }), "body.payload.conversationId"],
  ["a bad taskId", envelope({ conversationId, taskId: "x" }), "body.payload.taskId"],
  [
    "a taskId with newTask",
    envelope({ conversationId, taskId: "00000000-0000-4000-8000-0000000000c2", newTask: true }),
    "body.payload.taskId",
  ],
  [
    "a taskId with a mode",
    envelope({ conversationId, taskId: "00000000-0000-4000-8000-0000000000c2", mode: "auto" }),
    "body.payload.mode",
  ],
  ["a bad mode", envelope({ conversationId, mode: "fast" }), "body.payload.mode"],
  [
    "a newTask that is not a boolean",
    envelope({ conversationId, newTask: "yes" }),
    "body.payload.newTask",
  ],
  [
    "another command type",
    envelope({ conversationId }, { type: "conversation.PostAgentReply" }),
    "body.type",
  ],
  [
    "an expectedVersion",
    envelope({ conversationId }, { expectedVersion: 0 }),
    "body.expectedVersion",
  ],
  [
    "an agent actor",
    envelope({ conversationId }, { actor: { kind: "agent", id: "a" } }),
    "body.actor.kind",
  ],
  [
    "a system actor",
    envelope({ conversationId }, { actor: { kind: "system", id: "s" } }),
    "body.actor.kind",
  ],
  ["a bad commandId", envelope({ conversationId }, { commandId: "not-a-uuid" }), "body.commandId"],
] as const) {
  test(`PostOwnerMessage with ${name} gives 400 problem JSON`, async () => {
    const response = await send(body);
    assert.equal(response.status, 400);
    assert.equal(response.headers.get("content-type"), "application/problem+json");
    const problem = (await response.json()) as { code: string; errors: { path: string }[] };
    assert.equal(problem.code, "validation_failed");
    assert.ok(
      problem.errors.some((issue) => issue.path === path),
      JSON.stringify(problem.errors),
    );
  });
}

test("the replay API gives events in position order, in pages", async () => {
  const all = (await (await fetch(`${base}/internal/events?limit=1000`)).json()) as {
    events: { position: number }[];
  };
  assert.ok(all.events.length >= 3);
  assert.deepEqual(
    all.events.map((event) => event.position),
    all.events.map((_, index) => index + 1),
  );
  const page = (await (await fetch(`${base}/internal/events?afterPosition=1&limit=2`)).json()) as {
    events: unknown[];
  };
  assert.deepEqual(page.events, all.events.slice(1, 3));
});

for (const query of [
  "?limit=1001",
  "?limit=0",
  "?limit=1.5",
  "?afterPosition=-1",
  "?afterPosition=",
]) {
  test(`the replay API with ${query} gives 400`, async () => {
    assert.equal((await fetch(`${base}/internal/events${query}`)).status, 400);
  });
}
