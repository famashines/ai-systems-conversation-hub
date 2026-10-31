import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createService } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { createMemoryEventStore, fakeClock, fakeIds } from "@famashines/ai-systems-kit/testing";
import { routes } from "../../src/adapters/http.ts";
import type { ThreadTask } from "../../src/domain/conversation.ts";

const clock = fakeClock();
const store = createMemoryEventStore({ now: clock.now, newId: fakeIds(1) });
const messageIds = fakeIds(2);
const tasks = new Map<string, ThreadTask[]>();
const app = createService({
  logger: createLogger({ service: "conversation-hub", release: "test", level: "silent" }),
  routes: routes({
    store,
    newId: messageIds,
    threadTasks: (conversationId) => Promise.resolve(tasks.get(conversationId) ?? []),
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

const send = (body: unknown) =>
  fetch(`${base}/internal/commands/post-owner-message`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
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
