import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { parseEvent, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { loadConfig } from "@famashines/ai-systems-kit/config";
import {
  connectNats,
  processedMigration,
  startConsumer,
  type DurableConsumer,
  type Nats,
} from "@famashines/ai-systems-kit/messaging";
import { connectMongo, runMigrations, type Mongo } from "@famashines/ai-systems-kit/mongo";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import {
  createStreams,
  startMongo,
  startNats,
  waitFor,
  type Started,
} from "@famashines/ai-systems-kit/testing";
import { envSchema, start } from "../../src/main.ts";

const conversationHubGrants = [
  "conversation.>",
  "dlq.conversation-hub.>",
  "$JS.API.INFO",
  "$JS.API.STREAM.INFO.CONVERSATION",
  "$JS.API.STREAM.INFO.ORCHESTRATION",
  "$JS.API.CONSUMER.CREATE.ORCHESTRATION.conversation-hub.orchestration.>",
  "$JS.API.CONSUMER.INFO.ORCHESTRATION.conversation-hub",
  "$JS.API.CONSUMER.MSG.NEXT.ORCHESTRATION.conversation-hub",
  "$JS.ACK.ORCHESTRATION.conversation-hub.>",
];

const orchestrationGrants = [
  "orchestration.>",
  "rpc.budget.reserve",
  "rpc.budget.settle",
  "dlq.orchestration.>",
  "$JS.API.INFO",
  "$JS.API.STREAM.INFO.ORCHESTRATION",
  "$JS.API.STREAM.INFO.CONVERSATION",
  "$JS.API.CONSUMER.CREATE.CONVERSATION.orchestration.conversation.OwnerMessagePosted",
  "$JS.API.CONSUMER.INFO.CONVERSATION.orchestration",
  "$JS.API.CONSUMER.MSG.NEXT.CONVERSATION.orchestration",
  "$JS.ACK.CONVERSATION.orchestration.>",
  "$JS.API.STREAM.INFO.LLM",
  "$JS.API.CONSUMER.CREATE.LLM.orchestration.llm.>",
  "$JS.API.CONSUMER.INFO.LLM.orchestration",
  "$JS.API.CONSUMER.MSG.NEXT.LLM.orchestration",
  "$JS.ACK.LLM.orchestration.>",
];

const adminGrants = [
  "$JS.API.>",
  "$JS.ACK.>",
  "orchestration.>",
  "conversation.>",
  "llm.>",
  "budget.>",
  "agents.>",
  "prompts.>",
  "dlq.>",
];

const list = (subjects: string[]): string => subjects.map((s) => `"${s}"`).join(", ");

const user = (name: string, password: string, grants: string[], inbox: string) => `
      {
        user: ${name}, password: ${password}
        permissions {
          publish { allow: [${list(grants)}] }
          subscribe { allow: ["${inbox}.>"] }
        }
      }`;

const natsConfig = `
jetstream { store_dir: /data }
accounts {
  AISYSTEMS {
    jetstream { max_file: 1073741824, max_memory: 0 }
    users = [
      ${user("ai-systems-admin", "admin", adminGrants, "_INBOX_admin")}
      ${user("ai-systems-conversation-hub", "hub", conversationHubGrants, "_INBOX_conversation-hub")}
      ${user("ai-systems-orchestration", "orchestration", orchestrationGrants, "_INBOX_orchestration")}
    ]
  }
}
`;

const logger = createLogger({ service: "test", release: "test", level: "silent" });
let mongoContainer: Started;
let natsContainer: Started;
let mongo: Mongo;
let orchestrationMongo: Mongo;
let admin: Nats;
let orchestration: Nats;
let orchestrationConsumer: DurableConsumer;
let manager: JetStreamManager;
let running: Awaited<ReturnType<typeof start>> | undefined;
let base = "";
const seenByOrchestration: RegisteredEvent[] = [];

before(async () => {
  [mongoContainer, natsContainer] = await Promise.all([
    startMongo(),
    startNats({ config: natsConfig }),
  ]);
  mongo = await connectMongo({ url: mongoContainer.url, database: "conversation_hub" });
  orchestrationMongo = await connectMongo({ url: mongoContainer.url, database: "orchestration" });
  await runMigrations(orchestrationMongo.db, [processedMigration]);
  admin = await connectNats({
    servers: natsContainer.url,
    user: "ai-systems-admin",
    pass: "admin",
    service: "admin",
    logger,
  });
  manager = await jetstreamManager(admin.connection);
  await createStreams(admin.connection, [
    { name: "ORCHESTRATION", subjects: ["orchestration.>"] },
    { name: "CONVERSATION", subjects: ["conversation.>"] },
  ]);
  orchestration = await connectNats({
    servers: natsContainer.url,
    user: "ai-systems-orchestration",
    pass: "orchestration",
    service: "orchestration",
    logger,
  });
  orchestrationConsumer = startConsumer({
    nats: orchestration,
    db: orchestrationMongo.db,
    stream: "CONVERSATION",
    filter: "conversation.OwnerMessagePosted",
    parse: parseEvent,
    handler: (event) => {
      seenByOrchestration.push(event);
      return Promise.resolve();
    },
  });
  const config = loadConfig(envSchema, {
    MONGODB_URI: mongoContainer.url,
    NATS_SERVERS: natsContainer.url,
    NATS_USER: "ai-systems-conversation-hub",
    NATS_PASSWORD: "hub",
    LOG_LEVEL: "silent",
  });
  running = await start({ ...config, PORT: 0 }, { drainDelayMs: 0 });
  base = `http://127.0.0.1:${String(running.address.port)}`;
  await waitFor(async () => (await fetch(`${base}/ready`)).ok);
  await waitFor(orchestrationConsumer.ready);
});

after(async () => {
  if (running) assert.equal(await running.app.close(), true);
  await orchestrationConsumer.close();
  await orchestration.close();
  await admin.close();
  await mongo.close();
  await orchestrationMongo.close();
  await Promise.all([mongoContainer.stop(), natsContainer.stop()]);
});

const events = () =>
  mongo.db.collection<{
    _id: string;
    type: string;
    publishedAt: Date | null;
    payload: Record<string, unknown>;
  }>("events");

interface Answer {
  conversationId: string;
  messageId: string;
  intent: string;
  taskId?: string;
}

const post = async (
  conversationId: string,
  message: { text?: string; taskId?: string; newTask?: boolean } = {},
): Promise<Answer> => {
  const response = await fetch(`${base}/internal/commands/post-owner-message`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      commandId: randomUUID(),
      type: "conversation.PostOwnerMessage",
      actor: { kind: "owner", id: "owner-1" },
      payload: { conversationId, text: "Write a poem", ...message },
    }),
  });
  assert.equal(response.status, 201);
  return (await response.json()) as Answer;
};

const versions = new Map<string, number>();

const orchestrationEvent = (
  type: string,
  payload: { taskId: string } & Record<string, unknown>,
) => {
  const version = (versions.get(payload.taskId) ?? 0) + 1;
  versions.set(payload.taskId, version);
  return {
    eventId: randomUUID(),
    type,
    schemaVersion: 1,
    streamId: `task-${payload.taskId}`,
    version,
    occurredAt: new Date().toISOString(),
    correlationId: payload.taskId,
    causationId: randomUUID(),
    actor: { kind: "system", id: "orchestration" },
    payload,
  };
};

const publish = async (event: { eventId: string; type: string }, msgID = event.eventId) => {
  await jetstream(admin.connection).publish(event.type, JSON.stringify(event), { msgID });
};

const drained = async (): Promise<void> => {
  await waitFor(async () => {
    const { state } = await manager.streams.info("ORCHESTRATION");
    const info = await manager.consumers
      .info("ORCHESTRATION", "conversation-hub")
      .catch(() => null);
    return info?.ack_floor.stream_seq === state.last_seq;
  });
};

const submitted = async (conversationId: string, messageId: string): Promise<string> => {
  const taskId = randomUUID();
  await publish(
    orchestrationEvent("orchestration.TaskSubmitted", {
      taskId,
      text: "Write a poem",
      mode: "auto",
      conversationId,
      messageId,
    }),
  );
  return taskId;
};

const replies = (sourceEventId: string) =>
  events()
    .find({ type: "conversation.AgentReplyPosted", "payload.sourceEventId": sourceEventId })
    .toArray();

const streamMessages = async (eventId: string) => {
  const { state } = await manager.streams.info("CONVERSATION");
  const found = [];
  for (let seq = state.first_seq; seq <= state.last_seq && state.messages > 0; seq += 1) {
    const message = await manager.streams.getMessage("CONVERSATION", { seq });
    if (message?.header.get("Nats-Msg-Id") === eventId) found.push(message);
  }
  return found;
};

const published = (eventId: string) =>
  waitFor(async () => (await events().findOne({ _id: eventId }))?.publishedAt != null);

test("the first message starts the thread and reaches CONVERSATION once", async () => {
  const conversationId = randomUUID();
  const answer = await post(conversationId);
  assert.equal(answer.intent, "new_task");
  const stored = await events()
    .find({ streamId: `conversation-${conversationId}` })
    .toArray();
  assert.deepEqual(
    stored.map((event) => event.type),
    ["conversation.ConversationStarted", "conversation.OwnerMessagePosted"],
  );
  for (const event of stored) {
    await published(event._id);
    const messages = await streamMessages(event._id);
    assert.equal(messages.length, 1);
    assert.equal(messages[0]?.subject, event.type);
  }
});

test("the intent follows the task state that comes from orchestration events", async () => {
  const conversationId = randomUUID();
  const first = await post(conversationId);
  const taskId = await submitted(conversationId, first.messageId);
  await drained();
  const steer = await post(conversationId, { text: "Shorter" });
  assert.deepEqual(steer, { conversationId, messageId: steer.messageId, intent: "steer", taskId });
  assert.equal((await post(conversationId, { newTask: true })).intent, "new_task");
  const runId = randomUUID();
  await publish(orchestrationEvent("orchestration.RunStarted", { taskId, runId }));
  const blocked = orchestrationEvent("orchestration.RunBlocked", {
    taskId,
    runId,
    code: "budget_refused",
    reason: "Monthly cap reached: 19.80 of 20.00 USD",
  });
  await publish(blocked);
  await drained();
  assert.equal((await post(conversationId)).intent, "new_task");
  assert.equal((await post(conversationId, { taskId })).intent, "steer");
  const done = orchestrationEvent("orchestration.RunCompleted", {
    taskId,
    runId,
    summary: "Here is the poem",
    costMicroUsd: 1200,
  });
  await publish(done);
  await drained();
  assert.equal((await post(conversationId, { taskId })).intent, "new_task");
  assert.equal((await post(conversationId)).intent, "new_task");
  const [blockedReply] = await replies(blocked.eventId);
  const [doneReply] = await replies(done.eventId);
  assert.deepEqual(blockedReply?.payload, {
    conversationId,
    messageId: blockedReply?.payload.messageId,
    text: "Monthly cap reached: 19.80 of 20.00 USD",
    taskId,
    runId,
    sourceEventId: blocked.eventId,
  });
  assert.equal(doneReply?.payload.text, "Here is the poem");
});

test("a reply appears once after a redelivery of its source event", async () => {
  const conversationId = randomUUID();
  const first = await post(conversationId);
  const taskId = await submitted(conversationId, first.messageId);
  const failed = orchestrationEvent("orchestration.RunFailed", {
    taskId,
    runId: randomUUID(),
    code: "empty_output",
    reason: "The model gave no text",
  });
  await publish(failed);
  await drained();
  await publish(failed, randomUUID());
  await drained();
  const found = await replies(failed.eventId);
  assert.equal(found.length, 1);
  const [reply] = found;
  await published(reply?._id ?? "");
  assert.equal((await streamMessages(reply?._id ?? "")).length, 1);
  assert.equal(
    await events().countDocuments({
      type: "conversation.AgentReplyPosted",
      "payload.conversationId": conversationId,
    }),
    1,
  );
});

test("MessageRefused gives its reason as a reply and changes no task state", async () => {
  const conversationId = randomUUID();
  const first = await post(conversationId);
  const taskId = await submitted(conversationId, first.messageId);
  const refused = orchestrationEvent("orchestration.MessageRefused", {
    taskId,
    conversationId,
    messageId: first.messageId,
    intent: "steer",
    reason: "The task has ended",
  });
  await publish(refused);
  await drained();
  const [reply] = await replies(refused.eventId);
  assert.deepEqual(reply?.payload, {
    conversationId,
    messageId: reply?.payload.messageId,
    text: "The task has ended",
    taskId,
    sourceEventId: refused.eventId,
  });
  const task = await mongo.db.collection("tasks").findOne({ _id: taskId as never });
  assert.equal(task?.status, "running");
  const next = await post(conversationId);
  assert.deepEqual(next, { conversationId, messageId: next.messageId, intent: "steer", taskId });
});

test("a reason with no text gives a fixed reply text", async () => {
  const conversationId = randomUUID();
  const first = await post(conversationId);
  const taskId = await submitted(conversationId, first.messageId);
  const failed = orchestrationEvent("orchestration.RunFailed", {
    taskId,
    runId: randomUUID(),
    code: "internal",
    reason: " \u0000 ",
  });
  await publish(failed);
  await drained();
  const [reply] = await replies(failed.eventId);
  assert.equal(reply?.payload.text, "No reason was given.");
  const task = await mongo.db.collection("tasks").findOne({ _id: taskId as never });
  assert.equal(task?.status, "ended");
});

test("a task that is not from the chat gives no reply", async () => {
  const taskId = randomUUID();
  await publish(
    orchestrationEvent("orchestration.TaskSubmitted", { taskId, text: "API task", mode: "auto" }),
  );
  const done = orchestrationEvent("orchestration.RunCompleted", {
    taskId,
    runId: randomUUID(),
    summary: "Done",
    costMicroUsd: 0,
  });
  await publish(done);
  await drained();
  assert.equal((await replies(done.eventId)).length, 0);
  assert.equal(await mongo.db.collection("tasks").countDocuments({ _id: taskId as never }), 0);
});

test("a replay of ORCHESTRATION keeps ended tasks ended and makes no new reply", async () => {
  const conversationId = randomUUID();
  const first = await post(conversationId);
  const taskId = await submitted(conversationId, first.messageId);
  const runId = randomUUID();
  await publish(orchestrationEvent("orchestration.RunStarted", { taskId, runId }));
  await publish(
    orchestrationEvent("orchestration.RunCompleted", {
      taskId,
      runId,
      summary: "Done",
      costMicroUsd: 0,
    }),
  );
  await drained();
  const tasks = mongo.db.collection<{ _id: string; status: string }>("tasks");
  const before = await tasks.find().sort({ _id: 1 }).toArray();
  const count = await events().countDocuments();
  assert.equal((await tasks.findOne({ _id: taskId }))?.status, "ended");
  await manager.consumers.delete("ORCHESTRATION", "conversation-hub");
  await waitFor(
    async () =>
      (await manager.consumers.info("ORCHESTRATION", "conversation-hub").catch(() => null)) !==
      null,
  );
  await drained();
  assert.deepEqual(await tasks.find().sort({ _id: 1 }).toArray(), before);
  assert.equal(await events().countDocuments(), count);
  assert.equal((await post(conversationId)).intent, "new_task");
});

test("a reply never reaches orchestration: its consumer gets only owner messages", async () => {
  const owner = await events().find({ type: "conversation.OwnerMessagePosted" }).toArray();
  const agent = await events().find({ type: "conversation.AgentReplyPosted" }).toArray();
  assert.ok(agent.length >= 3);
  for (const event of [...owner, ...agent]) await published(event._id);
  await waitFor(() => Promise.resolve(seenByOrchestration.length === owner.length));
  await waitFor(async () => {
    const info = await manager.consumers.info("CONVERSATION", "orchestration");
    return info.num_pending === 0 && info.num_ack_pending === 0;
  });
  assert.deepEqual(
    [...new Set(seenByOrchestration.map((event) => event.type))],
    ["conversation.OwnerMessagePosted"],
  );
  assert.deepEqual(
    seenByOrchestration.map((event) => event.eventId).sort(),
    owner.map((event) => event._id).sort(),
  );
});
