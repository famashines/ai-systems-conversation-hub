import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { parseEvent, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { exportJWK, generateKeyPair, SignJWT, type CryptoKey } from "jose";
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
const issuer = "https://auth.example.test/realms/ai-systems";
let jwks: Server;
let privateKey: CryptoKey;
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

const token = (roles = ["owner"]) =>
  new SignJWT({ typ: "Bearer", azp: "ai-systems-app", realm_access: { roles } })
    .setProtectedHeader({ alg: "RS256", kid: "k1" })
    .setIssuer(issuer)
    .setAudience("ai-systems-app")
    .setSubject("owner-1")
    .setIssuedAt()
    .setExpirationTime("5m")
    .sign(privateKey);

before(async () => {
  [mongoContainer, natsContainer] = await Promise.all([
    startMongo(),
    startNats({ config: natsConfig }),
  ]);
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...(await exportJWK(pair.publicKey)), kid: "k1", alg: "RS256", use: "sig" };
  jwks = createServer((_request, response) => {
    response
      .writeHead(200, { "content-type": "application/json" })
      .end(JSON.stringify({ keys: [jwk] }));
  });
  await new Promise<void>((resolve) => jwks.listen(0, "127.0.0.1", resolve));
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
    OIDC_ISSUER: issuer,
    OIDC_JWKS_URL: `http://127.0.0.1:${String((jwks.address() as AddressInfo).port)}/certs`,
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
  await new Promise((resolve) => jwks.close(resolve));
  await Promise.all([mongoContainer.stop(), natsContainer.stop()]);
});

const events = () =>
  mongo.db.collection<{
    _id: string;
    type: string;
    publishedAt: Date | null;
    payload: Record<string, unknown>;
  }>("events");

interface Message {
  text?: string;
  taskId?: string;
  newTask?: boolean;
  mode?: string;
}

interface Answer {
  conversationId: string;
  messageId: string;
  intent: string;
  taskId?: string;
}

const send = (conversationId: string, message: Message, authorization?: string) =>
  fetch(`${base}/internal/commands/post-owner-message`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(authorization === undefined ? {} : { authorization }),
    },
    body: JSON.stringify({
      commandId: randomUUID(),
      type: "conversation.PostOwnerMessage",
      actor: { kind: "owner", id: "owner-1" },
      payload: { conversationId, text: "Write a poem", ...message },
    }),
  });

const post = async (conversationId: string, message: Message = {}): Promise<Answer> => {
  const response = await send(conversationId, message, `Bearer ${await token()}`);
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

test("a post needs an owner token signed by the JWKS key; the replay API needs none", async () => {
  const conversationId = randomUUID();
  assert.equal((await send(conversationId, {})).status, 401);
  assert.equal((await send(conversationId, {}, "Bearer abc.def.ghi")).status, 401);
  assert.equal((await send(conversationId, {}, `Bearer ${await token([])}`)).status, 403);
  for (const path of [
    "//internal/commands/post-owner-message",
    "/internal//commands/post-owner-message",
  ]) {
    const response = await fetch(`${base}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
    assert.equal(response.status, 401, path);
  }
  assert.equal(await events().countDocuments({ "payload.conversationId": conversationId }), 0);
  assert.equal((await fetch(`${base}/internal/events`)).status, 200);
});

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
  assert.equal((await post(conversationId)).intent, "new_task");
  assert.equal((await post(conversationId, { taskId })).intent, "steer");
  const runId = randomUUID();
  await publish(orchestrationEvent("orchestration.RunStarted", { taskId, runId }));
  await drained();
  const steer = await post(conversationId, { text: "Shorter" });
  assert.deepEqual(steer, { conversationId, messageId: steer.messageId, intent: "steer", taskId });
  assert.equal((await post(conversationId, { newTask: true })).intent, "new_task");
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

const replyOf = async (event: { eventId: string }) => {
  const found = await replies(event.eventId);
  assert.equal(found.length, 1, event.eventId);
  return found[0]?.payload;
};

const statusOf = async (taskId: string) =>
  (await mongo.db.collection<{ _id: string; status: string }>("tasks").findOne({ _id: taskId }))
    ?.status;

test("questions, an answer, a plan, a steer and a cancel go through the thread", async () => {
  const conversationId = randomUUID();
  const first = await post(conversationId, { mode: "approval" });
  const ownerEvent = await events().findOne({
    type: "conversation.OwnerMessagePosted",
    "payload.messageId": first.messageId,
  });
  assert.equal(ownerEvent?.payload.mode, "approval");
  const taskId = await submitted(conversationId, first.messageId);
  const asked = orchestrationEvent("orchestration.ClarificationRequested", {
    taskId,
    questions: ["Who reads it?", "How long?"],
  });
  await publish(asked);
  await drained();
  assert.equal(await statusOf(taskId), "clarifying");
  const askedReply = await replyOf(asked);
  assert.deepEqual(askedReply, {
    conversationId,
    messageId: askedReply?.messageId,
    text: "I need answers to these questions before I make a plan:\n\n1. Who reads it?\n2. How long?",
    taskId,
    sourceEventId: asked.eventId,
  });
  const answer = await post(conversationId, { text: "Kids, four lines" });
  assert.deepEqual(answer, {
    conversationId,
    messageId: answer.messageId,
    intent: "answer",
    taskId,
  });
  await publish(
    orchestrationEvent("orchestration.ClarificationAnswered", {
      taskId,
      conversationId,
      messageId: answer.messageId,
      text: "Kids, four lines",
    }),
  );
  await drained();
  assert.equal(await statusOf(taskId), "planning");
  assert.equal((await post(conversationId)).intent, "new_task");
  const proposed = orchestrationEvent("orchestration.PlanProposed", {
    taskId,
    planVersion: 1,
    subtasks: [
      {
        subtaskId: randomUUID(),
        title: "Write the poem",
        instructions: "Four lines for kids",
        agentId: randomUUID(),
        dependsOn: [],
        needsApproval: false,
      },
    ],
    awaitsApproval: true,
  });
  await publish(proposed);
  await drained();
  assert.equal(await statusOf(taskId), "awaiting_approval");
  assert.equal(
    (await replyOf(proposed))?.text,
    "Plan version 1, 1 subtask:\n\n1. Write the poem\n\nThe plan waits for your approval.",
  );
  const runId = randomUUID();
  await publish(orchestrationEvent("orchestration.PlanApproved", { taskId, planVersion: 1 }));
  await publish(orchestrationEvent("orchestration.RunStarted", { taskId, runId }));
  await drained();
  const steer = await post(conversationId, { text: "Make it rhyme" });
  assert.deepEqual(steer, { conversationId, messageId: steer.messageId, intent: "steer", taskId });
  const cancelled = orchestrationEvent("orchestration.RunCancelled", { taskId, runId });
  await publish(cancelled);
  await drained();
  assert.equal(await statusOf(taskId), "ended");
  const cancelReply = await replyOf(cancelled);
  assert.equal(cancelReply?.text, "The run is cancelled.");
  assert.equal(cancelReply.runId, runId);
  assert.equal((await post(conversationId, { taskId })).intent, "new_task");
  const refused = orchestrationEvent("orchestration.MessageRefused", {
    taskId,
    conversationId,
    messageId: steer.messageId,
    intent: "steer",
    reason: "The task has ended. Send a new task.",
  });
  await publish(refused);
  await drained();
  assert.equal((await replyOf(refused))?.text, "The task has ended. Send a new task.");
  assert.equal(await statusOf(taskId), "ended");
});

test("TaskCancelled ends a task with a reply; RunBlocked subtask_failed gives its reason", async () => {
  const conversationId = randomUUID();
  const first = await post(conversationId);
  const taskId = await submitted(conversationId, first.messageId);
  const cancelled = orchestrationEvent("orchestration.TaskCancelled", {
    taskId,
    reason: "Not needed",
  });
  await publish(cancelled);
  const second = await post(conversationId, { newTask: true });
  const otherTask = await submitted(conversationId, second.messageId);
  const runId = randomUUID();
  await publish(orchestrationEvent("orchestration.RunStarted", { taskId: otherTask, runId }));
  const blocked = orchestrationEvent("orchestration.RunBlocked", {
    taskId: otherTask,
    runId,
    code: "subtask_failed",
    reason: "The subtask Write failed 3 times",
  });
  await publish(blocked);
  await drained();
  assert.equal(await statusOf(taskId), "ended");
  assert.equal((await replyOf(cancelled))?.text, "The task is cancelled. Not needed");
  assert.equal(await statusOf(otherTask), "blocked");
  assert.equal((await replyOf(blocked))?.text, "The subtask Write failed 3 times");
  assert.equal(await events().countDocuments({ "payload.sourceEventId": blocked.eventId }), 1);
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
  await publish(orchestrationEvent("orchestration.RunStarted", { taskId, runId: randomUUID() }));
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
