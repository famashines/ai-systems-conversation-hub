import assert from "node:assert/strict";
import { test } from "node:test";
import { ReplyText, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import {
  conversationStreamId,
  decide,
  noReasonText,
  replyText,
  replyTo,
  statusAfter,
  TaskNotInConversationError,
  type ConversationEvent,
  type PostOwnerMessage,
  type ThreadTask,
} from "../../src/domain/conversation.ts";

const conversationId = "00000000-0000-4000-8000-000000000001";
const messageId = "00000000-0000-4000-8000-000000000002";
const older = "00000000-0000-4000-8000-0000000000a1";
const newer = "00000000-0000-4000-8000-0000000000a2";
const other = "00000000-0000-4000-8000-0000000000a3";

const task = (taskId: string, status: ThreadTask["status"], submittedAt: string): ThreadTask => ({
  taskId,
  status,
  submittedAt,
});

const started: ConversationEvent = {
  type: "conversation.ConversationStarted",
  schemaVersion: 1,
  payload: { conversationId },
};

const post = (
  tasks: ThreadTask[],
  message: { taskId?: string; newTask?: boolean } = {},
): PostOwnerMessage => ({
  type: "PostOwnerMessage",
  conversationId,
  messageId,
  text: "Make it shorter",
  newTask: false,
  tasks,
  ...message,
});

const given = (tasks: ThreadTask[]) => ({
  when:
    (message: { taskId?: string; newTask?: boolean } = {}) =>
    () =>
      decide(post(tasks, message)),
});

const posted = (intent: Record<string, string>): ConversationEvent =>
  ({
    type: "conversation.OwnerMessagePosted",
    schemaVersion: 1,
    payload: { conversationId, messageId, text: "Make it shorter", ...intent },
  }) as ConversationEvent;

test("given no tasks, when PostOwnerMessage, then ConversationStarted and OwnerMessagePosted new_task", () => {
  assert.deepEqual(given([]).when()(), [started, posted({ intent: "new_task" })]);
});

test("given a running task, when PostOwnerMessage with newTask, then new_task with no taskId", () => {
  const tasks = [task(older, "running", "2026-10-31T08:00:00.000Z")];
  const events = given(tasks).when({ newTask: true })();
  assert.deepEqual(events, [started, posted({ intent: "new_task" })]);
  assert.equal(Object.hasOwn(events[1].payload, "taskId"), false);
});

test("given two running tasks, when PostOwnerMessage with no taskId, then steer the latest", () => {
  const tasks = [
    task(newer, "running", "2026-10-31T09:00:00.000Z"),
    task(older, "running", "2026-10-31T08:00:00.000Z"),
    task(other, "blocked", "2026-10-31T10:00:00.000Z"),
  ];
  assert.deepEqual(given(tasks).when()(), [started, posted({ intent: "steer", taskId: newer })]);
});

test("given two running tasks at the same time, when PostOwnerMessage, then steer the higher id", () => {
  const at = "2026-10-31T08:00:00.000Z";
  const tasks = [task(newer, "running", at), task(older, "running", at)];
  assert.deepEqual(given(tasks).when()(), [started, posted({ intent: "steer", taskId: newer })]);
});

test("given only blocked and ended tasks, when PostOwnerMessage with no taskId, then new_task", () => {
  const tasks = [
    task(older, "blocked", "2026-10-31T08:00:00.000Z"),
    task(newer, "ended", "2026-10-31T09:00:00.000Z"),
  ];
  assert.deepEqual(given(tasks).when()(), [started, posted({ intent: "new_task" })]);
});

for (const status of ["running", "blocked"] as const) {
  test(`given a ${status} task, when PostOwnerMessage with its taskId, then steer that task`, () => {
    const tasks = [
      task(older, status, "2026-10-31T08:00:00.000Z"),
      task(newer, "running", "2026-10-31T09:00:00.000Z"),
    ];
    assert.deepEqual(given(tasks).when({ taskId: older })(), [
      started,
      posted({ intent: "steer", taskId: older }),
    ]);
  });
}

test("given an ended task, when PostOwnerMessage with its taskId, then new_task", () => {
  const tasks = [
    task(older, "ended", "2026-10-31T08:00:00.000Z"),
    task(newer, "running", "2026-10-31T09:00:00.000Z"),
  ];
  assert.deepEqual(given(tasks).when({ taskId: older })(), [
    started,
    posted({ intent: "new_task" }),
  ]);
});

test("given no task with the id, when PostOwnerMessage with a taskId, then TaskNotInConversationError", () => {
  const tasks = [task(older, "running", "2026-10-31T08:00:00.000Z")];
  assert.throws(given(tasks).when({ taskId: other }), TaskNotInConversationError);
  assert.throws(given([]).when({ taskId: other }), TaskNotInConversationError);
});

test("no rule gives the intent answer: contracts v0.3.0 has no event for a waiting task", () => {
  const statuses = ["running", "blocked", "ended"] as const;
  for (const status of statuses) {
    for (const message of [{}, { taskId: older }, { newTask: true }]) {
      const [, event] = given([task(older, status, "2026-10-31T08:00:00.000Z")]).when(message)();
      assert.notEqual(event.payload.intent, "answer");
    }
  }
});

test("the stream id is conversation-<conversationId>", () => {
  assert.equal(conversationStreamId(conversationId), `conversation-${conversationId}`);
});

const envelope = (type: string, payload: Record<string, unknown>) =>
  ({
    eventId: "00000000-0000-4000-8000-0000000000e1",
    type,
    schemaVersion: 1,
    streamId: `task-${older}`,
    version: 2,
    occurredAt: "2026-10-31T08:00:00.000Z",
    correlationId: older,
    causationId: older,
    actor: { kind: "system", id: "orchestration" },
    payload,
  }) as RegisteredEvent;

const runId = "00000000-0000-4000-8000-0000000000b1";

test("each orchestration event gives its reply text and task status", () => {
  const cases: [RegisteredEvent, ReturnType<typeof replyTo>, ReturnType<typeof statusAfter>][] = [
    [
      envelope("orchestration.TaskSubmitted", { taskId: older, text: "Write", mode: "auto" }),
      undefined,
      undefined,
    ],
    [envelope("orchestration.RunStarted", { taskId: older, runId }), undefined, "running"],
    [
      envelope("orchestration.RunCompleted", {
        taskId: older,
        runId,
        summary: "Here is the poem",
        costMicroUsd: 1200,
      }),
      { taskId: older, runId, text: "Here is the poem" },
      "ended",
    ],
    [
      envelope("orchestration.RunFailed", {
        taskId: older,
        runId,
        code: "empty_output",
        reason: "The model gave no text",
      }),
      { taskId: older, runId, text: "The model gave no text" },
      "ended",
    ],
    [
      envelope("orchestration.RunBlocked", {
        taskId: older,
        runId,
        code: "budget_refused",
        reason: "Monthly cap reached",
      }),
      { taskId: older, runId, text: "Monthly cap reached" },
      "blocked",
    ],
    [
      envelope("orchestration.MessageRefused", {
        taskId: older,
        conversationId,
        messageId,
        intent: "steer",
        reason: "The task has ended",
      }),
      { taskId: older, conversationId, text: "The task has ended" },
      undefined,
    ],
  ];
  for (const [event, reply, status] of cases) {
    assert.deepEqual(replyTo(event), reply, event.type);
    assert.equal(statusAfter(event), status, event.type);
  }
});

test("a conversation event never gives a reply or a status, so a reply cannot make a reply", () => {
  for (const event of [
    envelope("conversation.ConversationStarted", { conversationId }),
    envelope("conversation.OwnerMessagePosted", {
      conversationId,
      messageId,
      text: "Hi",
      intent: "new_task",
    }),
    envelope("conversation.AgentReplyPosted", {
      conversationId,
      messageId,
      text: "Hi",
      taskId: older,
      sourceEventId: runId,
    }),
  ]) {
    assert.equal(replyTo(event), undefined);
    assert.equal(statusAfter(event), undefined);
  }
});

test("a reason that is not a valid reply text gives a valid reply text", () => {
  for (const [reason, text] of [
    [" ", noReasonText],
    ["\n\t", noReasonText],
    ["\0", noReasonText],
    [" \0 \n", noReasonText],
    ["  The cap\0 is reached \n", "  The cap is reached \n"],
    ["Done", "Done"],
  ] as const) {
    assert.equal(replyText(reason), text, JSON.stringify(reason));
  }
  const reply = replyTo(
    envelope("orchestration.RunFailed", {
      taskId: older,
      runId,
      code: "internal",
      reason: "\0 ",
    }),
  );
  assert.equal(reply?.text, noReasonText);
  assert.ok(ReplyText.safeParse(reply.text).success);
  assert.equal(noReasonText, "No reason was given.");
});
