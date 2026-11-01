import assert from "node:assert/strict";
import { test } from "node:test";
import { ReplyText, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import {
  approvalNote,
  conversationStreamId,
  decide,
  noReasonText,
  questionsIntro,
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

type Message = { taskId?: string; newTask?: boolean; mode?: "auto" | "approval" };

const post = (tasks: ThreadTask[], message: Message = {}): PostOwnerMessage => ({
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
    (message: Message = {}) =>
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

for (const status of ["submitted", "planning", "awaiting_approval"] as const) {
  test(`given a ${status} task, when PostOwnerMessage with no taskId, then new_task`, () => {
    const tasks = [task(older, status, "2026-10-31T08:00:00.000Z")];
    assert.deepEqual(given(tasks).when()(), [started, posted({ intent: "new_task" })]);
  });

  test(`given a ${status} task, when PostOwnerMessage with its taskId, then steer, which orchestration refuses before a run`, () => {
    const tasks = [task(older, status, "2026-10-31T08:00:00.000Z")];
    assert.deepEqual(given(tasks).when({ taskId: older })(), [
      started,
      posted({ intent: "steer", taskId: older }),
    ]);
  });
}

test("given a clarifying task, when PostOwnerMessage with its taskId, then answer that task", () => {
  const tasks = [
    task(older, "clarifying", "2026-10-31T08:00:00.000Z"),
    task(newer, "clarifying", "2026-10-31T09:00:00.000Z"),
  ];
  assert.deepEqual(given(tasks).when({ taskId: older })(), [
    started,
    posted({ intent: "answer", taskId: older }),
  ]);
});

test("given a clarifying and a later running task, when PostOwnerMessage with no taskId, then answer the clarifying task", () => {
  const tasks = [
    task(older, "clarifying", "2026-10-31T08:00:00.000Z"),
    task(newer, "running", "2026-10-31T09:00:00.000Z"),
  ];
  assert.deepEqual(given(tasks).when()(), [started, posted({ intent: "answer", taskId: older })]);
});

test("given two clarifying tasks, when PostOwnerMessage with no taskId, then answer the latest", () => {
  const tasks = [
    task(newer, "clarifying", "2026-10-31T09:00:00.000Z"),
    task(older, "clarifying", "2026-10-31T08:00:00.000Z"),
  ];
  assert.deepEqual(given(tasks).when()(), [started, posted({ intent: "answer", taskId: newer })]);
});

test("given a clarifying task, when PostOwnerMessage with the taskId of a running task, then steer the running task", () => {
  const tasks = [
    task(older, "running", "2026-10-31T08:00:00.000Z"),
    task(newer, "clarifying", "2026-10-31T09:00:00.000Z"),
  ];
  assert.deepEqual(given(tasks).when({ taskId: older })(), [
    started,
    posted({ intent: "steer", taskId: older }),
  ]);
});

test("the mode goes into OwnerMessagePosted only for new_task", () => {
  assert.deepEqual(given([]).when({ mode: "auto" })(), [
    started,
    posted({ intent: "new_task", mode: "auto" }),
  ]);
  const running = [task(older, "running", "2026-10-31T08:00:00.000Z")];
  assert.deepEqual(given(running).when({ newTask: true, mode: "approval" })(), [
    started,
    posted({ intent: "new_task", mode: "approval" }),
  ]);
  for (const status of ["running", "clarifying"] as const) {
    const [, event] = given([task(older, status, "2026-10-31T08:00:00.000Z")]).when({
      mode: "auto",
    })();
    assert.notEqual(event.payload.intent, "new_task");
    assert.equal(Object.hasOwn(event.payload, "mode"), false, status);
  }
  const [, plain] = given([]).when()();
  assert.equal(Object.hasOwn(plain.payload, "mode"), false);
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

const subtask = (subtaskId: string, title: string) => ({
  subtaskId,
  title,
  instructions: "Do it",
  agentId: other,
  dependsOn: [],
  needsApproval: false,
});

const plan = [
  subtask("00000000-0000-4000-8000-0000000000c1", "Find sources"),
  subtask("00000000-0000-4000-8000-0000000000c2", "Write the draft"),
];

const planText = "Plan version 2, 2 subtasks:\n\n1. Find sources\n2. Write the draft";

test("each orchestration event gives its reply text and task status", () => {
  const cases: [RegisteredEvent, ReturnType<typeof replyTo>, ReturnType<typeof statusAfter>][] = [
    [
      envelope("orchestration.TaskSubmitted", { taskId: older, text: "Write", mode: "auto" }),
      undefined,
      undefined,
    ],
    [
      envelope("orchestration.ClarificationRequested", {
        taskId: older,
        questions: ["Who reads it?", "How long?"],
      }),
      { taskId: older, text: `${questionsIntro}\n\n1. Who reads it?\n2. How long?` },
      "clarifying",
    ],
    [
      envelope("orchestration.ClarificationAnswered", {
        taskId: older,
        conversationId,
        messageId,
        text: "Kids, short",
      }),
      undefined,
      "planning",
    ],
    [
      envelope("orchestration.PlanProposed", {
        taskId: older,
        planVersion: 2,
        subtasks: plan,
        awaitsApproval: true,
      }),
      { taskId: older, text: `${planText}\n\n${approvalNote}` },
      "awaiting_approval",
    ],
    [
      envelope("orchestration.PlanProposed", {
        taskId: older,
        planVersion: 2,
        subtasks: plan,
        awaitsApproval: false,
      }),
      { taskId: older, text: planText },
      "planning",
    ],
    [
      envelope("orchestration.PlanProposed", {
        taskId: older,
        planVersion: 1,
        subtasks: plan.slice(0, 1),
        awaitsApproval: false,
      }),
      { taskId: older, text: "Plan version 1, 1 subtask:\n\n1. Find sources" },
      "planning",
    ],
    [
      envelope("orchestration.PlanEdited", { taskId: older, planVersion: 3, subtasks: plan }),
      undefined,
      "awaiting_approval",
    ],
    [
      envelope("orchestration.PlanApproved", { taskId: older, planVersion: 3 }),
      undefined,
      undefined,
    ],
    [
      envelope("orchestration.PlanRejected", { taskId: older, planVersion: 3, reason: "Too much" }),
      undefined,
      "ended",
    ],
    [envelope("orchestration.RunStarted", { taskId: older, runId }), undefined, "running"],
    [envelope("orchestration.RunPaused", { taskId: older, runId }), undefined, undefined],
    [envelope("orchestration.RunResumed", { taskId: older, runId }), undefined, "running"],
    [
      envelope("orchestration.SteeringMessageAdded", {
        taskId: older,
        runId,
        conversationId,
        messageId,
        text: "Shorter",
      }),
      undefined,
      undefined,
    ],
    [
      envelope("orchestration.TaskModeChanged", { taskId: older, mode: "auto" }),
      undefined,
      undefined,
    ],
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
    ...(["budget_refused", "runaway_stopped", "forbidden", "subtask_failed"] as const).map(
      (code): [RegisteredEvent, ReturnType<typeof replyTo>, ReturnType<typeof statusAfter>] => [
        envelope("orchestration.RunBlocked", {
          taskId: older,
          runId,
          code,
          reason: `Blocked: ${code}`,
        }),
        { taskId: older, runId, text: `Blocked: ${code}` },
        "blocked",
      ],
    ),
    [
      envelope("orchestration.RunCancelled", { taskId: older, runId }),
      { taskId: older, runId, text: "The run is cancelled." },
      "ended",
    ],
    [
      envelope("orchestration.RunCancelled", { taskId: older, runId, reason: "Not needed" }),
      { taskId: older, runId, text: "The run is cancelled. Not needed" },
      "ended",
    ],
    [
      envelope("orchestration.TaskCancelled", { taskId: older }),
      { taskId: older, text: "The task is cancelled." },
      "ended",
    ],
    [
      envelope("orchestration.TaskCancelled", { taskId: older, reason: " \0 " }),
      { taskId: older, text: "The task is cancelled." },
      "ended",
    ],
    [
      envelope("orchestration.TaskCancelled", { taskId: older, reason: "Wrong\0 task" }),
      { taskId: older, text: "The task is cancelled. Wrong task" },
      "ended",
    ],
    ...(["steer", "answer"] as const).map(
      (intent): [RegisteredEvent, ReturnType<typeof replyTo>, ReturnType<typeof statusAfter>] => [
        envelope("orchestration.MessageRefused", {
          taskId: older,
          conversationId,
          messageId,
          intent,
          reason: "The task has ended",
        }),
        { taskId: older, conversationId, text: "The task has ended" },
        undefined,
      ],
    ),
  ];
  for (const [event, reply, status] of cases) {
    assert.deepEqual(replyTo(event), reply, event.type);
    assert.equal(statusAfter(event), status, event.type);
    if (reply) assert.ok(ReplyText.safeParse(reply.text).success, event.type);
  }
});

test("the longest questions, plan and summary give a valid reply text", () => {
  const long = (length: number) => "x".repeat(length);
  const events = [
    envelope("orchestration.ClarificationRequested", {
      taskId: older,
      questions: Array.from({ length: 5 }, () => long(1000)),
    }),
    envelope("orchestration.PlanProposed", {
      taskId: older,
      planVersion: 999999,
      subtasks: Array.from({ length: 20 }, (_, index) =>
        subtask(`00000000-0000-4000-8000-${String(index).padStart(12, "0")}`, long(200)),
      ),
      awaitsApproval: true,
    }),
    envelope("orchestration.RunCompleted", {
      taskId: older,
      runId,
      summary: long(100000),
      costMicroUsd: 0,
    }),
    envelope("orchestration.RunCancelled", { taskId: older, runId, reason: long(1000) }),
  ];
  for (const event of events) {
    assert.ok(ReplyText.safeParse(replyTo(event)?.text).success, event.type);
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
