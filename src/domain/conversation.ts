import type {
  AgentReplyPostedEventPayloadV1,
  ConversationStartedEventPayloadV1,
  OwnerMessagePostedEventPayloadV1,
  RegisteredEvent,
  TaskMode,
} from "@famashines/ai-systems-contracts";

export type TaskStatus =
  "submitted" | "clarifying" | "planning" | "awaiting_approval" | "running" | "blocked" | "ended";

export interface ThreadTask {
  taskId: string;
  status: TaskStatus;
  submittedAt: string;
}

export interface PostOwnerMessage {
  type: "PostOwnerMessage";
  conversationId: string;
  messageId: string;
  text: string;
  taskId?: string;
  newTask: boolean;
  mode?: TaskMode;
  tasks: readonly ThreadTask[];
}

export type ConversationCommand = PostOwnerMessage;

export interface ConversationStarted {
  type: "conversation.ConversationStarted";
  schemaVersion: 1;
  payload: ConversationStartedEventPayloadV1;
}

export interface OwnerMessagePosted {
  type: "conversation.OwnerMessagePosted";
  schemaVersion: 1;
  payload: OwnerMessagePostedEventPayloadV1;
}

export interface AgentReplyPosted {
  type: "conversation.AgentReplyPosted";
  schemaVersion: 1;
  payload: AgentReplyPostedEventPayloadV1;
}

export type ConversationEvent = ConversationStarted | OwnerMessagePosted | AgentReplyPosted;

export type Intent = { intent: "new_task" } | { intent: "steer" | "answer"; taskId: string };

export class TaskNotInConversationError extends Error {
  constructor() {
    super("The task is not in this conversation");
    this.name = "TaskNotInConversationError";
  }
}

export const conversationStreamId = (conversationId: string): string =>
  `conversation-${conversationId}`;

const latest = (tasks: readonly ThreadTask[]): ThreadTask | undefined =>
  tasks.reduce<ThreadTask | undefined>(
    (found, task) =>
      found === undefined ||
      task.submittedAt > found.submittedAt ||
      (task.submittedAt === found.submittedAt && task.taskId > found.taskId)
        ? task
        : found,
    undefined,
  );

export function intentOf(
  tasks: readonly ThreadTask[],
  { taskId, newTask }: { taskId?: string; newTask: boolean },
): Intent {
  if (newTask) return { intent: "new_task" };
  if (taskId !== undefined) {
    const task = tasks.find((candidate) => candidate.taskId === taskId);
    if (!task) throw new TaskNotInConversationError();
    if (task.status === "ended") return { intent: "new_task" };
    return { intent: task.status === "clarifying" ? "answer" : "steer", taskId };
  }
  const clarifying = latest(tasks.filter((task) => task.status === "clarifying"));
  if (clarifying) return { intent: "answer", taskId: clarifying.taskId };
  const running = latest(tasks.filter((task) => task.status === "running"));
  return running ? { intent: "steer", taskId: running.taskId } : { intent: "new_task" };
}

export function decide(command: ConversationCommand): [ConversationStarted, OwnerMessagePosted] {
  const { conversationId, messageId, text, mode } = command;
  const intent = intentOf(command.tasks, command);
  return [
    { type: "conversation.ConversationStarted", schemaVersion: 1, payload: { conversationId } },
    {
      type: "conversation.OwnerMessagePosted",
      schemaVersion: 1,
      payload:
        intent.intent === "new_task" && mode !== undefined
          ? { conversationId, messageId, text, ...intent, mode }
          : { conversationId, messageId, text, ...intent },
    },
  ];
}

export interface Reply {
  taskId: string;
  text: string;
  runId?: string;
  conversationId?: string;
}

export const noReasonText = "No reason was given.";

const clean = (text: string): string => text.replaceAll("\0", "");

export function replyText(text: string): string {
  const cleaned = clean(text);
  return cleaned.trim() === "" ? noReasonText : cleaned;
}

export const questionsIntro = "I need answers to these questions before I make a plan:";
export const approvalNote = "The plan waits for your approval.";

const numbered = (lines: readonly string[]): string =>
  lines.map((line, index) => `${String(index + 1)}. ${line}`).join("\n");

const withReason = (text: string, reason: string | undefined): string => {
  const cleaned = clean(reason ?? "");
  return cleaned.trim() === "" ? text : `${text} ${cleaned}`;
};

export function replyTo(event: RegisteredEvent): Reply | undefined {
  const reply = rawReplyTo(event);
  return reply && { ...reply, text: replyText(reply.text) };
}

function rawReplyTo(event: RegisteredEvent): Reply | undefined {
  switch (event.type) {
    case "orchestration.ClarificationRequested":
      return {
        taskId: event.payload.taskId,
        text: `${questionsIntro}\n\n${numbered(event.payload.questions)}`,
      };
    case "orchestration.PlanProposed": {
      const { taskId, planVersion, subtasks, awaitsApproval } = event.payload;
      const count = subtasks.length === 1 ? "1 subtask" : `${String(subtasks.length)} subtasks`;
      const plan = `Plan version ${String(planVersion)}, ${count}:\n\n${numbered(subtasks.map((subtask) => subtask.title))}`;
      return { taskId, text: awaitsApproval ? `${plan}\n\n${approvalNote}` : plan };
    }
    case "orchestration.RunCompleted":
      return {
        taskId: event.payload.taskId,
        runId: event.payload.runId,
        text: event.payload.summary,
      };
    case "orchestration.RunFailed":
    case "orchestration.RunBlocked":
      return {
        taskId: event.payload.taskId,
        runId: event.payload.runId,
        text: event.payload.reason,
      };
    case "orchestration.RunCancelled":
      return {
        taskId: event.payload.taskId,
        runId: event.payload.runId,
        text: withReason("The run is cancelled.", event.payload.reason),
      };
    case "orchestration.TaskCancelled":
      return {
        taskId: event.payload.taskId,
        text: withReason("The task is cancelled.", event.payload.reason),
      };
    case "orchestration.MessageRefused":
      return {
        taskId: event.payload.taskId,
        conversationId: event.payload.conversationId,
        text: event.payload.reason,
      };
    default:
      return undefined;
  }
}

export function statusAfter(event: RegisteredEvent): TaskStatus | undefined {
  switch (event.type) {
    case "orchestration.ClarificationRequested":
      return "clarifying";
    case "orchestration.ClarificationAnswered":
      return "planning";
    case "orchestration.PlanProposed":
      return event.payload.awaitsApproval ? "awaiting_approval" : "planning";
    case "orchestration.PlanEdited":
      return "awaiting_approval";
    case "orchestration.RunStarted":
    case "orchestration.RunResumed":
      return "running";
    case "orchestration.RunBlocked":
      return "blocked";
    case "orchestration.RunCompleted":
    case "orchestration.RunFailed":
    case "orchestration.RunCancelled":
    case "orchestration.TaskCancelled":
    case "orchestration.PlanRejected":
      return "ended";
    default:
      return undefined;
  }
}
