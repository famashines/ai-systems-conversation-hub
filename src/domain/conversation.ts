import type {
  AgentReplyPostedEventPayloadV1,
  ConversationStartedEventPayloadV1,
  OwnerMessagePostedEventPayloadV1,
  RegisteredEvent,
} from "@famashines/ai-systems-contracts";

export type TaskStatus = "running" | "blocked" | "ended";

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

export type Intent = { intent: "new_task" } | { intent: "steer"; taskId: string };

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
    return task.status === "ended" ? { intent: "new_task" } : { intent: "steer", taskId };
  }
  const running = latest(tasks.filter((task) => task.status === "running"));
  return running ? { intent: "steer", taskId: running.taskId } : { intent: "new_task" };
}

export function decide(command: ConversationCommand): [ConversationStarted, OwnerMessagePosted] {
  const { conversationId, messageId, text } = command;
  return [
    { type: "conversation.ConversationStarted", schemaVersion: 1, payload: { conversationId } },
    {
      type: "conversation.OwnerMessagePosted",
      schemaVersion: 1,
      payload: { conversationId, messageId, text, ...intentOf(command.tasks, command) },
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

export function replyText(text: string): string {
  const clean = text.replaceAll("\0", "");
  return clean.trim() === "" ? noReasonText : clean;
}

export function replyTo(event: RegisteredEvent): Reply | undefined {
  const reply = rawReplyTo(event);
  return reply && { ...reply, text: replyText(reply.text) };
}

function rawReplyTo(event: RegisteredEvent): Reply | undefined {
  switch (event.type) {
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
    case "orchestration.RunStarted":
      return "running";
    case "orchestration.RunBlocked":
      return "blocked";
    case "orchestration.RunCompleted":
    case "orchestration.RunFailed":
      return "ended";
    default:
      return undefined;
  }
}
