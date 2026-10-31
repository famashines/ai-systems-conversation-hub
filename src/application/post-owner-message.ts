import type {
  Actor,
  MessageIntent,
  PostOwnerMessageCommandPayloadV1,
} from "@famashines/ai-systems-contracts";
import { ConcurrencyError, type EventStore } from "@famashines/ai-systems-kit/event-store";
import {
  conversationStreamId,
  decide,
  initialState,
  type ConversationEvent,
  type OwnerMessagePosted,
  type ThreadTask,
} from "../domain/conversation.ts";

export interface PostOwnerMessageInput {
  commandId: string;
  actor: Actor;
  payload: PostOwnerMessageCommandPayloadV1;
}

export interface PostOwnerMessageAnswer {
  conversationId: string;
  messageId: string;
  intent: MessageIntent;
  taskId?: string;
}

export interface Deps {
  store: EventStore;
  newId: () => string;
  threadTasks: (conversationId: string) => Promise<ThreadTask[]>;
}

export async function postOwnerMessage(
  { store, newId, threadTasks }: Deps,
  { commandId, actor, payload }: PostOwnerMessageInput,
): Promise<{ answer: PostOwnerMessageAnswer; replayed: boolean }> {
  const { conversationId } = payload;
  const streamId = conversationStreamId(conversationId);
  const messageId = newId();
  const command = {
    type: "PostOwnerMessage" as const,
    messageId,
    ...payload,
    tasks: await threadTasks(conversationId),
  };
  const first = decide(initialState, command);
  const { intent, taskId } = (first.at(-1) as OwnerMessagePosted).payload;
  const answer = { conversationId, messageId, intent, ...(taskId === undefined ? {} : { taskId }) };
  const append = (events: ConversationEvent[], expectedVersion?: number) =>
    store.append({
      streamId,
      ...(expectedVersion === undefined ? {} : { expectedVersion }),
      events: events.map((event) => ({
        ...event,
        correlationId: messageId,
        causationId: commandId,
        actor,
      })),
      command: { commandId, answer },
    });
  let result;
  try {
    result = await append(first, 0);
  } catch (error) {
    if (!(error instanceof ConcurrencyError)) throw error;
    result = await append(decide({ started: true }, command));
  }
  return { answer: result.answer ?? answer, replayed: result.replayed };
}
