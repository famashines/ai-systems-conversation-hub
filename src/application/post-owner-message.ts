import type {
  Actor,
  MessageIntent,
  PostOwnerMessageCommandPayloadV1,
} from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import {
  conversationStreamId,
  decide,
  evolve,
  initialState,
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
  const state = (await store.readStream(streamId)).reduce(evolve, initialState);
  const messageId = newId();
  const events = decide(state, {
    type: "PostOwnerMessage",
    messageId,
    ...payload,
    tasks: await threadTasks(conversationId),
  });
  const { intent, taskId } = (events.at(-1) as OwnerMessagePosted).payload;
  const answer = { conversationId, messageId, intent, ...(taskId === undefined ? {} : { taskId }) };
  const result = await store.append({
    streamId,
    ...(state.started ? {} : { expectedVersion: 0 }),
    events: events.map((event) => ({
      ...event,
      correlationId: messageId,
      causationId: commandId,
      actor,
    })),
    command: { commandId, answer },
  });
  return { answer: result.answer ?? answer, replayed: result.replayed };
}
