import type { RegisteredEvent } from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import type { EventHandler } from "@famashines/ai-systems-kit/messaging";
import type { Migration } from "@famashines/ai-systems-kit/mongo";
import type { Db } from "mongodb";
import {
  conversationStreamId,
  replyTo,
  statusAfter,
  type TaskStatus,
  type ThreadTask,
} from "../domain/conversation.ts";

export interface TaskDoc {
  _id: string;
  conversationId: string;
  submittedAt: string;
  status: TaskStatus;
}

export const tasksMigration: Migration = {
  name: "001-tasks",
  up: async (db) => {
    await db.collection("tasks").createIndex({ conversationId: 1 }, undefined, undefined);
  },
};

const tasksOf = (db: Db) => db.collection<TaskDoc>("tasks");

export const threadTasks =
  (db: Db) =>
  async (conversationId: string): Promise<ThreadTask[]> =>
    (await tasksOf(db).find({ conversationId }).toArray()).map(({ _id, status, submittedAt }) => ({
      taskId: _id,
      status,
      submittedAt,
    }));

const actor = { kind: "system", id: "conversation-hub" } as const;

export function orchestrationHandler(
  db: Db,
  store: EventStore,
  newId: () => string,
): EventHandler<RegisteredEvent> {
  const tasks = tasksOf(db);
  return async (event, { markProcessed }) => {
    if (event.type === "orchestration.TaskSubmitted") {
      const { taskId, conversationId } = event.payload;
      if (conversationId === undefined) return;
      await tasks.updateOne(
        { _id: taskId },
        { $setOnInsert: { conversationId, submittedAt: event.occurredAt, status: "running" } },
        { upsert: true },
      );
      return;
    }
    const status = statusAfter(event);
    const reply = replyTo(event);
    if (!reply) {
      if (status && "taskId" in event.payload) {
        await tasks.updateOne({ _id: event.payload.taskId }, { $set: { status } });
      }
      return;
    }
    const { taskId, text, runId } = reply;
    const conversationId =
      reply.conversationId ?? (await tasks.findOne({ _id: taskId }))?.conversationId;
    if (conversationId === undefined) return;
    await store.append({
      streamId: conversationStreamId(conversationId),
      events: [
        {
          type: "conversation.AgentReplyPosted",
          schemaVersion: 1,
          correlationId: event.correlationId,
          causationId: event.eventId,
          actor,
          payload: {
            conversationId,
            messageId: newId(),
            text,
            taskId,
            ...(runId === undefined ? {} : { runId }),
            sourceEventId: event.eventId,
          },
        },
      ],
      also: async (session) => {
        if (status) await tasks.updateOne({ _id: taskId }, { $set: { status } }, { session });
        await markProcessed(session);
      },
    });
  };
}
