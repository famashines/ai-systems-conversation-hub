import {
  Actor,
  CommandEnvelopeV1,
  PostOwnerMessageCommandPayloadV1,
} from "@famashines/ai-systems-contracts";
import {
  ConflictError,
  UnavailableError,
  validate,
  type Handler,
  type Route,
} from "@famashines/ai-systems-kit/http";
import { isInfrastructure } from "@famashines/ai-systems-kit/messaging";
import { z } from "zod";
import { postOwnerMessage, type Deps } from "../application/post-owner-message.ts";
import { TaskNotInConversationError } from "../domain/conversation.ts";

const PostOwnerMessageBody = CommandEnvelopeV1(PostOwnerMessageCommandPayloadV1).extend({
  type: z.literal("conversation.PostOwnerMessage"),
  expectedVersion: z.never().exactOptional(),
  actor: Actor.extend({ kind: z.literal("owner") }),
});

const count = (fallback: string, schema: z.ZodInt) =>
  z.string().regex(/^\d+$/).default(fallback).transform(Number).pipe(schema);

const EventsQuery = z.object({
  afterPosition: count("0", z.int().min(0)),
  limit: count("100", z.int().min(1).max(1000)),
});

export function toHttpError(error: unknown): unknown {
  if (error instanceof TaskNotInConversationError) return new ConflictError(error.message);
  if (isInfrastructure(error)) return new UnavailableError("The database is not available");
  return error;
}

const guard =
  (handler: Handler): Handler =>
  async (request) => {
    try {
      return await handler(request);
    } catch (error) {
      throw toHttpError(error);
    }
  };

export function routes(deps: Deps): Route[] {
  return [
    {
      method: "POST",
      path: "/internal/commands/post-owner-message",
      handler: guard(async (request) => {
        const { body } = await validate(request, { body: PostOwnerMessageBody });
        const { answer, replayed } = await postOwnerMessage(deps, body);
        return { status: replayed ? 200 : 201, body: answer };
      }),
    },
    {
      method: "GET",
      path: "/internal/events",
      handler: guard(async (request) => {
        const { query } = await validate(request, { query: EventsQuery });
        return { body: { events: await deps.store.readAll(query) } };
      }),
    },
  ];
}
