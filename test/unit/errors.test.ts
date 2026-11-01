import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { ConflictError, createService, UnavailableError } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { MongoNetworkError, MongoServerSelectionError } from "mongodb";
import { routes, toHttpError } from "../../src/adapters/http.ts";
import { TaskNotInConversationError } from "../../src/domain/conversation.ts";

const down = (): Promise<never> =>
  Promise.reject(new MongoServerSelectionError("no primary at mongo-0:27017", {} as never));

const app = createService({
  logger: createLogger({ service: "conversation-hub", release: "test", level: "silent" }),
  routes: routes({
    store: { append: down, readStream: down, readAll: down },
    newId: () => "00000000-0000-4000-8000-000000000001",
    threadTasks: down,
    verify: () => Promise.resolve({ sub: "owner-1", roles: ["owner"], exp: 0 }),
  }),
  drainDelayMs: 0,
});
let base: string;

before(async () => {
  const { port } = await app.listen(0, "127.0.0.1");
  base = `http://127.0.0.1:${String(port)}`;
});

after(async () => {
  await app.close();
});

const expectUnavailable = async (response: Response): Promise<void> => {
  assert.equal(response.status, 503);
  const text = await response.text();
  assert.equal((JSON.parse(text) as { code: string }).code, "unavailable");
  assert.ok(!text.includes("mongo-0"));
};

test("PostOwnerMessage gives 503 with no detail when MongoDB is down", async () => {
  await expectUnavailable(
    await fetch(`${base}/internal/commands/post-owner-message`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer owner" },
      body: JSON.stringify({
        commandId: "00000000-0000-4000-8000-000000000002",
        type: "conversation.PostOwnerMessage",
        actor: { kind: "owner", id: "owner-1" },
        payload: { conversationId: "00000000-0000-4000-8000-000000000003", text: "Hi" },
      }),
    }),
  );
});

test("the replay API gives 503 with no detail when MongoDB is down", async () => {
  await expectUnavailable(await fetch(`${base}/internal/events`));
});

test("toHttpError maps the domain and database errors", () => {
  assert.ok(toHttpError(new TaskNotInConversationError()) instanceof ConflictError);
  assert.ok(toHttpError(new MongoNetworkError("connection closed")) instanceof UnavailableError);
  const other = new Error("bug");
  assert.equal(toHttpError(other), other);
});
