import { describe, it, expect } from "bun:test";
import { createRedisStore } from "../packages/kstate/stores/redis-store/redis-store";
describe("createRedisStore", () => {
  it("should be a function", () => {
    expect(typeof createRedisStore).toBe("function");
  });
});
