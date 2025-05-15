import { describe, it, expect } from "bun:test";
import { syncDB } from "../packages/kstate/sync-db";
describe("syncDB", () => {
  it("should be a function", () => {
    expect(typeof syncDB).toBe("function");
  });
});
