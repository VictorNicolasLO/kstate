import { describe, it, expect, vi } from "bun:test";
import { eachBatch } from "../packages/kstate/each-batch";
describe("eachBatch", () => {
  it("should be a function", () => {
    expect(typeof eachBatch).toBe("function");
  });
});
