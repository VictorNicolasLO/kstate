import { describe, it, expect } from "bun:test";
import { createKState } from "../packages/kstate/index";
describe("createKState", () => {
  it("should be a function", () => {
    expect(typeof createKState).toBe("function");
  });
});
