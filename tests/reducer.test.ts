import { describe, it, expect } from "bun:test";
import { startReducer } from "../packages/kstate/reducer";

describe("startReducer", () => {
  it("should be a function", () => {
    expect(typeof startReducer).toBe("function");
  });
});
