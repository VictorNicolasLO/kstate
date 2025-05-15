import { describe, it, expect } from "bun:test";
import * as types from "../packages/kstate/types";
describe("types", () => {
  it("should load the types module", () => {
    expect(types).toBeDefined();
  });
});
