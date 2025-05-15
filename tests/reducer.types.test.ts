import { describe, it, expect } from "bun:test";
import * as reducerTypes from "../packages/kstate/reducer.types";
describe("reducer.types", () => {
  it("should load the reducer.types module", () => {
    expect(reducerTypes).toBeDefined();
  });
});
