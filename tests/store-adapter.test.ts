import { describe, it, expect } from "bun:test";
import * as storeAdapter from "../packages/kstate/stores/store-adapter";
describe("store-adapter", () => {
  it("should load the store-adapter module", () => {
    expect(storeAdapter).toBeDefined();
  });
});
