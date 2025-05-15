import { describe, it, expect } from "bun:test";
import { getInMemoryStore } from "../packages/kstate/stores/in-memory-store/in-memory-store";

describe("getInMemoryStore", () => {
  it("should set and get a value", async () => {
    const store = getInMemoryStore({});
    await store.setMany({ key: "value" });
    const value = await store.get("key");
    expect(value).toBe("value");
  });
});
