import { describe, it, expect } from "bun:test";
import { getInMemoryStore, createInMemoryStore } from "../../../packages/kstate/stores/in-memory-store/in-memory-store";

describe("getInMemoryStore", () => {
  it("should set and get a value", async () => {
    const store = getInMemoryStore({});
    await store.setMany({ key: "value" });
    const value = await store.get("key");
    expect(value).toBe("value");
  });

  it("should set and get multiple values", async () => {
    const store = getInMemoryStore({});
    await store.setMany({ a: 1, b: 2 });
    const a = await store.get("a");
    const b = await store.get("b");
    expect(a).toBe(1);
    expect(b).toBe(2);
  });

  it("should overwrite existing values", async () => {
    const store = getInMemoryStore({});
    await store.setMany({ key: "first" });
    await store.setMany({ key: "second" });
    const value = await store.get("key");
    expect(value).toBe("second");
  });

  it("should return undefined for non-existent key", async () => {
    const store = getInMemoryStore({});
    const value = await store.get("doesNotExist");
    expect(value).toBeUndefined();
  });

  it("should set and get many values using getMany (array result)", async () => {
    const store = getInMemoryStore({});
    await store.setMany({ x: 10, y: 20, z: 30 });
    const values = await store.getMany(["x", "y", "z", "notThere"]);
    expect(values).toEqual([10, 20, 30, null]);
  });

  it("should isolate data between store instances", async () => {
    const store1 = getInMemoryStore({});
    const store2 = getInMemoryStore({});
    await store1.setMany({ key: "one" });
    await store2.setMany({ key: "two" });
    const v1 = await store1.get("key");
    const v2 = await store2.get("key");
    expect(v1).toBe("one");
    expect(v2).toBe("two");
  });

  it("should isolate data between store instances and correspond to the dict passed", async () => {
    const dict1: Record<string, any> = {} ;
    const dict2: Record<string, any> = {};
    const store1 = getInMemoryStore(dict1);
    const store2 = getInMemoryStore(dict2);
    await store1.setMany({ key: "one" });
    await store2.setMany({ key: "two" });
    const v1 = await store1.get("key");
    const v2 = await store2.get("key");
    expect(v1).toBe("one");
    expect(v2).toBe("two");
    expect(dict1['key']).toBe("one");
    expect(dict2['key']).toBe("two");
  });

  it("should call connect and disconnect (no-op, but covered)", async () => {
    const store = getInMemoryStore({});
    // Should not throw and should print to console
    await store.connect();
    await store.disconnect();
  });

  it("should setManyRaw and parse JSON values", async () => {
    const store = getInMemoryStore({});
    await store.setManyRaw({ foo: JSON.stringify(123), bar: JSON.stringify({ a: 1 }) });
    const foo = await store.get("foo");
    const bar = await store.get("bar");
    expect(foo).toBe(123);
    expect(bar).toEqual({ a: 1 });
  });

  it("should return null for getMany when value is falsy (e.g., undefined, null, 0, '')", async () => {
    const memory: any = { a: 0, b: '', c: null, d: undefined, e: false };
    const store = getInMemoryStore(memory);
    const values = await store.getMany(["a", "b", "c", "d", "e", "f"]);
    expect(values).toEqual([null, null, null, null, null, null]);
  });

  it("should setManyRaw handle invalid JSON and throw", async () => {
    const store = getInMemoryStore({});
    await expect(store.setManyRaw({ bad: "not-json" })).rejects.toThrow();
  });

  it("createInMemoryStore should return a StoreAdapter with getStore that returns isolated stores", async () => {
    const adapter = createInMemoryStore();
    expect(typeof adapter.getStore).toBe("function");
    const storeA = adapter.getStore("topicA", 0);
    const storeB = adapter.getStore("topicB", 1);
    await storeA.setMany({ foo: 1 });
    await storeB.setMany({ foo: 2 });
    const a = await storeA.get("foo");
    const b = await storeB.get("foo");
    expect(a).toBe(1);
    expect(b).toBe(2);
    // Stores should be isolated
    await storeA.setMany({ bar: 3 });
    const barA = await storeA.get("bar");
    const barB = await storeB.get("bar");
    expect(barA).toBe(3);
    expect(barB).toBeUndefined();
  });
});
