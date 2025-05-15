import { describe, it, expect, beforeEach } from "bun:test";
import { createRedisStore } from "../../../packages/kstate/stores/redis-store/redis-store";

describe("createRedisStore", () => {
  it("should be a function", () => {
    expect(typeof createRedisStore).toBe("function");
  });

  // Simple manual mock function for Bun
  function mockFn(impl = (..._args: any[]) => {}) {
    const fn: any = (...args: any[]) => {
      fn.calls.push(args);
      return impl(...args);
    };
    fn.calls = [];
    fn.mockClear = () => { fn.calls = []; };
    fn.mockImplementation = (newImpl: any) => { impl = newImpl; };
    fn.mock = true;
    return fn;
  }

  function mockRedis() {
    const _store: Record<string, string> = {};
    return {
      mSet: mockFn(async (kv) => { Object.assign(_store, kv); }),
      mGet: mockFn(async (keys) => keys.map((k: string) => _store[k] ?? null)),
      get: mockFn(async (key) => _store[key] ?? null),
      connect: mockFn(async () => {}),
      __store: _store,
    };
  }

  let redisClient: ReturnType<typeof mockRedis>;
  let redisStore: ReturnType<ReturnType<typeof createRedisStore>["getStore"]>;

  beforeEach(() => {
    redisClient = mockRedis();
    const adapter = createRedisStore(redisClient as any);
    redisStore = adapter.getStore("topic", 0);
  });

  it("setMany and get should store and retrieve values with prefix", async () => {
    await redisStore.setMany({ foo: 123, bar: { a: 1 } });
    expect(redisClient.mSet.calls.length).toBe(1);
    expect(redisClient.__store["topic/foo"]).toBe(JSON.stringify(123));
    expect(redisClient.__store["topic/bar"]).toBe(JSON.stringify({ a: 1 }));
    const foo = await redisStore.get("foo");
    const bar = await redisStore.get("bar");
    expect(foo).toBe(123);
    expect(bar).toEqual({ a: 1 });
  });

  it("getMany should return deserialized values in order", async () => {
    await redisStore.setMany({ a: 1, b: 2 });
    const values = await redisStore.getMany(["a", "b", "c"]);
    expect(values).toEqual([1, 2, null]);
  });

  it("setManyRaw should store raw values", async () => {
    await redisStore.setManyRaw({ foo: "raw1", bar: "raw2" });
    expect(redisClient.mSet.calls.length).toBe(1);
    expect(redisClient.__store["topic/foo"]).toBe("raw1");
    expect(redisClient.__store["topic/bar"]).toBe("raw2");
  });

  it("connect should call redisClient.connect at least once and not throw on repeat", async () => {
    await redisStore.connect();
    await redisStore.connect();
    expect(redisClient.connect.calls.length).toBeGreaterThanOrEqual(1);
  });

  it("get should return undefined for missing key", async () => {
    const value = await redisStore.get("notfound");
    expect(value).toBeUndefined();
  });
});
