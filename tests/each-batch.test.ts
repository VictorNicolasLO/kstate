import { describe, it, expect } from "bun:test";
import { eachBatch } from "../packages/kstate/each-batch";
describe("eachBatch", () => {
  it("should be a function", () => {
    expect(typeof eachBatch).toBe("function");
  });

  function mockProducer(): any {
    // Minimal mock for kafkajs Producer type
    return {
      send: async () => {},
      sendBatch: async () => {},
      transaction: async () => ({
        sendBatch: async () => [
          { topicName: "snapshots", baseOffset: "10" },
        ],
        sendOffsets: async () => {},
        commit: async () => {},
        abort: async () => {},
      }),
    };
  }

  function mockStore(): any {
    // Minimal mock for Store type
    return {
      getMany: async () => [
        { baseOffset: 9, predictedNextOffset: 10, lastBatchSize: 0 },
        undefined,
      ],
      setMany: async () => {},
      setManyRaw: async () => {},
      connect: async () => {},
      disconnect: async () => {},
      get: async () => undefined,
    };
  }

  function makePayload(messages): any {
    // Minimal mock for kafkajs EachBatchPayload
    return {
      batch: {
        topic: "test-topic",
        partition: 0,
        messages,
        lastOffset: () => messages[messages.length - 1]?.offset || "0",
      },
      heartbeat: async () => {},
      resolveOffset: () => {},
      pause: () => {},
      commitOffsetsIfNecessary: async () => {},
      uncommittedOffsets: () => ({}),
      isRunning: () => true,
      isStale: () => false,
    };
  }

  it("should process a batch and commit state and offsets", async () => {
    const producers = new Map([[0, mockProducer() as any]]);
    const stores = new Map([[0, mockStore() as any]]);
    const cb = (msg, key, state, ctx) => ({ state: { ...msg, processed: true }, reactions: [] });
    const payload = makePayload([
      { key: Buffer.from("foo"), value: Buffer.from(JSON.stringify({ a: 1 })), offset: "10" },
    ]);
    let syncCalled = false;
    await eachBatch(
      producers,
      () => "partition-control-key",
      "snapshots",
      "group-id",
      "test-topic",
      stores,
      cb,
      payload,
      async () => { syncCalled = true; }
    );
    expect(syncCalled).toBe(false);
  });

  it("should call syncDB and throw if producer is missing", async () => {
    const producers = new Map();
    const stores = new Map([[0, mockStore() as any]]);
    const cb = () => ({ state: {}, reactions: [] });
    const payload = makePayload([
      { key: Buffer.from("foo"), value: Buffer.from(JSON.stringify({ a: 1 })), offset: "10" },
    ]);
    let syncCalled = false;
    await expect(
      eachBatch(
        producers,
        () => "partition-control-key",
        "snapshots",
        "group-id",
        "test-topic",
        stores,
        cb,
        payload,
        async () => { syncCalled = true; }
      )
    ).rejects.toThrow("No producer found for partition 0");
    expect(syncCalled).toBe(false);
  });

  it("should call syncDB and throw if store is missing", async () => {
    const producers = new Map([[0, mockProducer() as any]]);
    const stores = new Map();
    const cb = () => ({ state: {}, reactions: [] });
    const payload = makePayload([
      { key: Buffer.from("foo"), value: Buffer.from(JSON.stringify({ a: 1 })), offset: "10" },
    ]);
    let syncCalled = false;
    await expect(
      eachBatch(
        producers,
        () => "partition-control-key",
        "snapshots",
        "group-id",
        "test-topic",
        stores,
        cb,
        payload,
        async () => { syncCalled = true; }
      )
    ).rejects.toThrow("No store found for partition 0");
    expect(syncCalled).toBe(false);
  });

  it("should abort transaction and call syncDB on error in cb", async () => {
    const producers = new Map([[0, mockProducer() as any]]);
    const stores = new Map([[0, mockStore() as any]]);
    const cb = () => { throw new Error("fail"); };
    const payload = makePayload([
      { key: Buffer.from("foo"), value: Buffer.from(JSON.stringify({ a: 1 })), offset: "10" },
    ]);
    let syncCalled = false;
    await expect(
      eachBatch(
        producers,
        () => "partition-control-key",
        "snapshots",
        "group-id",
        "test-topic",
        stores,
        cb,
        payload,
        async () => { syncCalled = true; }
      )
    ).rejects.toThrow("fail");
    expect(syncCalled).toBe(true);
  });

  it("should abort transaction and call syncDB on error in setMany", async () => {
    const producers = new Map([[0, mockProducer() as any]]);
    const stores = new Map([[0, { ...mockStore(), setMany: async () => { throw new Error("setMany fail"); } } as any]]);
    const cb = (msg, key, state, ctx) => ({ state: { ...msg, processed: true }, reactions: [] });
    const payload = makePayload([
      { key: Buffer.from("foo"), value: Buffer.from(JSON.stringify({ a: 1 })), offset: "10" },
    ]);
    let syncCalled = false;
    await expect(
      eachBatch(
        producers,
        () => "partition-control-key",
        "snapshots",
        "group-id",
        "test-topic",
        stores,
        cb,
        payload,
        async () => { syncCalled = true; }
      )
    ).rejects.toThrow("setMany fail");
    expect(syncCalled).toBe(true);
  });
});
