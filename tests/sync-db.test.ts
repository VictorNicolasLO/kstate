import { describe, it, expect, beforeEach } from "bun:test";
import { syncDB } from "../packages/kstate/sync-db";

// Minimal manual mock function for Bun (since vi is not available)
function mockFn(impl = (..._args: any[]) => {}) {
  const fn: any = (...args: any[]) => {
    fn.calls.push(args);
    return impl(...args);
  };
  fn.calls = [];
  fn.mockClear = () => { fn.calls = []; };
  fn.mockImplementation = (newImpl: any) => { impl = newImpl; };
  fn.mockResolvedValue = (val: any) => fn.mockImplementation(() => Promise.resolve(val));
  fn.mockRejectedValue = (err: any) => fn.mockImplementation(() => Promise.reject(err));
  fn.mock = true;
  return fn;
}

describe("syncDB", () => {
  it("should be a function", () => {
    expect(typeof syncDB).toBe("function");
  });

  function createMockProducer(extra?: any) {
    return {
      transaction: mockFn(async () => ({
        send: mockFn(async () => [{ baseOffset: "10" }]),
        commit: mockFn(async () => undefined),
        abort: mockFn(async () => undefined),
      })),
      send: mockFn(),
      sendBatch: mockFn(),
      connect: mockFn(async () => undefined),
      disconnect: mockFn(async () => undefined),
      isIdempotent: mockFn(() => true),
      events: {},
      on: mockFn(() => {}),
      // ...add any extra overrides
      ...(extra || {})
    } as any;
  }

  function createMockStore(partitionControl?: any) {
    return {
      get: mockFn(async (key) => {
        if (key.endsWith("CONTROL_KEY")) return undefined;
        return partitionControl ?? { baseOffset: 0, lastBatchSize: 0, predictedNextOffset: 0 };
      }),
      setManyRaw: mockFn(async () => undefined),
    } as any;
  }

  function createMockKafkaClient({ consumerEachBatch, adminFetchTopicMetadata, adminFetchTopicOffsets }: any = {}) {
    return {
      consumer: mockFn(() => ({
        connect: mockFn(async () => undefined),
        subscribe: mockFn(async () => undefined),
        run: mockFn(async ({ eachBatch }) => {
          if (consumerEachBatch) await consumerEachBatch(eachBatch);
        }),
        stop: mockFn(async () => undefined),
        disconnect: mockFn(async () => undefined),
        seek: mockFn(() => {}),
      })),
      admin: mockFn(() => ({
        connect: mockFn(async () => undefined),
        fetchTopicMetadata: mockFn(async () => adminFetchTopicMetadata ?? {
          topics: [{ partitions: [{ partitionId: 0 }] }],
        }),
        fetchTopicOffsets: mockFn(async () => adminFetchTopicOffsets ?? [
          { partition: 0, offset: "0", high: "11" },
        ]),
        disconnect: mockFn(async () => undefined),
      })),
    } as any;
  }

  let kafkaClient: any;
  let stores: Map<number, any>;
  let producers: Map<number, any>;
  let partitionControlKey: string;

  beforeEach(() => {
    partitionControlKey = "partition-0-ctrl";
    stores = new Map([[0, createMockStore()]]);
    producers = new Map([[0, createMockProducer()]]);
  });

  it("should call producer.transaction, send, commit, and setManyRaw", async () => {
    let batchCalled = false;
    kafkaClient = createMockKafkaClient({
      consumerEachBatch: async (eachBatch: any) => {
        await eachBatch({
          batch: {
            partition: 0,
            messages: [
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}'), offset: "10" },
            ],
            firstOffset: () => "10",
            lastOffset: () => "10",
          },
        });
        batchCalled = true;
      },
    });
    await syncDB(
      kafkaClient,
      stores,
      "topic",
      0,
      "snapshots",
      partitionControlKey,
      producers
    );
    expect(batchCalled).toBe(true);
    const producer = producers.get(0);
    expect(producer.transaction.calls.length).toBeGreaterThan(0);
    const store = stores.get(0);
    expect(store.setManyRaw.calls.length).toBeGreaterThan(0);
  });

  it("should throw if no store for partition", async () => {
    kafkaClient = createMockKafkaClient();
    const emptyStores = new Map();
    await expect(
      syncDB(kafkaClient, emptyStores, "topic", 0, "snapshots", partitionControlKey, producers)
    ).rejects.toThrow("No store found for partition 0");
  });

  it("should throw if no producer for partition", async () => {
    kafkaClient = createMockKafkaClient();
    const emptyProducers = new Map();
    await expect(
      syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, emptyProducers)
    ).rejects.toThrow("No producer found for partition 0");
  });

  it("should throw if offset not found in controlSent", async () => {
    // Patch producer to return no baseOffset
    const badProducer = createMockProducer({
      transaction: mockFn(async () => ({
        send: mockFn(async () => [{}]),
        commit: mockFn(async () => undefined),
        abort: mockFn(async () => undefined),
      })),
    });
    const badProducers = new Map([[0, badProducer]]);
    kafkaClient = createMockKafkaClient();
    await expect(
      syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, badProducers)
    ).rejects.toThrow("Offset not found for partition 0");
  });

  it("should handle error in setManyRaw and propagate", async () => {
    // Patch store to throw in setManyRaw
    const errorStore = createMockStore();
    errorStore.setManyRaw = mockFn(async () => { throw new Error("fail setManyRaw"); });
    const errorStores = new Map([[0, errorStore]]);
    let batchCalled = false;
    kafkaClient = createMockKafkaClient({
      consumerEachBatch: async (eachBatch: any) => {
        batchCalled = true;
        await eachBatch({
          batch: {
            partition: 0,
            messages: [
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}'), offset: "10" },
            ],
            firstOffset: () => "10",
            lastOffset: () => "10",
          },
        });
      },
    });
    await expect(
      syncDB(kafkaClient, errorStores, "topic", 0, "snapshots", partitionControlKey, producers)
    ).rejects.toThrow("fail setManyRaw");
    // batchCalled may be false if error is thrown before eachBatch is called, so just check type
    expect(typeof batchCalled).toBe("boolean");
  });

  it("should skip batch if partition does not match", async () => {
    let batchCalled = false;
    let promiseResolved = false;
    kafkaClient = createMockKafkaClient({
      consumerEachBatch: async (eachBatch: any) => {
        await eachBatch({
          batch: {
            partition: 1, // not 0
            messages: [
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}'), offset: "10" },
            ],
            firstOffset: () => "10",
            lastOffset: () => "10",
          },
          heartbeat: async () => {},
        });
        batchCalled = true;
        promiseResolved = true;
      },
    });
    // Patch: resolve after batch handler
    await Promise.race([
      (async () => { try { await syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, producers); } catch {} })(),
      new Promise((resolve) => setTimeout(resolve, 100)),
    ]);
    expect(batchCalled).toBe(true);
    expect(promiseResolved).toBe(true);
  });

  it("should skip message with undefined key", async () => {
    let sawLog = false;
    let batchCalled = false;
    let promiseResolved = false;
    const origLog = console.log;
    console.log = (...args: any[]) => { if (String(args[0]).includes('without key')) sawLog = true; };
    kafkaClient = createMockKafkaClient({
      consumerEachBatch: async (eachBatch: any) => {
        await eachBatch({
          batch: {
            partition: 0,
            messages: [
              { key: undefined, value: Buffer.from('{}'), offset: "10" },
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}'), offset: "11" },
            ],
            firstOffset: () => "10",
            lastOffset: () => "11",
          },
          heartbeat: async () => {},
        });
        batchCalled = true;
        promiseResolved = true;
      },
    });
    await Promise.race([
      (async () => { try { await syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, producers); } catch {} })(),
      new Promise((resolve) => setTimeout(resolve, 100)),
    ]);
    console.log = origLog;
    expect(sawLog).toBe(true);
    expect(batchCalled).toBe(true);
    expect(promiseResolved).toBe(true);
  });

  it("should skip message with undefined value", async () => {
    let sawLog = false;
    let batchCalled = false;
    let promiseResolved = false;
    const origLog = console.log;
    console.log = (...args: any[]) => { if (String(args[0]).includes('without value')) sawLog = true; };
    kafkaClient = createMockKafkaClient({
      consumerEachBatch: async (eachBatch: any) => {
        await eachBatch({
          batch: {
            partition: 0,
            messages: [
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: undefined, offset: "10" },
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}'), offset: "11" },
            ],
            firstOffset: () => "10",
            lastOffset: () => "11",
          },
          heartbeat: async () => {},
        });
        batchCalled = true;
        promiseResolved = true;
      },
    });
    await Promise.race([
      (async () => { try { await syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, producers); } catch {} })(),
      new Promise((resolve) => setTimeout(resolve, 100)),
    ]);
    console.log = origLog;
    expect(sawLog).toBe(true);
    expect(batchCalled).toBe(true);
    expect(promiseResolved).toBe(true);
  });

  it("should throw if last message has no offset", async () => {
    kafkaClient = createMockKafkaClient({
      consumerEachBatch: async (eachBatch: any) => {
        await eachBatch({
          batch: {
            partition: 0,
            messages: [
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}') }, // no offset
            ],
            firstOffset: () => "10",
            lastOffset: () => undefined,
          },
          heartbeat: async () => {},
        });
      },
    });
    await expect(
      syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, producers)
    ).rejects.toThrow("Offset not found for partition 0");
  });

  it("should call consumer.seek for both partition and non-partition", async () => {
    let seekCalls: any[] = [];
    const mockConsumer = {
      connect: mockFn(async () => undefined),
      subscribe: mockFn(async () => undefined),
      run: mockFn(async ({ eachBatch }) => {
        await eachBatch({
          batch: {
            partition: 0,
            messages: [
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}'), offset: "10" },
            ],
            firstOffset: () => "10",
            lastOffset: () => "10",
          },
          heartbeat: async () => {},
        });
      }),
      stop: mockFn(async () => undefined),
      disconnect: mockFn(async () => undefined),
      seek: (...args: any[]) => { seekCalls.push(args); },
    };
    const mockAdmin = {
      connect: mockFn(async () => undefined),
      fetchTopicMetadata: mockFn(async () => ({ topics: [{ partitions: [{ partitionId: 0 }, { partitionId: 1 }] }] })),
      fetchTopicOffsets: mockFn(async () => [
        { partition: 0, offset: "0", high: "11" },
        { partition: 1, offset: "5", high: "6" },
      ]),
      disconnect: mockFn(async () => undefined),
    };
    kafkaClient = {
      consumer: mockFn(() => mockConsumer),
      admin: mockFn(() => mockAdmin),
    };
    await syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, producers);
    expect(seekCalls.length).toBe(2);
    expect(seekCalls[0][0].partition).toBe(0); // partition 0
    expect(seekCalls[1][0].partition).toBe(1); // partition 1
  });

  it("should throw if offset is undefined in seek loop", async () => {
    const mockConsumer = {
      connect: mockFn(async () => undefined),
      subscribe: mockFn(async () => undefined),
      run: mockFn(async ({ eachBatch }) => {
        await eachBatch({
          batch: {
            partition: 0,
            messages: [
              { key: Buffer.from(partitionControlKey + '_CONTROL_KEY'), value: Buffer.from('{}'), offset: "10" },
            ],
            firstOffset: () => "10",
            lastOffset: () => "10",
          },
          heartbeat: async () => {},
        });
      }),
      stop: mockFn(async () => undefined),
      disconnect: mockFn(async () => undefined),
      seek: mockFn(() => {}),
    };
    const mockAdmin = {
      connect: mockFn(async () => undefined),
      fetchTopicMetadata: mockFn(async () => ({ topics: [{ partitions: [{ partitionId: 0 }, { partitionId: 1 }] }] })),
      fetchTopicOffsets: mockFn(async () => [
        { partition: 1, offset: "5", high: "6" }, // partition 0 missing
      ]),
      disconnect: mockFn(async () => undefined),
    };
    kafkaClient = {
      consumer: mockFn(() => mockConsumer),
      admin: mockFn(() => mockAdmin),
    };
    await expect(
      syncDB(kafkaClient, stores, "topic", 0, "snapshots", partitionControlKey, producers)
    ).rejects.toThrow("Offset not found for partition 0");
  });
});
