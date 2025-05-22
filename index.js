// packages/kstate/each-batch.ts
var eachBatch = async (producers, getPartitionControlKey, snapshotTopic, groupId, topic, stores, cb, payload, syncDB) => {
  const { batch, heartbeat } = payload;
  console.log("Each-batch batch", batch.topic, batch.partition, batch.messages.length);
  const { messages } = batch;
  const partition = batch.partition;
  const partitionControlKey = getPartitionControlKey(partition);
  const producer = producers.get(partition);
  if (!producer)
    throw new Error(`No producer found for partition ${partition}`);
  const store = stores.get(partition);
  if (!store)
    throw new Error(`No store found for partition ${payload.batch.partition}`);
  const tx = await producer.transaction();
  const nextStates = {};
  try {
    const keySets = [partitionControlKey];
    const messageGroups = {};
    for (const message of messages) {
      const key = message.key ? message.key.toString() : "UNDEFINED";
      const value = message.value ? JSON.parse(message.value.toString()) : {};
      if (messageGroups[key]) {
        messageGroups[key].push({ offset: message.offset, msg: value });
      } else {
        keySets.push(key);
        messageGroups[key] = [{ offset: message.offset, msg: value }];
      }
    }
    const states = await store.getMany(keySets);
    await heartbeat();
    const reactions = [{ topic: snapshotTopic, messages: [] }];
    const snapshotReactionIndex = 0;
    const existentTopics = {};
    const lastPartitionControl = states[0] ?? {
      baseOffset: 0,
      lastBatchSize: 0,
      predictedNextOffset: 0
    };
    for (const i in keySets) {
      let state = states[i] ?? {};
      const key = keySets[i];
      if (key === partitionControlKey) {
        continue;
      }
      const messages2 = messageGroups[key];
      for (const message of messages2) {
        if (parseInt(message.offset, 10) <= state.inputOffset)
          throw new Error(`Message offset ${message.offset} is less than or equal to state offset ${state.inputOffset}`);
        const { state: newState, reactions: newReactions } = cb(message.msg, key, state.payload, {
          topic,
          partition,
          offset: parseInt(message.offset, 10)
        });
        state.version = (state.version || 0) + 1;
        state.previousBatchBaseOffset = lastPartitionControl.baseOffset;
        state.inputOffset = parseInt(message.offset, 10);
        state.payload = newState;
        for (const reaction of newReactions) {
          if (!existentTopics[reaction.topic]) {
            existentTopics[reaction.topic] = reactions.push({
              topic: reaction.topic,
              messages: [{ value: JSON.stringify(reaction.message), key: reaction.key }]
            }) - 1;
          } else {
            reactions[existentTopics[reaction.topic]].messages.push({
              value: JSON.stringify(reaction.message),
              key: reaction.key
            });
          }
        }
      }
      reactions[snapshotReactionIndex].messages.push({
        value: JSON.stringify(state),
        key,
        partition
      });
      nextStates[key] = state;
    }
    const batchResponse = await tx.sendBatch({
      acks: -1,
      topicMessages: reactions,
      compression: 1
    });
    const snapshotIndex = batchResponse.findIndex(({ topicName }) => topicName === snapshotTopic);
    if (!batchResponse[snapshotIndex] || !batchResponse[snapshotIndex].baseOffset)
      throw new Error(`Batch response is empty or base offset is not defined ${JSON.stringify(batchResponse)}`);
    const baseOffset = parseInt(batchResponse[snapshotIndex].baseOffset, 10);
    if (lastPartitionControl.predictedNextOffset !== baseOffset)
      throw new Error(`Offsets not equal, ${lastPartitionControl.predictedNextOffset} != ${baseOffset} ---- ${batchResponse[snapshotIndex].topicName}`);
    lastPartitionControl.predictedNextOffset = baseOffset + reactions[snapshotReactionIndex].messages.length + 1;
    lastPartitionControl.baseOffset = baseOffset;
    lastPartitionControl.lastBatchSize = messages.length;
    nextStates[partitionControlKey] = lastPartitionControl;
    await heartbeat();
    await tx.sendOffsets({
      consumerGroupId: groupId,
      topics: [{
        topic: batch.topic,
        partitions: [{
          partition: batch.partition,
          offset: (Number(batch.lastOffset()) + 1).toString()
        }]
      }]
    });
    await tx.commit();
    payload.resolveOffset(batch.lastOffset());
  } catch (err) {
    console.error("Transaction failed, aborting:", err);
    await tx.abort();
    await syncDB();
    throw err;
  }
  try {
    await store.setMany(nextStates);
  } catch (err) {
    console.error("Error setting states", err);
    await syncDB();
    throw err;
  }
};

// packages/kstate/builders.ts
var buildSnapshotTopicConfig = (snapshotTopic, topicDetails) => ({
  topics: [
    {
      topic: snapshotTopic,
      numPartitions: topicDetails.topics[0].partitions.length,
      replicationFactor: topicDetails.topics[0].partitions[0].replicas.length,
      configEntries: [
        { name: "cleanup.policy", value: "compact" },
        { name: "retention.ms", value: "-1" },
        { name: "retention.bytes", value: "-1" }
      ]
    }
  ]
});

// packages/kstate/utils/deferred-promise.ts
function createDeferredPromise() {
  let resolve, reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

// packages/kstate/sync-db.ts
var syncDB = async (kafkaClient, stores, topic, partitionNumber, snapshotTopic, partitionControlKey, producers) => {
  console.log("Syncing DB", topic, partitionNumber);
  const store = stores.get(partitionNumber);
  if (!store)
    throw new Error(`No store found for partition ${partitionNumber}`);
  const partitionControl = await store.get(partitionControlKey) ?? {
    baseOffset: 0,
    lastBatchSize: 0,
    predictedNextOffset: 0
  };
  const fromOffset = partitionControl.baseOffset;
  const producer = producers.get(partitionNumber);
  if (!producer)
    throw new Error(`No producer found for partition ${partitionNumber}`);
  const tx = await producer.transaction();
  const controlSent = await tx.send({
    topic: snapshotTopic,
    messages: [
      {
        key: partitionControlKey + "_CONTROL_KEY",
        value: JSON.stringify(partitionControl),
        partition: partitionNumber
      }
    ]
  });
  await tx.commit();
  console.log("Control sent", controlSent[0]);
  if (!controlSent[0].baseOffset)
    throw new Error(`Offset not found for partition ${partitionNumber}`);
  const toOffset = parseInt(controlSent[0].baseOffset, 10);
  const deferredPromise = createDeferredPromise();
  const consumer = kafkaClient.consumer({ groupId: `sync-consumer-${topic}-${partitionNumber}`, readUncommitted: false });
  await consumer.connect();
  await consumer.subscribe({ topic: snapshotTopic, fromBeginning: false });
  const admin = kafkaClient.admin();
  await admin.connect();
  const topicDetails = await admin.fetchTopicMetadata({ topics: [snapshotTopic] });
  const topicsoffsets = await admin.fetchTopicOffsets(snapshotTopic);
  await admin.disconnect();
  await consumer.run({
    eachBatch: async (payload) => {
      console.log("Batch", payload.batch.partition, partitionNumber, payload.batch.firstOffset(), payload.batch.lastOffset());
      if (payload.batch.partition !== partitionNumber) {
        return;
      }
      const { batch, heartbeat } = payload;
      const states = {};
      for (const message of batch.messages) {
        if (message.key === undefined || message.key === null) {
          console.log("Message without key", message);
          continue;
        }
        if (message.value === undefined || message.value === null) {
          console.log("Message without value", message);
          continue;
        }
        const key = message.key.toString();
        const value = message.value.toString();
        states[key] = value;
        if (message.offset === toOffset + "") {
          console.log("Offset found", partitionNumber, message.offset);
          console.log("Syncing DB DONE Batch", topic, partitionNumber, fromOffset, toOffset);
          const partitionControl3 = {
            baseOffset: toOffset,
            lastBatchSize: 1,
            predictedNextOffset: toOffset + 1 + 1
          };
          states[partitionControlKey] = JSON.stringify(partitionControl3);
          await store.setManyRaw(states);
          console.log("mset done", topic, partitionNumber);
          consumer.stop().then(() => consumer.disconnect());
          console.log("Syncing DB resolved", topic, partitionNumber);
          deferredPromise.resolve();
          return;
        }
      }
      const offsetStr = batch.messages[batch.messages.length - 1].offset;
      if (!offsetStr) {
        throw new Error(`Offset not found for partition ${partitionNumber}`);
      }
      const offset = parseInt(offsetStr, 10);
      const partitionControl2 = {
        baseOffset: offset,
        lastBatchSize: 0,
        predictedNextOffset: offset
      };
      states[partitionControlKey] = JSON.stringify(partitionControl2);
      await store.setManyRaw(states);
      await heartbeat();
    }
  });
  for (const partition of topicDetails.topics[0].partitions) {
    const offset = topicsoffsets.find(({ partition: p }) => p === partition.partitionId)?.offset;
    const offsetHigh = topicsoffsets.find(({ partition: p }) => p === partition.partitionId)?.high;
    if (offset === undefined) {
      throw new Error(`Offset not found for partition ${partition.partitionId}`);
    }
    if (partition.partitionId == partitionNumber) {
      console.log("Topic having messages {partitionid, fromOffset, toOffset, offsetHigh}", partition.partitionId, fromOffset, toOffset, offsetHigh);
      consumer.seek({ offset: fromOffset + "", topic: snapshotTopic, partition: partition.partitionId });
    } else
      consumer.seek({ offset, topic: snapshotTopic, partition: partition.partitionId });
  }
  await deferredPromise.promise;
  console.log("Syncing DB DONE", topic, partitionNumber, fromOffset, toOffset);
};

// packages/kstate/reducer.ts
var createTransactionalProducer = async (kafkaClient, topic, partition) => {
  const txId = `kstate-${topic}-producer-${partition}`;
  const producer = kafkaClient.producer({
    transactionalId: txId,
    maxInFlightRequests: 1,
    idempotent: true
  });
  await producer.connect();
  return producer;
};
var startReducer = async (cb, kafkaClient, storeAdapter, topic, options) => {
  const groupId = `kstate-${topic}-group`;
  const snapshotTopic = `${topic}-snapshots`;
  const getPartitionControlKey = (partition) => `snapshot-offset-${topic}-${partition}`;
  const admin = kafkaClient.admin();
  await admin.connect();
  const topicDetails = await admin.fetchTopicMetadata({ topics: [topic] });
  const snapshotConfig = buildSnapshotTopicConfig(snapshotTopic, topicDetails);
  const concurrencyNumber = snapshotConfig.topics[0].numPartitions;
  await admin.createTopics(snapshotConfig);
  await admin.disconnect();
  const producers = new Map;
  const stores = new Map;
  const consumer = kafkaClient.consumer({ groupId, readUncommitted: false });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  consumer.on("consumer.group_join", async (e) => {
    console.log("Group join", e);
    for (const partition of producers.values()) {
      await partition.disconnect();
    }
    for (const store of stores.values()) {
      await store.disconnect();
    }
    producers.clear();
    const assignedPartitions = e.payload.memberAssignment[topic] || [];
    consumer.pause([{ topic, partitions: assignedPartitions }]);
    await Promise.all(assignedPartitions.map(async (partition) => {
      if (!producers.has(partition)) {
        const producer = await createTransactionalProducer(kafkaClient, topic, partition);
        producers.set(partition, producer);
      }
    }));
    await Promise.all(assignedPartitions.map(async (partition) => {
      if (!stores.has(partition)) {
        const store = storeAdapter.getStore(topic, partition);
        await store.connect();
        stores.set(partition, store);
      }
    }));
    await Promise.all(assignedPartitions.map((partition) => syncDB(kafkaClient, stores, topic, partition, snapshotTopic, getPartitionControlKey(partition), producers)));
    console.log("Group join DONE -- resume", assignedPartitions, producers.size);
    consumer.resume([{ topic, partitions: assignedPartitions }]);
  });
  const BATCH_LIMIT = options.batch_limit || 5000;
  consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: concurrencyNumber,
    eachBatchAutoResolve: false,
    eachBatch: async (payload) => {
      for (let i = 0;i < payload.batch.messages.length; i += BATCH_LIMIT) {
        const messages = payload.batch.messages.slice(i, i + BATCH_LIMIT);
        await eachBatch(producers, getPartitionControlKey, snapshotTopic, groupId, topic, stores, cb, {
          ...payload,
          batch: {
            ...payload.batch,
            messages,
            lastOffset() {
              return messages[messages.length - 1].offset;
            }
          }
        }, () => syncDB(kafkaClient, stores, topic, payload.batch.partition, snapshotTopic, getPartitionControlKey(payload.batch.partition), producers));
      }
    }
  });
};

// packages/kstate/stores/in-memory-store/in-memory-store.ts
var getInMemoryStore = (memory) => {
  return {
    setMany: async (kv) => {
      for (const key in kv) {
        memory[key] = kv[key];
      }
    },
    getMany: async (keys) => {
      const values = keys.map((key) => memory[key]);
      return values.map((v) => v ? v : null);
    },
    connect: async () => {
      console.log("In-memory store connected");
    },
    disconnect: async () => {
      console.log("In-memory store disconnected");
    },
    get: async (key) => {
      const value = memory[key];
      if (!value)
        return;
      return value;
    },
    setManyRaw: async (kv) => {
      for (const key in kv) {
        memory[key] = JSON.parse(kv[key]);
      }
    }
  };
};
var createInMemoryStore = () => {
  return {
    getStore: (topic, partition) => {
      const memory = {};
      return getInMemoryStore(memory);
    }
  };
};

// packages/kstate/stores/redis-store/redis-store.ts
var connected = false;
var getRedisStore = (redisClient, topic, _partition) => {
  const prefix = `${topic}/`;
  return {
    setMany: async (kv) => {
      const newKv = {};
      for (const key in kv) {
        newKv[prefix + key] = JSON.stringify(kv[key]);
      }
      await redisClient.mSet(newKv);
    },
    getMany: async (keys) => {
      const values = await redisClient.mGet(keys.map((key) => prefix + key));
      return values.map((v) => v ? JSON.parse(v) : null);
    },
    connect: async () => {
      if (connected)
        return;
      connected = true;
      await redisClient.connect();
    },
    disconnect: async () => {
    },
    get: async (key) => {
      const value = await redisClient.get(prefix + key);
      if (!value)
        return;
      return JSON.parse(value);
    },
    setManyRaw: async (kv) => {
      const newKv = {};
      for (const key in kv) {
        newKv[prefix + key] = kv[key];
      }
      await redisClient.mSet(newKv);
    }
  };
};
var createRedisStore = (redisClient) => {
  return {
    getStore: (topic, partition) => getRedisStore(redisClient, topic, partition)
  };
};

// packages/kstate/index.ts
var fromTopic = (store, kafkaClient) => (topic, topicOptions) => ({
  reduce: (cb) => startReducer(cb, kafkaClient, store, topic, topicOptions)
});
var createKState = (store, kafkaCLient) => {
  return {
    fromTopic: fromTopic(store, kafkaCLient)
  };
};
export {
  createRedisStore,
  createKState,
  createInMemoryStore
};
