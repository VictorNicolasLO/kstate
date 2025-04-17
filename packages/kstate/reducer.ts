import { Kafka, Producer, TopicMessages } from "kafkajs";
import { KafkaOptions, RedisOptions, ReducerCb } from "./types";
import { RedisClientType } from "redis";

const createTransactionalProducer = async (kafkaClient: Kafka, topic: string, partition: number) => {
    const producer = kafkaClient.producer({
        transactionalId: `kstate-${topic}-producer-${partition}`,
        maxInFlightRequests: 1,
        idempotent: true,
    })
    await producer.connect()
    return producer
}

export const startReducer = async <T>(
    redisOptions: RedisOptions,
    kafkaOptions: KafkaOptions,
    cb: ReducerCb<T>,
    kafkaClient: Kafka,
    redisClient: RedisClientType,
    topic: string,
    concurrencyNumber: number = 1,
) => {
    // Create compacted topic if not exists
    const groupId = `kstate-${topic}-group`
    const snapshotTopic = `${topic}-snapshots`
    const lastOffsetSnapshotTopic = (partition: number) => `snapshot-offset-${topic}-${partition}`
    const admin = kafkaClient.admin()
    await admin.connect()
    const topicDetails = await admin.fetchTopicMetadata({ topics: [topic] })
    await admin.createTopics({
        topics: [
            {
                topic: snapshotTopic,
                numPartitions: topicDetails.topics[0].partitions.length,
                replicationFactor: topicDetails.topics[0].partitions[0].replicas.length,
                configEntries: [
                    { name: 'cleanup.policy', value: 'compact' },
                    { name: 'retention.ms', value: '-1' },
                    { name: 'retention.bytes', value: '-1' },
                ]
            }
        ],
    })
    await admin.disconnect();

    // Sync Database with topic
    // TODO


    // Start consumer
    const producers = new Map<number, Producer>()


    const consumer = kafkaClient.consumer({ groupId })
    await consumer.connect()
    consumer.on('consumer.group_join', async (e) => {
        for (const partition of producers.values()) {
            await partition.disconnect()
        }
        producers.clear()
        const assignedPartitions = e.payload.memberAssignment[topic] || []
        for (const partition of assignedPartitions) {
            if (!producers.has(partition)) {
                const producer = await createTransactionalProducer(kafkaClient, topic, partition)
                producers.set(partition, producer)
            }
            /// TODO Make recovery
        }
    })

    // Consuming messages
    consumer.run({
        autoCommit: false,
        partitionsConsumedConcurrently: concurrencyNumber,
        eachBatchAutoResolve: false,
        eachBatch: async (payload) => {
            const { batch, resolveOffset, heartbeat } = payload
            const { messages } = batch
            const partition = batch.partition

            const producer = producers.get(partition)
            if (!producer) {
                throw new Error(`No producer found for partition ${partition}`)
            }
            const tx = await producer.transaction()
            try {
                const keySets: string[] = [lastOffsetSnapshotTopic(partition)]
                const messageGroups: { [x: string]: { offset: string, msg: any }[] } = {}
                for (const message of messages) {
                    const key = message.key.toString()
                    const value = message.value.toString()
                    if (messageGroups[key]) {
                        messageGroups[key].push({ offset: message.offset, msg: value })
                    } else {
                        messageGroups[key] = [{ offset: message.offset, msg: value }]
                    }
                }

                const states = await redisClient.mGet(keySets)
                await heartbeat()
                const reactions: TopicMessages[] = [{ topic: snapshotTopic, messages: [] }]
                const existentTopics = {}
                const nextStates = {}


                const lastOffsetinDb = states[0]
                // if (lastOffsetinDb !== offsetsMap.get(partition))
                //     throw new Error(`Offsets not equal, ${lastOffset} != ${offsetsMap.get(partition)}`)
                    /// TODO Make recovery

                for (const i in keySets) {
                    if (i === '0') {
                        continue
                    }
                    let state: { payload: any, offset: number, version: number } = states[i]
                    const key = keySets[i]
                    const messages = messageGroups[key]
                    for (const message of messages) {
                        if(parseInt(message.offset, 10) <= state.offset) 
                            throw new Error(`Message offset ${message.offset} is less than or equal to state offset ${state.offset}`)
                            /// TODO Make backwards recovery
                        const { state: newState, reactions: newReactions } = cb(message, key, state.payload, {
                            topic,
                            partition,
                            offset: parseInt(message.offset, 10),
                        });

                        state.version = (state.version || 0) + 1;
                        state.offset = parseInt(message.offset, 10);
                        state.payload = newState

                        for (const reaction of newReactions) {
                            if (!existentTopics[reaction.topic]) {
                                existentTopics[reaction.topic] = reactions.push({
                                    topic: reaction.topic,
                                    messages: [{ value: JSON.stringify(reaction.message), key: reaction.key }],
                                }) - 1
                            } else {
                                reactions[existentTopics[reaction.topic]].messages.push({
                                    value: JSON.stringify(reaction.message),
                                    key: reaction.key,
                                })
                            }
                        }
                        reactions[0].messages.push({
                            value: JSON.stringify(state),
                            key: key,
                            partition: partition,
                        })
                        resolveOffset(message.offset);

                    }

                    nextStates[key] = state
                }
                const batchResponse = await tx.sendBatch({
                    acks: -1,
                    topicMessages: reactions
                })
                if(!batchResponse[0] || !batchResponse[0].offset)
                    throw new Error(`Batch response is empty or offset is not defined`)
                const lastSnapshotOffsetSent = parseInt(batchResponse[0].offset, 10)
                if (lastOffsetinDb !== lastSnapshotOffsetSent - 1)
                    throw new Error(`Offsets not equal, ${lastOffsetinDb} != ${lastSnapshotOffsetSent} - 1`)
   
                nextStates[lastOffsetSnapshotTopic(partition)] = lastSnapshotOffsetSent // TODO Confirm this operations goes to the end
                await heartbeat()
                await tx.sendOffsets({
                    consumerGroupId: groupId,
                    topics: [{
                        topic: batch.topic,
                        partitions: [{
                            partition: batch.partition,
                            offset: (Number(batch.lastOffset()) + 1).toString(),
                        }],
                    }],
                });
                await tx.commit()
                
                await redisClient.mSet(nextStates)
            } catch (err) {
                console.error('Transaction failed, aborting:', err);
                await tx.abort();
                throw err
            }
        },

    })


}

