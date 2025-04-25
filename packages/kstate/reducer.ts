import { Kafka, Producer, TopicMessages } from "kafkajs";
import { KafkaOptions, RedisOptions, ReducerCb } from "./types";
import { createClient, RedisClientType } from "redis";
import { eachBatch } from "./each-batch";
import { buildSnapshotTopicConfig } from "./builders";
import { syncDB } from "./sync-db";

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
    cb: ReducerCb<T>,
    kafkaClient: Kafka,
    redisClient: ReturnType<typeof createClient>,
    topic: string,
    concurrencyNumber: number = 1,
) => {
    // await redisClient.connect()
    // Create compacted topic if not exists
    const groupId = `kstate-${topic}-group`
    const snapshotTopic = `${topic}-snapshots`
    const getPartitionControlKey = (partition: number) => `snapshot-offset-${topic}-${partition}`

    const admin = kafkaClient.admin()
    await admin.connect()
    const topicDetails = await admin.fetchTopicMetadata({ topics: [topic] })
    await admin.createTopics(buildSnapshotTopicConfig(snapshotTopic, topicDetails))
    await admin.disconnect();

    // Sync Database with topic
    // TODO


    // Start consumer
    const producers = new Map<number, Producer>()


    const consumer = kafkaClient.consumer({ groupId, readUncommitted: false }) 
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: false })
    consumer.on('consumer.group_join', async (e) => {
        console.log('Group join', e)
        for (const partition of producers.values()) {
            await partition.disconnect()
        }
        producers.clear()
        const assignedPartitions = e.payload.memberAssignment[topic] || []
        consumer.pause([ {topic, partitions: assignedPartitions }])
        await Promise.all(assignedPartitions.map(async (partition) => {
            if (!producers.has(partition)) {
                const producer = await createTransactionalProducer(kafkaClient, topic, partition)
                producers.set(partition, producer)
            }
        }))
        await Promise.all(assignedPartitions.map((partition)=>  syncDB(
            kafkaClient,
            redisClient,
            topic,
            partition,
            snapshotTopic,
            getPartitionControlKey(partition),
            producers.get(partition) as Producer
        ) ))

        console.log('Group join DONE -- resume', assignedPartitions)
        consumer.resume([ {topic, partitions: assignedPartitions }])
       
    })

    // Consuming messages
    consumer.run({
        autoCommit: false,
        partitionsConsumedConcurrently: concurrencyNumber,
        eachBatchAutoResolve: true,
        eachBatch:  (payload)=> eachBatch(
            producers,
            getPartitionControlKey,
            snapshotTopic,
            groupId,
            topic,
            redisClient,
            cb,
            payload,
            () => syncDB(
                kafkaClient,
                redisClient,
                topic,
                payload.batch.partition,
                snapshotTopic,
                getPartitionControlKey(payload.batch.partition),
                producers.get(payload.batch.partition) as Producer
            )
        ),

    })


}

