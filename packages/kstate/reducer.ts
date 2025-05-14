import { Kafka, Producer } from "kafkajs";
import { eachBatch } from "./each-batch";
import { buildSnapshotTopicConfig } from "./builders";
import { syncDB } from "./sync-db";
import { Store, StoreAdapter } from "./stores/store-adapter";
import { ReducerCb } from "./types";

const createTransactionalProducer = async (kafkaClient: Kafka, topic: string, partition: number) => {
    const txId = `kstate-${topic}-producer-${partition}`
    const producer = kafkaClient.producer({
        transactionalId: txId,
        maxInFlightRequests: 1,
        idempotent: true,
    })
    // producer.on('producer.network.request', (e)=> {
    //     console.log('Network request from', partition, e.payload.apiName, e.payload.correlationId)

    // })
    await producer.connect()
    return producer
}

export const startReducer = async <T>(
    cb: ReducerCb<T>,
    kafkaClient: Kafka,
    storeAdapter: StoreAdapter,
    topic: string
) => {
    // Create compacted topic if not exists
    const groupId = `kstate-${topic}-group`
    const snapshotTopic = `${topic}-snapshots`
    const getPartitionControlKey = (partition: number) => `snapshot-offset-${topic}-${partition}`

    const admin = kafkaClient.admin()
    await admin.connect()
    const topicDetails = await admin.fetchTopicMetadata({ topics: [topic] })
    const snapshotConfig = buildSnapshotTopicConfig(snapshotTopic, topicDetails)
    const concurrencyNumber = snapshotConfig.topics[0].numPartitions
    await admin.createTopics(snapshotConfig)
    await admin.disconnect();

    const producers = new Map<number, Producer>()
    const stores = new Map<number, Store>()

    const consumer = kafkaClient.consumer({ groupId, readUncommitted: false }) 
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: false })
    consumer.on('consumer.group_join', async (e) => {
        console.log('Group join', e)
        for (const partition of producers.values()) {
            await partition.disconnect()
        }
        for (const store of stores.values()) {
            await store.disconnect()
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

        await Promise.all(assignedPartitions.map(async (partition) => {
            if (!stores.has(partition)) {
                const store = storeAdapter.getStore(topic, partition)
                await store.connect()
                stores.set(partition, store)
            }
        }))
        await Promise.all(assignedPartitions.map((partition)=>  syncDB(
            kafkaClient,
            stores,
            topic,
            partition,
            snapshotTopic,
            getPartitionControlKey(partition),
            producers
        ) ))
       
        console.log('Group join DONE -- resume', assignedPartitions, producers.size)
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
            stores,
            cb,
            payload,
            () => syncDB(
                kafkaClient,
                stores,
                topic,
                payload.batch.partition,
                snapshotTopic,
                getPartitionControlKey(payload.batch.partition),
                producers
            )
        ),

    })


}

