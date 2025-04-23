import { createClient, RedisClientType } from "redis";
import { KafkaOptions, RedisOptions } from "./types";
import { Kafka } from "kafkajs";
import { PartitionControl } from "./reducer.types";

function createDeferredPromise() {
    let resolve:any, reject: any;
    
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });

    return { promise, resolve: resolve as (...args:any)=> void, reject: reject as (...args:any)=> void};
}


export const syncDB = async (
    kafkaClient: Kafka,
    redisClient: ReturnType<typeof createClient>,
    topic: string,
    partitionNumber: number,
    snapshotTopic: string,
    partitionControlKey: string,
)=>{
    console.log('Syncing DB', topic, partitionNumber)
    const partitionControl:PartitionControl =  JSON.parse(await redisClient.get(partitionControlKey)) || { snapshotOffset: 0 }
    const fromOffset = partitionControl.snapshotOffset - 1
    let toOffset: number | undefined = undefined
    const deferredPromise =  createDeferredPromise()
    const consumer = kafkaClient.consumer({groupId: `sync-consumer-${topic}-${partitionNumber}`, readUncommitted: false}); 
    ////TODO Fix or test uncomitted messages // 👈 no group
    await consumer.connect();
    await consumer.subscribe({ topic: snapshotTopic, fromBeginning: false });
    const admin = kafkaClient.admin();
    await admin.connect();
    const topicDetails = await admin.fetchTopicMetadata({ topics: [snapshotTopic] });
    const topicsoffsets = await admin.fetchTopicOffsets(snapshotTopic);
    await admin.disconnect();



    await consumer.run({
        eachBatch: async (payload) => {
            console.log('Batch', payload.batch.partition, partitionNumber, payload.batch.firstOffset(), payload.batch.lastOffset())
            if(payload.batch.partition !== partitionNumber) {
                return
            }
            const { batch, heartbeat } = payload;
            const states = {}
            for (const message of batch.messages) {
                const key = message.key.toString();
                const value = message.value.toString();
                states[key] = value;
            
            }
            const offsetStr = payload.batch.lastOffset()
            if(!offsetStr) {
                throw new Error(`Offset not found for partition ${partitionNumber}`)
            }
            const offset = parseInt(offsetStr, 10)
            const partitionControl: PartitionControl = {snapshotOffset: offset}
            states[partitionControlKey] = JSON.stringify(partitionControl);
            await redisClient.mSet(states) 
            console.log('comparing', offset, toOffset)
            if (toOffset && offset >= toOffset) {
                deferredPromise.resolve()
                console.log('Syncing DB DONE Batch', topic, partitionNumber, fromOffset, toOffset)
                await consumer.disconnect();
                
                
            }
            await heartbeat();
        },
    })

    for(const partition of topicDetails.topics[0].partitions) {
        const offset = topicsoffsets.find(({partition: p})=> p === partition.partitionId  )?.offset
        const offsetHigh = topicsoffsets.find(({partition: p})=> p === partition.partitionId  )?.high
        if(offset === undefined) {
            throw new Error(`Offset not found for partition ${partition.partitionId}`)
        }
        if(partition.partitionId == partitionNumber) {
            toOffset = parseInt(offset, 10)  - 1
            if(toOffset === -1 || fromOffset === toOffset){
                console.log('Topic not having any message', partition.partitionId)
                await consumer.disconnect();
                deferredPromise.resolve()
                console.log('Syncing DB DONE', topic, partitionNumber, fromOffset, toOffset)
                return
            }else {
                console.log('Topic having messages', partition.partitionId , fromOffset, toOffset, offsetHigh)
                consumer.seek({offset: fromOffset+'', topic: snapshotTopic, partition: partition.partitionId})
                
            }
        }else 
            consumer.seek({offset, topic: snapshotTopic, partition: partition.partitionId})
    }
    if(!toOffset)
        throw new Error(`Offset not found for partition ${partitionNumber}`)


    await deferredPromise.promise
    console.log('Syncing DB DONE', topic, partitionNumber, fromOffset, toOffset)
}