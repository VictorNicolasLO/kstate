import { createClient, RedisClientType } from "redis";
import { KafkaOptions, RedisOptions } from "./types";
import { Kafka, Producer } from "kafkajs";
import { PartitionControl } from "./reducer.types";
import { StoreAdapter } from "./stores/store-adapter";

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
    store: StoreAdapter,
    topic: string,
    partitionNumber: number,
    snapshotTopic: string,
    partitionControlKey: string,
    producer: Producer
)=>{
    console.log('Syncing DB', topic, partitionNumber)
    const partitionControl:PartitionControl =  await store.get(partitionControlKey) ?? { 
        baseOffset: 0,
        lastBatchSize: 0,
        predictedNextOffset: 0,
    }

    const fromOffset = partitionControl.baseOffset

    /// TODO Before transaction, validate fromOffset is equals to current "partition offset watermark" if so -> is up to date so return and resolve

    const tx = await producer.transaction()
    const controlSent = await tx.send({
        topic: snapshotTopic,
        messages: [
            {
                key: partitionControlKey + '_CONTROL_KEY',
                value: JSON.stringify(partitionControl),
                partition: partitionNumber,
            },
        ],
    })
    await tx.commit()
    console.log('Control sent', controlSent[0])
    if(!controlSent[0].baseOffset)
        throw new Error(`Offset not found for partition ${partitionNumber}`)
    const toOffset: number  = parseInt(controlSent[0].baseOffset, 10) 
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
               if(message.key === undefined || message.key === null) {
                    console.log('Message without key', message)
                    continue
                }
                if(message.value === undefined || message.value === null) {
                    console.log('Message without value', message)
                    continue
                }
                const key = message.key.toString();
                const value = message.value.toString();
                states[key] = value;
                if(message.offset === toOffset + '') {
                    console.log('Offset found', partitionNumber, message.offset)
                    console.log('Syncing DB DONE Batch', topic, partitionNumber, fromOffset, toOffset)
                    const partitionControl: PartitionControl = {
                        baseOffset: toOffset, 
                        lastBatchSize: 1, 
                        predictedNextOffset: toOffset + 1 + 1 // offset + batchSize + commit mark 
                    }
                    states[partitionControlKey] = JSON.stringify(partitionControl);
                    await store.setManyRaw(states)
                    console.log('mset done', topic, partitionNumber)
                    consumer.stop().then(()=> consumer.disconnect());
                    console.log('Syncing DB resolved', topic, partitionNumber)
                    deferredPromise.resolve()
                    return
                }
            }
            const offsetStr = batch.messages[batch.messages.length - 1].offset
            if(!offsetStr) {
                throw new Error(`Offset not found for partition ${partitionNumber}`)
            }
            const offset = parseInt(offsetStr, 10)
            const partitionControl: PartitionControl = {
                baseOffset: offset, 
                lastBatchSize: 0, 
                predictedNextOffset: offset 
            }
            states[partitionControlKey] = JSON.stringify(partitionControl);
            await store.setManyRaw(states)
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
            console.log('Topic having messages {partitionid, fromOffset, toOffset, offsetHigh}', partition.partitionId , fromOffset, toOffset, offsetHigh)
            consumer.seek({offset: fromOffset +'', topic: snapshotTopic, partition: partition.partitionId})
        }else 
            consumer.seek({offset, topic: snapshotTopic, partition: partition.partitionId})
    }


    await deferredPromise.promise
    console.log('Syncing DB DONE', topic, partitionNumber, fromOffset, toOffset)
}