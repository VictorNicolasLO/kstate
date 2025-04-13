import { RedisClientType } from "redis";
import { KafkaOptions, RedisOptions } from "./types";
import { Kafka } from "kafkajs";

export const syncDB = async (
    redisOptions: RedisOptions,
    kafkaOptions: KafkaOptions,
    kafkaClient: Kafka,
    redisClient: RedisClientType,
    topic: string,
    partitionNumber: number,
    offsetNumber: number,
    snapshotTopic: string,
)=>{
    const consumer = kafkaClient.consumer({groupId: `sync-consumer-${topic}-${partitionNumber}`}); // 👈 no group
    const admin = kafkaClient.admin();
    await admin.connect();
    const topicDetails = await admin.fetchTopicMetadata({ topics: [snapshotTopic] });
    const topicsoffsets = await admin.fetchTopicOffsets(snapshotTopic);

    for(const partition of topicDetails.topics[0].partitions) {
        
        if(partition.partitionId == partitionNumber) {
            consumer.seek({offset: offsetNumber+'', topic: snapshotTopic, partition: partition.partitionId})
        }
        const offset = topicsoffsets.find(({partition: p})=> p === partition.partitionId  )?.offset
        if(!offset) {
            throw new Error(`Offset not found for partition ${partition.partitionId}`)
        }
        consumer.seek({offset, topic: snapshotTopic, partition: partition.partitionId})
    }
    await consumer.connect();
    await consumer.run({
        
    })
    consumer.on('message', async (message) => {
        const { topic, partition, offset } = message;
        const key = message.key.toString();
        const value = message.value.toString();
        const snapshotKey = `${redisOptions.prefix}:${topic}:${partition}:${key}`;
        await redisClient.set(snapshotKey, value);
        console.log(`Snapshot saved for ${snapshotKey}`);
    });
}

export const syncDBBackwards = async (
    redisOptions: RedisOptions,
    kafkaOptions: KafkaOptions,
    kafkaClient: Kafka,
    redisClient: RedisClientType,
    topic: string,
    partition: number,
    offset: number,
    snapshotTopic: string,
)=>{
    
}