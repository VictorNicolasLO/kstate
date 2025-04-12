import { Kafka } from "kafkajs";
import { KafkaOptions, RedisOptions, ReducerCb } from "./types";
import { RedisClientType } from "redis";

export const createReducer = async <T>(
    redisOptions: RedisOptions, 
    kafkaOptions: KafkaOptions, 
    cb: ReducerCb<T>,
    kafkaClient: Kafka,
    redisClient: RedisClientType
)=>{
    // Create compacted topic if not exists
    kafkaClient.admin()

    // Sync Database with topic


    

}

