import {
    createClient,
    RedisClientOptions,
    RedisClientType
} from 'redis'

import {
    Kafka,
    KafkaConfig
} from 'kafkajs'
import { KafkaOptions, RedisOptions, ReducerCb } from './types'
import { startReducer } from './reducer'
import { StoreAdapter } from './stores/store-adapter'
import { createRedisStore } from './stores/redis-store/redis-store'


const fromTopic = (store: StoreAdapter, kafkaClient: Kafka) =>
    <T>(topic: string, topicOptions?: any) => ({
        reduce: (cb: ReducerCb<T>) => startReducer(cb, kafkaClient, store, topic)
    })

export const createRedisKState =  async (
    redisOptions: RedisOptions,
    kafkaOptions: KafkaOptions
) => {
    const redisClient = createClient(redisOptions.client)
    await redisClient.connect()
    const redisStore = createRedisStore(redisClient)
    const kafkaClient = new Kafka(kafkaOptions.client)
    return {
        fromTopic: fromTopic(redisStore, kafkaClient),
    }
}


