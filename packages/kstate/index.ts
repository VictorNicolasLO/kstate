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

export const createKState = (
    store: StoreAdapter,
    kafkaCLient: Kafka
) => {
    return {
        fromTopic: fromTopic(store, kafkaCLient),
    }
}


