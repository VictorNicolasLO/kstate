import {
    Kafka,
} from 'kafkajs'
import {type ReducerCb } from './types'
import { startReducer } from './reducer'
import {type StoreAdapter } from './stores/store-adapter'
import { createInMemoryStore } from './stores/in-memory-store/in-memory-store'
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

export { createInMemoryStore, createRedisStore }


