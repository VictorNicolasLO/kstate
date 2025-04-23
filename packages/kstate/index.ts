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


const fromTopic = (redisClient: ReturnType<typeof createClient>, kafkaClient: Kafka) =>
    <T>(topic: string, topicOptions?: any) => ({
        reduce: (cb: ReducerCb<T>) => startReducer(cb, kafkaClient, redisClient, topic)
    })

export const createRedisKState = (
    redisOptions: RedisOptions,
    kafkaOptions: KafkaOptions
) => {
    const redisClient = createClient(redisOptions.client)
    const kafkaClient = new Kafka(kafkaOptions.client)
    return {
        fromTopic: fromTopic(redisClient, kafkaClient),
    }
}


