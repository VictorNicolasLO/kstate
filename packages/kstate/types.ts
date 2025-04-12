import { KafkaConfig } from "kafkajs"
import { RedisClientOptions } from "redis"

export type RedisOptions = {
    client: RedisClientOptions,
    prefix: string
}
export type KafkaOptions = {
    client: KafkaConfig,
    prefix: string
}

export type ReducerResponse<T> = {
    state: T,
    reactions: {
        topic: string,
        key: string,
        message: T
    }[]
}

export type ReducerCtx = {
    topic: string,
    partition: number,
    offset: number,
}


export type reducerFn<T> = (message: any, key:string ,state?: T) => ReducerResponse<T>