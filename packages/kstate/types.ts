import { KafkaConfig } from "kafkajs"
import { RedisClientOptions } from "redis"

export type RedisOptions = {
    client: RedisClientOptions,
    prefix?: string
}
export type KafkaOptions = {
    client: KafkaConfig,
    prefix?: string
}

export type ReducerResponse<T> = {
    state: T | undefined,
    reactions: {
        topic: string,
        key: string,
        message: any
    }[]
}

export type ReducerCtx = {
    topic: string,
    partition: number,
    offset: number,
}


export type ReducerCb<T> = ( message: any, key:string, state: T | undefined, ctx: ReducerCtx) => ReducerResponse<T>

export type TopicConfig<T> = {
    reduce: (cb: ReducerCb<T>) => void
}