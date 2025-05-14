import { createClient } from "redis"
import { Store, StoreAdapter } from "../store-adapter"

let connected = false

const getRedisStore = (
    redisClient: ReturnType<typeof createClient>,
    topic: string,
    _partition: number
): Store => {
    const prefix = `${topic}/`
    return {
        setMany: async (kv: any) => {
            const newKv: any = {}
            for (const key in kv) {
                newKv[prefix + key] = JSON.stringify(kv[key])
            }
            void await redisClient.mSet(newKv)
        },
        getMany: async (keys: string[]) => {
            const values = await redisClient.mGet(keys.map((key) => prefix + key))
            return values.map((v: string | null)=> v ? JSON.parse(v) : null)
        },
        connect: async () => {
            if (connected) return
            await redisClient.connect()
        } ,
        disconnect: async () => {},
        get: async (key: string) => {
            const value = await redisClient.get(prefix + key)
            if (!value) return undefined
            return JSON.parse(value)
        },
        setManyRaw: async (kv: any) => {
            const newKv: any = {}
            for (const key in kv) {
                newKv[prefix + key] = kv[key]
            }
            void await redisClient.mSet(newKv)
        }
    }
}

export const createRedisStore = (
    redisClient: ReturnType<typeof createClient>,
): StoreAdapter => {
    return {
        getStore: (topic:string, partition: number)=> getRedisStore(redisClient, topic, partition),
    }
}