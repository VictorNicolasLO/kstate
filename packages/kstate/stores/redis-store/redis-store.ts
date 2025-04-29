import { createClient } from "redis"
import { StoreAdapter } from "../store-adapter"

export const createRedisStore = (
    redisClient: ReturnType<typeof createClient>,
): StoreAdapter => {
    return {
        setMany: async (kv: any) => {
            console.log('SET MANY', kv)
            for (const key in kv) {
                kv[key] = JSON.stringify(kv[key])
            }
            void await redisClient.mSet(kv)
        },
        getMany: async (keys: string[]) => {
            const values = await redisClient.mGet(keys)
            return values.map((v: string | null)=> v ? JSON.parse(v) : null)
        },
        connect: async () => {
            await redisClient.connect() 
        } ,
        disconnect:  () => redisClient.disconnect(),
        get: async (key: string) => {
            const value = await redisClient.get(key)
            if (!value) return undefined
            return JSON.parse(value)
        },
        setManyRaw: async (kv: any) => {
            void await redisClient.mSet(kv)
        }
    }
}

