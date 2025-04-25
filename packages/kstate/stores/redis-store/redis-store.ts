import { createClient } from "redis"

export const redisStore = (
    redisClient: ReturnType<typeof createClient>,
) => {
    return {
        setMany: async (kv: any) => {
            for (const key in kv) {
                kv[key] = JSON.stringify(kv[key])
            }
            return await redisClient.mSet(kv)
        },
        getMany: async (keys: string[]) => {
            const values = await redisClient.mGet(keys)
            return values.map((v: string)=> JSON.parse(v))
        },
        connect: async () => redisClient.connect(),
        disconnect: async () => redisClient.disconnect(),
        get: async (key: string) => redisClient.get(key),
    }
}

