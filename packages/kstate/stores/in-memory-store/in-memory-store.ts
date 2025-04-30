import { createClient } from "redis"
import { StoreAdapter } from "../store-adapter"

const memory = {}

export const createInMemoryStore = (): StoreAdapter => {
    return {
        setMany: async (kv: any) => {
            for (const key in kv) {
                memory[key] = kv[key]
            }
        },
        getMany: async (keys: string[]) => {
            const values = keys.map((key) => memory[key])
            return values.map((v: string | null)=> v ? v : null)
        },
        connect: async () => {
            // No connection needed for in-memory store
            console.log('In-memory store connected')
        } ,
        disconnect:  async () => {
            // No disconnection needed for in-memory store
            console.log('In-memory store disconnected')
        },
        get: async (key: string) => {
            const value = memory[key]
            if (!value) return undefined
            return value
        },
        setManyRaw: async (kv: any) => {
            for (const key in kv) {
                memory[key] = JSON.parse(kv[key])
            }
            
        }
    }
}

