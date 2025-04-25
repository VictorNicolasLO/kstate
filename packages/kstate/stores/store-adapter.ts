export type StoreAdapter = {
    setMany: (kv: any) => Promise<void>
    getMany: (keys: string[]) => Promise<any[]>
    connect: () => Promise<void>
    disconnect: () => Promise<void>
}