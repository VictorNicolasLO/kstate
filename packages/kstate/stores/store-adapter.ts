// TODO: enable to load multiple stores like getStore(storeName: string)    

export type Store = {
    setMany: (topickv: any) => Promise<void>
    setManyRaw: (kv: any) => Promise<void>
    getMany: (keys: string[]) => Promise<(any| null)[]>
    connect: () => Promise<void>
    disconnect: () => Promise<void>
    get: (key: string) => Promise<any | undefined>
}

export type StoreAdapter = {
    getStore: (topic: string, partition: number) => Store
}