export type StoreAdapter = {
    setMany: (kv: any) => Promise<void>
    setManyRaw: (kv: any) => Promise<void>
    getMany: (keys: string[]) => Promise<(any| null)[]>
    connect: () => Promise<void>
    disconnect: () => Promise<void>
    get: (key: string) => Promise<any | undefined>
}