export type PartitionControl = {
    commandOffset?: number
    snapshotOffset: number
    status?: 'running' | 'done'
    lastProcessId?: string
    lastProcessTime?: number
}

export type State = {
    payload: any,
    inputOffset: number,
    previousInputOffset: number,
    snapshotOffset: number,
    previousSnapshotOffset: number,
    lastProcessId: string,
    version: number,
}