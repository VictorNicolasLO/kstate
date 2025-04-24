export type PartitionControl = {
    commandOffset?: number

    baseOffset: number,
    predictedNextOffset: number
    lastBatchSize: number
    
    status?: 'running' | 'done'
    lastProcessId?: string
    lastProcessTime?: number
}

export type State = {
    payload: any,
    inputOffset: number,
    previousInputOffset: number,
    previousBatchBaseOffset: number,
    snapshotOffset: number,
    previousSnapshotOffset: number,
    lastProcessId: string,
    version: number,
}