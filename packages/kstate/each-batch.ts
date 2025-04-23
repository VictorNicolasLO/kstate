import { EachBatchPayload, Producer, TopicMessages } from "kafkajs"
import { createClient, RedisClientType } from "redis"
import { ReducerCb } from "./types"
import { PartitionControl, State } from "./reducer.types"
import { syncDB } from "./sync-db"

export const eachBatch = async <T>(
    producers: Map<number, Producer>, 
    getPartitionControlKey: (partition: number) => string,
    snapshotTopic: string,
    groupId: string,
    topic: string,
    redisClient: ReturnType<typeof createClient>,
    cb: ReducerCb<T>,
    payload:EachBatchPayload,
    syncDB: ()=> Promise<void>,
) => {
    const { batch, resolveOffset, heartbeat } = payload
    const { messages } = batch
    const partition = batch.partition
    const partitionControlKey = getPartitionControlKey(partition)

    const producer = producers.get(partition)
    if (!producer)
        throw new Error(`No producer found for partition ${partition}`)
    
    const tx = await producer.transaction()
    const nextStates = {}
    try {
        const keySets: string[] = [partitionControlKey]
        const messageGroups: { [x: string]: { offset: string, msg: any }[] } = {}
        for (const message of messages) {
            const key = message.key.toString()
            const value = message.value.toString()
            if (messageGroups[key]) {
                messageGroups[key].push({ offset: message.offset, msg: value })
            } else {
                keySets.push(key)
                messageGroups[key] = [{ offset: message.offset, msg: value }]
            }
        }

        const states = await redisClient.mGet(keySets) // TODO ensure this is atomic -> if not use head and tail method
        await heartbeat()
        const reactions: TopicMessages[] = [{ topic: snapshotTopic, messages: [] }]
        const existentTopics = {}
        


        const lastPartitionControl:PartitionControl = states[0] ?  JSON.parse(states[0])  : { snapshotOffset: -1 }
        
        // if (lastOffsetinDb !== offsetsMap.get(partition))
        //     throw new Error(`Offsets not equal, ${lastOffset} != ${offsetsMap.get(partition)}`)
            /// TODO Make recovery

        for (const i in keySets) {

            let state: State = states[i] ? JSON.parse(states[i]) : {  }
            const key = keySets[i]
            if (key === partitionControlKey) {
                continue
            }
            const messages = messageGroups[key]
            for (const message of messages) {
                if(parseInt(message.offset, 10) <= state.inputOffset) 
                    throw new Error(`Message offset ${message.offset} is less than or equal to state offset ${state.inputOffset}`)
                    /// TODO Make backwards recovery
                const { state: newState, reactions: newReactions } = cb(message, key, state.payload, {
                    topic,
                    partition,
                    offset: parseInt(message.offset, 10),
                });

                state.version = (state.version || 0) + 1;
                state.inputOffset = parseInt(message.offset, 10);
                state.payload = newState

                for (const reaction of newReactions) {
                    if (!existentTopics[reaction.topic]) {
                        existentTopics[reaction.topic] = reactions.push({
                            topic: reaction.topic,
                            messages: [{ value: JSON.stringify(reaction.message), key: reaction.key }],
                        }) - 1
                    } else {
                        reactions[existentTopics[reaction.topic]].messages.push({
                            value: JSON.stringify(reaction.message),
                            key: reaction.key,
                        })
                    }
                }
                reactions[0].messages.push({
                    value: JSON.stringify(state),
                    key: key,
                    partition: partition,
                })
                // resolveOffset(message.offset);

            }

            nextStates[key] = JSON.stringify(state)
        }
        const batchResponse = await tx.sendBatch({
            acks: -1,
            topicMessages: reactions
        })
        if(!batchResponse[0] || !batchResponse[0].baseOffset)
            throw new Error(`Batch response is empty or base offset is not defined ${JSON.stringify(batchResponse)}`)

        // TODO C?HECK OFFSETS LOGIC
        const firstSnapshotOffsetSent = parseInt(batchResponse[0].baseOffset, 10)
        const lastSnapshotOffsetSent = firstSnapshotOffsetSent + reactions[0].messages.length - 1
        if (lastPartitionControl.snapshotOffset !== firstSnapshotOffsetSent - 1)
            throw new Error(`Offsets not equal, ${lastPartitionControl.snapshotOffset} != ${firstSnapshotOffsetSent} - 1`)
        lastPartitionControl.snapshotOffset = lastSnapshotOffsetSent
        nextStates[partitionControlKey] = JSON.stringify(lastPartitionControl)  // TODO Confirm this operations goes to the end
        await heartbeat()
        await tx.sendOffsets({
            consumerGroupId: groupId,
            topics: [{
                topic: batch.topic,
                partitions: [{
                    partition: batch.partition,
                    offset: (Number(batch.lastOffset()) + 1).toString(),
                }],
            }],
        });
        await tx.commit()
        
        
    
    } catch (err) {
        console.error('Transaction failed, aborting:', err);
        await tx.abort();
        await syncDB()
        throw err
    }

    try{
        console.log('mset', nextStates)
        await redisClient.mSet(nextStates)
    }catch(err){
        console.error('Error setting states', err)
        await syncDB()
        throw err
    }
}