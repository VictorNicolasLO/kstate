import { EachBatchPayload, Producer, TopicMessages } from "kafkajs"
import { createClient, RedisClientType } from "redis"
import { ReducerCb } from "./types"
import { PartitionControl, State } from "./reducer.types"


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
    const { batch, heartbeat } = payload
    console.log('Batch', batch.topic, batch.partition, batch.messages.length, )
    const { messages } = batch
    const partition = batch.partition
    const partitionControlKey = getPartitionControlKey(partition)
    const topicPrefix = `${topic}-`
    const producer = producers.get(partition)
    if (!producer)
        throw new Error(`No producer found for partition ${partition}`)
    
    const tx = await producer.transaction()
    const nextStates = {}
    try {
        const keySets: string[] = [partitionControlKey]
        const messageGroups: { [x: string]: { offset: string, msg: any }[] } = {}
        for (const message of messages) {
            const key = message.key ?  message.key.toString() : 'UNDEFINED'
            const value =  message.value ? JSON.parse(message.value.toString())  : {}
            if (messageGroups[key]) {
                messageGroups[key].push({ offset: message.offset, msg: value })
            } else {
                keySets.push(key) // TODO: PUT PREFIXES
                messageGroups[key] = [{ offset: message.offset, msg: value }]
            }
        }

        const states = await redisClient.mGet(keySets) // TODO ensure this is atomic -> if not use head and tail method
        await heartbeat()
        const reactions: TopicMessages[] = [{ topic: snapshotTopic, messages: [] }]
        const existentTopics = {}
        


        const lastPartitionControl:PartitionControl = states[0] ?  JSON.parse(states[0])  : { 
            baseOffset: 0,
            lastBatchSize: 0,
            predictedNextOffset: 0,
         }
        
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
                    /// TODO Make backwards recovery, use previousBatchBaseOffset in the state to start recovery from there
                const { state: newState, reactions: newReactions } = cb(message.msg, key, state.payload, {
                    topic,
                    partition,
                    offset: parseInt(message.offset, 10),
                });
                // console.log('reactions', newReactions)
                state.version = (state.version || 0) + 1;
                state.previousBatchBaseOffset = lastPartitionControl.baseOffset; //
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
        const snapshotIndex = batchResponse.findIndex(({topicName})=>topicName === snapshotTopic) 
        if(!batchResponse[snapshotIndex] || !batchResponse[snapshotIndex].baseOffset)
            throw new Error(`Batch response is empty or base offset is not defined ${JSON.stringify(batchResponse)}`)

        // TODO CHECK OFFSETS LOGIC
        const baseOffset = parseInt(batchResponse[snapshotIndex].baseOffset, 10)

        if (lastPartitionControl.predictedNextOffset !== baseOffset )
            throw new Error(`Offsets not equal, ${lastPartitionControl.predictedNextOffset} != ${baseOffset} ---- ${batchResponse[snapshotIndex].topicName}`)
        lastPartitionControl.predictedNextOffset = baseOffset + messages.length + 1 // (Commit mark)
        lastPartitionControl.baseOffset = baseOffset
        lastPartitionControl.lastBatchSize = messages.length

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
        // console.log('mset', nextStates)
        await redisClient.mSet(nextStates) // TODO COnfirm this operation is atomic completely, and lock keys
    }catch(err){
        console.error('Error setting states', err)
        await syncDB()
        throw err
    }
}