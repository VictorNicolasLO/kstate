import { createKState } from '../kstate'
import { createHash } from 'node:crypto'

import { Kafka } from 'kafkajs'
import { createClient } from 'redis'
import { createRedisStore } from '../kstate/stores/redis-store/redis-store'
import { createInMemoryStore } from '../kstate/stores/in-memory-store/in-memory-store'


const kafka = new Kafka({
    clientId: 'kstate',
    brokers: ['localhost:9092'],
    logLevel: 2,
})

const admin = kafka.admin()
const topicConfig = [
    { name: 'retention.ms', value: '-1' },
    { name: 'retention.bytes', value: '-1' },
]
await admin.connect()
await admin.createTopics({
    waitForLeaders: true,
    topics: [
        {
            topic: 'users',
            numPartitions: 20,
            replicationFactor: 1,
            configEntries: topicConfig
        },
        {
            topic: 'tasks',
            numPartitions: 20,
            replicationFactor: 1,
            configEntries: topicConfig
        },
        {
            topic: 'subscription-node',
            numPartitions: 10,
            replicationFactor: 1,
            configEntries: topicConfig
        }
    ]
})
await admin.disconnect()

const redis = createClient({
    url: 'redis://localhost:6379',
})
const store = createInMemoryStore()
await store.connect()
const kstate = createKState(
    store,
    kafka
)


// kstate
//     .fromTopic<{ name: string, tasks: number }>('users')
//     .reduce((message, key, state) => {
//         // console.log('message', message)
//         // console.log('user key', key)
//         // console.log('state', state)
//         if (!state) {
//             state = {
//                 name: message.name,
//                 tasks: 0
//             }
//             return {
//                 state: state,
//                 reactions: []
//             }
//         }
//         else {
//             state.tasks = state.tasks + 1
//             return {
//                 state,
//                 reactions: []
//             }
//         }



//     })


// kstate
//     .fromTopic<{ description: string, userKey: string }>('tasks')
//     .reduce((message, key, state) => {
//         // console.log('message', message)
//         // console.log('key', key)
//         // console.log('state', state)
//         if (!state) {
//             state = {
//                 description: message.description,
//                 userKey: message.userKey
//             }
//             return {
//                 state: state,
//                 reactions: [
//                     {
//                         topic: 'users',
//                         key: message.userKey,
//                         message: {
//                             name: message.description,
//                             tasks: 1
//                         }
//                     }
//                 ]
//             }
//         }
//         else
//             return {
//                 state,
//                 reactions: []
//             }
//     })





export type SubscriptionMessage =
    { type: 'add-subscription', id: string } |
    { type: 'remove-subscription', id: string } |
    { type: 'fanout', resourceId: string }


export type SubscriptionState = undefined |
{ type: 'router', partitions: Map<number, number> } |
{ type: 'fanner', subcriptionList: string[] }

const SUBSCRIPTION_NODE_LIMIT = 10



function getPartition(stringId: string, N: number, salt:string = '') {
    // Create an MD5 hash of the stringId
    const hash = createHash('md5').update(stringId + salt).digest('hex');
    // Convert the hex hash to an integer
    const intHash = BigInt('0x' + hash);
    // Get the partition number
    const partition = Number(intHash % BigInt(N));
    return partition;
}

kstate
    .fromTopic<SubscriptionState>('subscription-node')
    .reduce((message: SubscriptionMessage, key, state: SubscriptionState) => {
        // console.log('message', message)
        // console.log('key', key)
        // console.log('state', state)
        const path = key.split('|')[1].split('.').map(Number) // [0, 1, 2]
        const topicId = key.split('|')[0]
        const messageType = message.type
        if(state && state.type === 'fanner'){
            state.subcriptionList = state.subcriptionList.map((v)=>v || '')
        }

        if (messageType === 'add-subscription') {
            const subscriptionId = message.id
            if (!state) {
                state = {
                    type: 'fanner',
                    subcriptionList: [subscriptionId],
                }
                return {
                    state: state,
                    reactions: []
                }
            }
            if (state.type === 'fanner') {
                if (state.subcriptionList.includes(subscriptionId)) {
                    return {
                        state,
                        reactions: []
                    }
                }
                if (state.subcriptionList.length <= SUBSCRIPTION_NODE_LIMIT) {
                    state.subcriptionList.push(subscriptionId)
                    return {
                        state,
                        reactions: []
                    }
                } else {
                    const partitions = new Map<number, number>()
                    const reactions: any[] = []
                    for (let i = 0; i < state.subcriptionList.length; i++) {
                        const sub = state.subcriptionList[i]
                        const partitionId = getPartition(state.subcriptionList[i], SUBSCRIPTION_NODE_LIMIT, path.join('.'))
                        const partitionCount = partitions.get(partitionId)
                        if (partitionCount === undefined) {
                            partitions.set(partitionId, 1)
                        } else {
                            partitions.set(partitionId, partitionCount + 1)
                        }
                        const partitionPath = path.concat(partitionId)
                        reactions.push(
                            {
                                topic: 'subscription-node',
                                key: `${topicId}|${partitionPath.join('.')}`,
                                message: {
                                    type: 'add-subscription',
                                    id: sub
                                }
                            }
                        )
                    }

                    const nextState: SubscriptionState = {
                        type: 'router',
                        partitions
                    }

                    return {
                        state: nextState,
                        reactions
                    }
                }
            }
            if (state.type === 'router') {
                const partitions = new Map<number, number>(Object.entries(state.partitions) as any)
                const partitionId = getPartition(subscriptionId, SUBSCRIPTION_NODE_LIMIT , path.join('.'))
                const partitionCount = partitions.get(partitionId)
                if (partitionCount === undefined) {
                    partitions.set(partitionId, 1)
                } else {
                    partitions.set(partitionId, partitionCount + 1)
                }
                return {
                    state: {...state, partitions},
                    reactions: [{
                        topic: 'subscription-node',
                        key: `${topicId}|${path.concat(partitionId).join('.')}`,
                        message: {
                            type: 'add-subscription',
                            id: subscriptionId,
                        }
                    }]
                }
            }
        }
        if (messageType === 'remove-subscription') {
            const subscriptionId = message.id
            if (!state) {
                return {
                    state,
                    reactions: []
                }
            }
            if (state.type === 'fanner') {
                const index = state.subcriptionList.indexOf(subscriptionId)
                if (index !== -1) {
                    state.subcriptionList.splice(index, 1)
                }
                return {
                    state,
                    reactions: []
                }
            }
            if (state.type === 'router') {
                const partitions = new Map<number, number>(Object.entries(state.partitions) as any)
                const partitionId = getPartition(subscriptionId, SUBSCRIPTION_NODE_LIMIT, path.join('.'))
                const partitionCount = partitions.get(partitionId)
                if (partitionCount === undefined) {
                    return {
                        state,
                        reactions: []
                    }
                } else {
                    if (partitionCount === 1) {
                        partitions.delete(partitionId)
                    } else {
                        partitions.set(partitionId, partitionCount - 1)
                    }
                    return {
                        state: {...state, partitions},
                        reactions: [{
                            topic: 'subscription-node',
                            key: `${topicId}|${path.concat(partitionId).join('.')}`,
                            message: {
                                type: 'remove-subscription',
                                id: subscriptionId,
                            }
                        }]
                    }
                }
            }
        }

        return {
            state,
            reactions: []
        }

    })