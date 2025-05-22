import { createKState } from '../kstate'
import { createHash } from 'node:crypto'

import { Kafka } from 'kafkajs'
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
            topic: 'retryable-testn',
            numPartitions: 2,
            replicationFactor: 1,
            configEntries: topicConfig
        },
    ]
})
await admin.disconnect()

const store = createInMemoryStore()
const kstate = createKState(store, kafka)

const a:any = {}
kstate.fromTopic('retryable-testn').reduce((state, message) => { 
    
    console.log('message', message)
    console.log('state', state)

    a[0]['asdfadf']

    return {
        state: {},
        reactions:[]
    }
})