import { createRedisKState } from '../kstate'


import { Kafka } from 'kafkajs'


const kafka = new Kafka({
    clientId: 'kstate',
    brokers: ['localhost:9092'],
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
        }
    ]
})
await admin.disconnect()



const kstate = await createRedisKState(
    {
        client: {
            url: 'redis://localhost:6379',
        },
    },
    {
        client: {
            brokers: ['localhost:9092'],
        }
    }
)


kstate
    .fromTopic<{ name: string, tasks: number }>('users')
    .reduce((message, key, state) => {
        // console.log('message', message)
        // console.log('user key', key)
        // console.log('state', state)
        if (!state) {
            state = {
                name: message.name,
                tasks: 0
            }
            return {
                state: state,
                reactions: []
            }
        }
        else{
            state.tasks = state.tasks + 1
            return {
                state,
                reactions: []
            }
        }
    
            

    })


kstate
    .fromTopic<{ description: string, userKey: string }>('tasks')
    .reduce((message, key, state) => {
        // console.log('message', message)
        // console.log('key', key)
        // console.log('state', state)
        if (!state) {
            state = {
                description: message.description,
                userKey: message.userKey
            }
            return {
                state: state,
                reactions: [
                    {
                        topic: 'users',
                        key: message.userKey,
                        message: {
                            name: message.description,
                            tasks: 1
                        }
                    }
                ]
            }
        }
        else
            return {
                state,
                reactions: []
            }
    })