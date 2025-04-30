import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: 'kstate-testtt',
    brokers: ['localhost:9092'],
})

// const admin = kafka.admin()
// await admin.connect()
// await admin.deleteTopics({
//     topics: ['users', 'tasks'],
// })
// await admin.disconnect()

const producer = kafka.producer({
    maxInFlightRequests: 1,
    idempotent: true,
    transactionalId: 'kstate-producer-test',
})
const topic = 'users'

await producer.connect()

// await producer.send({
//     topic: 'users',
//     messages: new Array(10000).fill('').map(()=> ({value : '{"name":"test"}', key: 'test7'})) ,
// })

// await producer.send({
//     topic: 'users',
//     messages: new Array(10000).fill('').map(()=> ({value : '{"name":"test"}', key: 'test1'})) ,
// })

/// ----- 

// const elements = new Array(100000).fill('').map((a,index)=> ({value : JSON.stringify({"description":"test", "userKey":"test7"}), key: '6t'+index}))
// console.log('elements', elements.length)
// await Promise.all(new Array(1).fill('').map(()=> producer.send({
//     topic: 'tasks',
//     messages:  elements,
// })))

/// ----
const elements = new Array(1000).fill('').map((a,i)=> ({
    value : JSON.stringify({ type: 'add-subscription', id: i+'SOMEID' }),
    key: 'SUB16|0',
}))
console.log('elements', elements.length)
await Promise.all(new Array(1).fill('').map(()=> producer.sendBatch({
    topicMessages: [
       { topic: 'subscription-node',
        messages:  elements,}
    ],
})))

// await producer.send({
//     topic: 'subscription-node',
//     messages: new Array(100000).fill('').map((val, i)=> ( )) ,
// })

await producer.disconnect()


