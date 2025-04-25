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
//     messages: new Array(1).fill('').map(()=> ({value : '{"name":"test"}', key: 'test7'})) ,
// })

/// ----- 

const elements = new Array(100000).fill('').map((a,index)=> ({value : JSON.stringify({"description":"test", "userKey":"test7"}), key: '5t'+index}))
console.log('elements', elements.length)
await Promise.all(new Array(1).fill('').map(()=> producer.send({
    topic: 'tasks',
    messages:  elements,
})))




await producer.disconnect()