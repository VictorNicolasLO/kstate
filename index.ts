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
await producer.send({
    topic: 'users',
    messages: new Array(1).fill('').map(()=> ({value : '{"name":"test"}', key: 'test'})) ,
})
await producer.disconnect()