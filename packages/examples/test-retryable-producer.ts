import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: 'kstate-testtt',
    brokers: ['localhost:9092'],
})



const producer = kafka.producer({
    maxInFlightRequests: 1,
    idempotent: true,
    transactionalId: 'kstate-producer-test',
})
const topic = 'retryable-test'

await producer.connect()

producer.send({
    topic: 'retryable-testn',
    messages: new Array(1).fill('').map((a,index)=> ({value : JSON.stringify({"description":"test", "userKey":"test4"}), key: '7t'+index}))
})

producer.disconnect()