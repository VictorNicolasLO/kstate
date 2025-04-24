import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: 'kstate-testtt',
    brokers: ['localhost:9092'],
})

const admin = kafka.admin()
await admin.connect()
await admin.createTopics({
    topics: [{
        topic: 'testing',
        numPartitions: 20,
        replicationFactor: 1,
        configEntries: [
            { name: 'retention.ms', value: '-1' },
            { name: 'retention.bytes', value: '-1' },
            { name: 'cleanup.policy', value: 'compact' },
        ],
    }],
})
await admin.disconnect()

const producer = kafka.producer({
    maxInFlightRequests: 1,
    idempotent: true,
    transactionalId: 'kstate-producer-test',

})


await producer.connect()

// const tx = await producer.transaction()

// const res = await tx.send({
//     topic: 'testing',
//     messages: new Array(1).fill('').map(()=> ({value : '{"name":"test"}', key: 'test'})) ,
// })
// console.log(res)
// await tx.commit()
// const tx2 = await producer.transaction()
// const res2 = await tx2.send({
//     topic: 'testing',
//     messages: new Array(1).fill('').map(()=> ({value : '{"name":"test"}', key: 'test'})) ,
// })
// console.log(res2)
// await tx2.commit()
// await producer.disconnect()

const consumer = kafka.consumer({ groupId: 'kstate-test-group-2' })
await consumer.connect()
await consumer.subscribe({ topic: 'testing', fromBeginning: true })
consumer.on('consumer.start_batch_process', (e)=>{
    console.log('Batch empty', e)
}
)
await consumer.run({
    eachBatch: async (payload) => {
        console.log('Batch', payload.batch.partition, payload.batch.firstOffset(), payload.batch.lastOffset())
        const { batch, heartbeat } = payload;
        const states = {}
        for (const message of batch.messages) {
            console.log('message', message)
            const key = message.key.toString();
            const value = message.value.toString();
            states[key] = value;
        }
        console.log('States', states)
    }
})