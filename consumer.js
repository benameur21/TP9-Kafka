const { Kafka } = require('kafkajs');
const kafka = new Kafka({
 clientId: 'tp6-consumer',
 brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'test-group' });
const topic = process.env.KAFKA_TOPIC || 'test-topic';
const run = async () => {
 await consumer.connect();
 await consumer.subscribe({ topic, fromBeginning: true });
 await consumer.run({
 eachMessage: async ({ topic, partition, message }) => {
 const value = message.value?.toString();
 console.log({ topic, partition, offset: message.offset,
 key: message.key?.toString(), value });
 },
 });
}; run().catch(console.error);