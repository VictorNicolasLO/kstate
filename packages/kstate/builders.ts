import { ITopicMetadata } from "kafkajs";

export const buildSnapshotTopicConfig = (snapshotTopic: string, topicDetails: {
    topics: Array<ITopicMetadata>;
}) => ( {
            topics: [
                {
                    topic: snapshotTopic,
                    numPartitions: topicDetails.topics[0].partitions.length,
                    replicationFactor: topicDetails.topics[0].partitions[0].replicas.length,
                    configEntries: [
                        { name: 'cleanup.policy', value: 'compact' },
                        { name: 'retention.ms', value: '-1' },
                        { name: 'retention.bytes', value: '-1' },
                    ]
                }
            ],
        })