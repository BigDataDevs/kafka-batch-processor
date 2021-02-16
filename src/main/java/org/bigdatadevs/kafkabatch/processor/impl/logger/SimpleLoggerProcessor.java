package org.bigdatadevs.kafkabatch.processor.impl.logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.bigdatadevs.kafkabatch.processor.IBatchProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Simple implementation of the IBatchProcessor interface:
 * logs information about each message and poll it processes;
 * does not work with the actual messages;
 * returns TRUE from the completePoll() method to commit offsets of the last processed poll
 */
public class SimpleLoggerProcessor implements IBatchProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SimpleLoggerProcessor.class);

    @Override
    public boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord, int consumerId) throws Exception {
        logger.info("{}: processing message: topic: {}, partition: {}, headers: {}, timestamp: {}, key: {}, payload: {}",
                consumerId, currentKafkaRecord.topic(), currentKafkaRecord.partition(), currentKafkaRecord.headers().toString(),
                currentKafkaRecord.timestamp(), currentKafkaRecord.key(), currentKafkaRecord.value());
        return true;
    }

    @Override
    public boolean completePoll(int consumerId, Map<TopicPartition, OffsetAndMetadata> pollEndPosition) throws Exception {
        logger.info("{}: onPollEndCallBack(): processing end of poll - returning TRUE to commit offsets:", consumerId);
        pollEndPosition.forEach((topicPartition, offsetAndMetadata) -> {
            logger.info("onPollEndCallBack(): offset position to commit: consumerId: {}, topic: {}, partition: {}, offset: {}",
                    consumerId, topicPartition.topic(), topicPartition.partition(), offsetAndMetadata.offset());
        });
        return true;
    }
}
