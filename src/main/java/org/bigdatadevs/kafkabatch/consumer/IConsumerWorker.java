/**
  * @author marinapopova
  * Aug 2, 2019
 */
package org.bigdatadevs.kafkabatch.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface IConsumerWorker extends Runnable {

    /**
     * this method can be overwritten (implemented) in a custom implementation of the ConsumerWorker
     * if you want to expose offsets as custom JMX metrics or store them in some external storage/DB
     * @param previousPollEndPosition
     */
    public void exposeOffsetPosition(Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition);
        
    /**
     * Creates Kafka properties and an instance of a KafkaConsumer
     * 
     * @param consumerInstanceId
     */
    public void initConsumerInstance(int consumerInstanceId);
    
}