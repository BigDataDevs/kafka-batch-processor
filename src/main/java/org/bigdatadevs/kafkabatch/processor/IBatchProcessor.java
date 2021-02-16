/**
  * @author marinapopova
  * May 2, 2016
 */
package org.bigdatadevs.kafkabatch.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface IBatchProcessor {

	/**
	 * Process one message from Kafka - do parsing/ enrichment/ transformations/ call other services
	 * or whatever other logic is required;
	 *
	 * @param currentKafkaRecord
	 * @param consumerId ID of the consumer thread processing this message
	 * @return boolean: TRUE if message is processed successfully; FALSE - otherwise
	 * @throws Exception
	 */
    boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord, int consumerId) throws Exception;
    
    /**
     * callback method - called after each poll() request to Kafka brokers is done
     * but before any messages from the retrieved batch start to get processed;
     * can be used for additional logging; usually is NO-OP
     * 
     * @param consumerId
     * @throws Exception
     */
    default void onPollBeginCallBack(int consumerId) throws Exception {
    	// NO-OP
	};

    /**
     * callback method - called after all events from the last poll() were processed AND before the 
     * offsets for this poll() are about to be committed;
     * returns a flag indicating whether offsets for this poll() should be committed or not;
     * This allows to control how many polls each batch consists of;
     * Default is: 1 poll == 1 batch ==> offsets are committed (on successful processing)
     *
     * Returning TRUE from this method means that the offsets from the last poll()
     * (and any other previously un-committed polls) will be committed;
     * Returning FALSE means that the offsets will NOT be committed and the batch consumer will proceed
	 * with calling the next poll()
	 *
     * @param consumerId
     * @param pollEndPosition
     * @return boolean shouldCommitCurrentOffsets
     * @throws Exception
	 *
	 * TODO add BatchNonRecoverableException and modfy signature to throw only BatchRecoverableException or BatchNonRecoverableException
     */
	boolean completePoll(int consumerId, Map<TopicPartition, OffsetAndMetadata> pollEndPosition)
			throws BatchRecoverableException, Exception;

	/**
	 * This method is called when the ConsumerWorker instance that uses this Processor is started -
	 * right before the poll() calling loop starts;
	 * This can be used to initialize any resources that this instance of the
	 * BatchProcessor is using; for example: connections to DBs, caches, etc.
	 * and/or initialize/ publish/ log any batch-specific statistics if needed;
	 * by default - it is a NOOP method
 	 */
	default void onStartup(int consumerId) throws Exception {
		// NO-OP
	};

	/**
	 * This method is called when the ConsumerWorker instance that uses this Processor
	 * has finished its poll() calling loop  - before the consumer shuts down.
	 * This can be used to cleanup any resources that this instance of the
	 * BatchProcessor is using; for example: connections to DBs, caches, etc.
	 * and/or publish/log any batch-specific statistics if needed;
	 * by default - it is a NOOP method
	 */
	default void onShutdown(int consumerId) {
		// NO-OP
	};

}
