/**
 * @author marinapopova Apr 13, 2016
 */

package org.bigdatadevs.kafkabatch.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bigdatadevs.kafkabatch.CommonKafkaUtils;
import org.bigdatadevs.kafkabatch.FailedEventsLogger;
import org.bigdatadevs.kafkabatch.processor.BatchRecoverableException;
import org.bigdatadevs.kafkabatch.processor.IBatchProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerWorker implements AutoCloseable, IConsumerWorker {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    @Value("${kafka.consumer.poll.retry.limit:5}")
    private int pollRetryLimit;
    @Value("${kafka.consumer.poll.retry.delay.interval.ms:1000}")
    private long pollRetryIntervalMs;
    @Value("${kafka.consumer.ignore.overlimit.recoverable.errors:false}")
    private boolean ignoreOverlimitRecoverableErrors;
    @Value("${kafka.consumer.source.topic:testTopic}")
    private String kafkaTopic;
    @Value("${application.id:app1}")
    private String consumerInstanceName;
    // interval in MS to poll Kafka brokers for messages, in case there were no messages during the previous interval
    @Value("${kafka.consumer.poll.interval.ms:10000}")
    private long pollIntervalMs;
    @Value("${kafka.consumer.property.prefix:consumer.kafka.property.}")
    private String consumerKafkaPropertyPrefix;
    @Resource(name = "applicationProperties")
    private Properties applicationProperties;

    private OffsetLoggingCallbackImpl offsetLoggingCallback;
    private IBatchProcessor batchMessageProcessor;
    
    private Consumer<String, String> consumer;
    private AtomicBoolean running = new AtomicBoolean(false);
    private int consumerInstanceId;

    public ConsumerWorker() {        
    }
    
    @Override
    public void initConsumerInstance(int consumerInstanceId) {
        logger.info("init() is starting ....");
        this.consumerInstanceId = consumerInstanceId;
        Properties kafkaProperties = CommonKafkaUtils.extractKafkaProperties(applicationProperties, consumerKafkaPropertyPrefix);
        // add non-configurable properties
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        String consumerClientId = consumerInstanceName + "-" + consumerInstanceId;
        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(kafkaProperties);
        logger.info(
            "Created ConsumerWorker with properties: consumerClientId={}, consumerInstanceName={}, kafkaTopic={}, kafkaProperties={}",
            consumerClientId, consumerInstanceName, kafkaTopic, kafkaProperties);        
    }
    
    @Override
    public void run() {
        running.set(true);
        try {
            logger.info("Starting ConsumerWorker, consumerInstanceId={}", consumerInstanceId);
            consumer.subscribe(Arrays.asList(kafkaTopic), offsetLoggingCallback);
            batchMessageProcessor.onStartup(consumerInstanceId);
            while (running.get()) {
                processPoll();
            }
        } catch (WakeupException e) {
            logger.warn("ConsumerWorker [consumerInstanceId={}] got WakeupException - exiting ...", consumerInstanceId, e);
            // ignore for shutdown
        } catch (Throwable e) {
            logger.error("ConsumerWorker [consumerInstanceId={}] got Throwable Exception - will exit ...", consumerInstanceId, e);
            throw new RuntimeException(e);
        } finally {
            logger.warn("ConsumerWorker [consumerInstanceId={}] is shutting down ...", consumerInstanceId);
            offsetLoggingCallback.getPartitionOffsetMap()
                .forEach((topicPartition, offset)
                     -> logger.info("Offset position during the shutdown for consumerInstanceId : {}, partition : {}, offset : {}",
                     consumerInstanceId, topicPartition.partition(), offset.offset()));
            batchMessageProcessor.onShutdown(consumerInstanceId);
            consumer.close();
        }
    }

	/**
	 * perform one call to Kafka consumer's poll() and process all events from this poll
	 * @throws Exception
	 */
	public void processPoll() throws Exception {
		logger.debug("consumerInstanceId={}; about to call consumer.poll() ...", consumerInstanceId);
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
		batchMessageProcessor.onPollBeginCallBack(consumerInstanceId);
		boolean isPollFirstRecord = true;
		int numProcessedMessages = 0;
		int numFailedMessages = 0;
		int numMessagesInBatch = 0;
		long pollStartMs = 0L;
		for (ConsumerRecord<String, String> record : records) {
		    numMessagesInBatch++;
		    logger.debug("consumerInstanceId={}; received record: partition: {}, offset: {}, value: {}",
		            consumerInstanceId, record.partition(), record.offset(), record.value());
		    if (isPollFirstRecord) {
		        isPollFirstRecord = false;
		        logger.info("Start offset for partition {} in this poll : {}", record.partition(), record.offset());
		        pollStartMs = System.currentTimeMillis();
		    }
		    try {
		        boolean processedOK = batchMessageProcessor.processMessage(record, consumerInstanceId);
		        if (processedOK) {
		            numProcessedMessages++;
		        } else {
		            FailedEventsLogger.logFailedEvent("Failed to process event: ", record.value(), record.offset());
		            numFailedMessages++;                            
		        }
		    } catch (Exception e) {
		        FailedEventsLogger.logFailedEventWithException(e.getMessage(), record.value(), record.offset(), e);
		        numFailedMessages++;
		    }
		}
		long endOfPollLoopMs = System.currentTimeMillis();
	    Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition = getPreviousPollEndPosition();
	    boolean shouldCommitThisPoll = performCallbackWithRetry(records, previousPollEndPosition);
	    long afterProcessorCallbacksMs = System.currentTimeMillis();
		if (numMessagesInBatch > 0) {
		    commitOffsetsIfNeeded(shouldCommitThisPoll, previousPollEndPosition);
		    long afterOffsetsCommitMs = System.currentTimeMillis();
		    exposeOffsetPosition(previousPollEndPosition);
		    logger.info(
		        "Last poll snapshot: numMessagesInBatch: {}, numProcessedMessages: {}, numFailedMessages: {}, " + 
		        "timeToProcessLoop: {}ms, timeInMessageProcessor: {}ms, timeToCommit: {}ms, totalPollTime: {}ms",
		        numMessagesInBatch, numProcessedMessages, numFailedMessages,
		        endOfPollLoopMs - pollStartMs,
		        afterProcessorCallbacksMs - endOfPollLoopMs,
		        afterOffsetsCommitMs - afterProcessorCallbacksMs,
		        afterOffsetsCommitMs - pollStartMs);
		} else {
		    logger.info("No messages received during this poll");
		}
	}

    /**
     * After each poll() we call batchMessageProcessor.completePoll() method to invoke provided there
     * logic to complete processing of this (and potentially others) poll and to decide
     * whether the current poll() offsetts should be committed or not.
     * If during this operation some recoverable exceptions happen - try to re-process the poll() events and
     * re-do the completePoll() operation up until the configured number of times;
     * Examples of recoverable exceptions could be: 
     * --- Intermittent Timeout exceptions from Cassandra or Postgress or any other third-party called during this operation
     *
     * In such cases, in order to function properly, batchMessageProcessor.completePoll() method implementation has to
     * throw an instance of the ConsumerRecoverableException;
     * 
     * If some other non-recoverable exceptions happen - an instance of some other Exception should be thrown out;
     * it will cause the consumer to shutdown
     * 
     * WARNING!!! it is very important to make sure that the event processing (the batchMessageProcessor.processMessage() method)
     * is IDEMPOTENT! - meaning that it can safely re-process the same events multiple times
     *
     * TODO review the need to reprocess the individual poll events -
     * maybe add an option to only re-try calling the completePoll() method
     *
     * @param records
     * @param previousPollEndPosition
     * @return
     * @throws Exception
     */
    public boolean performCallbackWithRetry(
    	ConsumerRecords<String, String> records, 
    	Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition) throws Exception {
    	boolean shouldCommitThisPoll = true;
    	int retryAttempt = 0;
 		// only catch recoverable exception and try to re-process all records from the current poll();
		// any other Exception thrown from this method will be propagated up and will cause the consumer to shutdown
		boolean keepRetrying = true;
		while (keepRetrying) {
			try {
	    		shouldCommitThisPoll = batchMessageProcessor.completePoll(consumerInstanceId, previousPollEndPosition);
	    		// no errors - exit this method
	    		keepRetrying = false;
	    	} catch (BatchRecoverableException e) {
	    		// ignore this exception - it is recoverable - if the retry limit is not reached
    			retryAttempt++;
    			if (retryAttempt > pollRetryLimit) {
    				keepRetrying = false;
    				if (ignoreOverlimitRecoverableErrors) {
    					logger.warn("FAILED to re-process poll(): {} - reached limit of retry attempts: retryAttempt = {} out of {};" + 
        						"; ignoreOverlimitRecoverableErrors=TRUE - ignoring and continuing with the next poll()", 
        						e.getMessage(), retryAttempt, pollRetryLimit);
    				} else {
    					logger.error("FAILED to re-process poll(): {} - reached limit of retry attempts: retryAttempt = {} out of {};" + 
    						" ignoreOverlimitRecoverableErrors=FALSE - will throw ConsumerNonRecoverableException and shutdown", 
    						e.getMessage(), retryAttempt, pollRetryLimit);
    					throw new ConsumerNonRecoverableException(e.getMessage() + ": after retrying failed");
    				}
    			} else {
					logger.warn("Re-trying poll() after getting ConsumerRecoverableException: {}; retryAttempt = {} out of {};" +
							" will sleep for {}ms before re-trying", 
							e.getMessage(), retryAttempt, pollRetryLimit, pollRetryIntervalMs);
					// sleep for a configured delay and try to re-process events from the last poll() again
					Thread.sleep(pollRetryIntervalMs);
	    			reprocessPollEvents(retryAttempt, records);
    			}
	    	}
		}
    	return shouldCommitThisPoll;
    }
    
    public void reprocessPollEvents(int retryAttempt, ConsumerRecords<String, String> records) {
    	int numProcessedMessages = 0;
    	int numFailedMessages = 0;
    	// do not log failed events when reprocessing
        for (ConsumerRecord<String, String> record : records) {
            try {
                boolean processedOK = batchMessageProcessor.processMessage(record, consumerInstanceId);
                if (processedOK) {
                    numProcessedMessages++;
                } else {
                    numFailedMessages++;                            
                }
            } catch (Exception e) {
                numFailedMessages++;
            }
        }
        logger.info("Poll re-processing snapshot, retryAttempt={}: numProcessedMessages: {}, numFailedMessages: {} ",
        		retryAttempt, numProcessedMessages, numFailedMessages);   	
    }
    
    /**
     * this method can be overwritten (implemented) in your own ConsumerManager 
     * if you want to expose custom JMX metrics
     * @param previousPollEndPosition
     */
    public void exposeOffsetPosition(Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition) {
        // NO OP
    }
    
    private void commitOffsetsIfNeeded(boolean shouldCommitThisPoll, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap) {
        try {
            if (shouldCommitThisPoll) {
                long commitStartTime = System.nanoTime();
                if(partitionOffsetMap == null){
                    consumer.commitAsync(offsetLoggingCallback);
                } else {
                    consumer.commitAsync(partitionOffsetMap, offsetLoggingCallback);
                }
                long commitTime = System.nanoTime() - commitStartTime;
                logger.info("Commit successful for partitions/offsets : {} in {} ns", partitionOffsetMap, commitTime);
            } else {
                logger.debug("shouldCommitThisPoll = FALSE --> not committing offsets yet");
            }
        } catch (RetriableCommitFailedException e){
            logger.error("caught RetriableCommitFailedException while committing offsets : {}; abandoning the commit", partitionOffsetMap, e);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getPreviousPollEndPosition() {
        Map<TopicPartition, OffsetAndMetadata> nextCommitableOffset = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            nextCommitableOffset.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)));
        }
        return nextCommitableOffset;
    }

    @Override
    public void close() {
        logger.warn("ConsumerWorker [consumerInstanceId={}] shutdown() is called  - will call consumer.wakeup()", consumerInstanceId);
        running.set(false);
        consumer.wakeup();
    }

    public void shutdown() {
        logger.warn("ConsumerWorker [consumerInstanceId={}] shutdown() is called  - will call consumer.wakeup()", consumerInstanceId);
        running.set(false);
        consumer.wakeup();
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return offsetLoggingCallback.getPartitionOffsetMap();
    }

    public int getConsumerInstanceId() {
        return consumerInstanceId;
    }

    public void setBatchMessageProcessor(IBatchProcessor batchMessageProcessor) {
        this.batchMessageProcessor = batchMessageProcessor;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public void setOffsetLoggingCallback(OffsetLoggingCallbackImpl offsetLoggingCallback) {
        this.offsetLoggingCallback = offsetLoggingCallback;
    }

	public int getPollRetryLimit() {
		return pollRetryLimit;
	}

	public void setPollRetryLimit(int pollRetryLimit) {
		this.pollRetryLimit = pollRetryLimit;
	}

	public long getPollRetryIntervalMs() {
		return pollRetryIntervalMs;
	}

	public void setPollRetryIntervalMs(long pollRetryIntervalMs) {
		this.pollRetryIntervalMs = pollRetryIntervalMs;
	}

	public void setConsumer(Consumer<String, String> consumer) {
		this.consumer = consumer;
	}

	public void setConsumerInstanceId(int consumerInstanceId) {
		this.consumerInstanceId = consumerInstanceId;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public void setIgnoreOverlimitRecoverableErrors(boolean ignoreOverlimitRecoverableErrors) {
		this.ignoreOverlimitRecoverableErrors = ignoreOverlimitRecoverableErrors;
	}

}
