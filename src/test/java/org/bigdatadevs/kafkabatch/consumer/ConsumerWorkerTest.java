/**
 * @author marinapopova Sep 27, 2019
 */
package org.bigdatadevs.kafkabatch.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.bigdatadevs.kafkabatch.processor.BatchRecoverableException;
import org.bigdatadevs.kafkabatch.processor.IBatchProcessor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ConsumerWorkerTest {

	private IBatchProcessor mockedBatchMessageProcessor = Mockito.mock(IBatchProcessor.class);
	private MockConsumer<String, String> mockedConsumer;
	private ConsumerWorker consumerWorker = new ConsumerWorker();
	private Long startOffset = 1L;
	private String testTopic = "test-topic";
	private int partition = 0;
	private int consumerInstanceId  = 1;
	private int numberOfRecords = 2;
	private TopicPartition topicPartition0 = new TopicPartition(testTopic, partition);
	private List<ConsumerRecord<String, String>> testRecords;
    
	@BeforeEach
	public void setUp() throws Exception {
		mockedConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
		mockedConsumer.assign(Arrays.asList(topicPartition0));
		HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
	    beginningOffsets.put(topicPartition0, startOffset);
	    mockedConsumer.updateBeginningOffsets(beginningOffsets);
	    testRecords = new LinkedList<>();
	    for (int i=1; i<=numberOfRecords; i++) {
	    	ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(
	    			testTopic, partition, startOffset+i, "test-key"+i, "test-message"+i);
	    	testRecords.add(consumerRecord);
	    	mockedConsumer.addRecord(consumerRecord);
	    }
		consumerWorker.setConsumer(mockedConsumer);
		
		consumerWorker.setConsumerInstanceId(consumerInstanceId);
		consumerWorker.setKafkaTopic(testTopic);
		consumerWorker.setBatchMessageProcessor(mockedBatchMessageProcessor);
	}

	@Test
	public void testProcessPoll_happyPath() throws Exception {
		long expectedCommittedOffset = startOffset + numberOfRecords + 1;		
		long nextToReadOffset = expectedCommittedOffset;		
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId)).thenReturn(true);
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap())).thenReturn(true);
		consumerWorker.processPoll();
		// all records should be processed fine and , thus,
		// committed and next to read offsets should be the same in this case
		assertEquals(nextToReadOffset, mockedConsumer.position(topicPartition0));
		OffsetAndMetadata committedOffsetInfo = mockedConsumer.committed(topicPartition0);
		assertNotNull(committedOffsetInfo);
		assertEquals(expectedCommittedOffset, committedOffsetInfo.offset());
	}

	/**
	 * Use case: processing of any (even all) events in the processMessage() fails, it returns FALSE - but the poll()
	 * should still commit the offsets, since events will be stored into the failed events log
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProcessPoll_eventProcessingFails() throws Exception {
		long expectedCommittedOffset = startOffset + numberOfRecords + 1;		
		long nextToReadOffset = expectedCommittedOffset;		
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId)).thenReturn(false);
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap())).thenReturn(true);
		consumerWorker.processPoll();
		// committed and next to read offsets should be the same in this case
		assertEquals(nextToReadOffset, mockedConsumer.position(topicPartition0));
		OffsetAndMetadata committedOffsetInfo = mockedConsumer.committed(topicPartition0);
		assertNotNull(committedOffsetInfo);
		assertEquals(expectedCommittedOffset, committedOffsetInfo.offset());
	}

	/**
	 * Use case: processing of any (even all) events in the processMessage() fails, it throws an Exception - but the poll()
	 * should still commit the offsets, since events will be stored into the failed events log
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProcessPoll_eventProcessingExceptions() throws Exception {
		long expectedCommittedOffset = startOffset + numberOfRecords + 1;		
		long nextToReadOffset = expectedCommittedOffset;		
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId))
			.thenThrow(new IllegalArgumentException("Unit test exception"));
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap())).thenReturn(true);
		consumerWorker.processPoll();
		// committed and next to read offsets should be the same in this case
		assertEquals(nextToReadOffset, mockedConsumer.position(topicPartition0));
		OffsetAndMetadata committedOffsetInfo = mockedConsumer.committed(topicPartition0);
		assertNotNull(committedOffsetInfo);
		assertEquals(expectedCommittedOffset, committedOffsetInfo.offset());
	}

	/**
	 * Use case: call to beforeCommitCallBack() returns shouldCommitOffset = FALSE ==>
	 * offsets should not be committed, but the execution flow should not fail
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProcessPoll_beforeCommitCallBackReturnsFalse() throws Exception {
		long nextToReadOffset = startOffset + numberOfRecords + 1;		
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId)).thenReturn(true);
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap())).thenReturn(false);
		consumerWorker.processPoll();
		// next to read offset should still be incremented
		assertEquals(nextToReadOffset, mockedConsumer.position(topicPartition0));
		// nothing should be committed for this topic/partition yet - so the committed OffsetAndMEtadata object should be null
		OffsetAndMetadata committedOffsetInfo = mockedConsumer.committed(topicPartition0);
		assertNull(committedOffsetInfo);
	}

	/**
	 * Use case: call to beforeCommitCallBack() throws unrecoverable exception ==>
	 * consumer should fail and exit
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProcessPoll_beforeCommitCall_NonrecoverableException() throws Exception {
		long nextToReadOffset = startOffset + numberOfRecords + 1;		
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId)).thenReturn(true);
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap()))
			.thenThrow(new IllegalArgumentException("non-recoverable exception from unit test"));
	    assertThrows(IllegalArgumentException.class, () ->
				consumerWorker.processPoll() );
		// exception should be thrown out
	}

	/**
	 * Use case: call to beforeCommitCallBack() throws LESS than a configured MAX limit of
	 * recoverable exceptions ==> offsets should not be committed, but the execution flow should not fail
	 * 
	 * @throws Exception
	 */
	@Test()
	public void testProcessPoll_beforeCommitCall_RecoverableException_underlimit() throws Exception {
		long expectedCommittedOffset = startOffset + numberOfRecords + 1;		
		long nextToReadOffset = expectedCommittedOffset;		
		int pollRetryLimit = 2;
		long pollRetryIntervalMs = 2l;
		consumerWorker.setPollRetryLimit(pollRetryLimit);
		consumerWorker.setPollRetryIntervalMs(pollRetryIntervalMs);
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId)).thenReturn(true);
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap()))
			.thenThrow(new BatchRecoverableException("Recoverable exception from unit test #1"))
			.thenReturn(true);
		consumerWorker.processPoll();
		// committed and next to read offsets should be the same in this case
		assertEquals(nextToReadOffset, mockedConsumer.position(topicPartition0));
		OffsetAndMetadata committedOffsetInfo = mockedConsumer.committed(topicPartition0);
		assertNotNull(committedOffsetInfo);
		assertEquals(expectedCommittedOffset, committedOffsetInfo.offset());
	}

	
	/**
	 * Use case: call to beforeCommitCallBack() throws more than a configured MAX limit of
	 * recoverable exceptions, and ignoreOverlimitRecoverableErrors = FALSE  ==> consumer should fail and exit
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProcessPoll_beforeCommitCall_RecoverableException_overlimit() throws Exception {
		int pollRetryLimit = 2;
		long pollRetryIntervalMs = 2l;
		consumerWorker.setIgnoreOverlimitRecoverableErrors(false);
		consumerWorker.setPollRetryLimit(pollRetryLimit);
		consumerWorker.setPollRetryIntervalMs(pollRetryIntervalMs);
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId)).thenReturn(true);
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap()))
			.thenThrow(new BatchRecoverableException("Recoverable exception from unit test #1"))
			.thenThrow(new BatchRecoverableException("Recoverable exception from unit test #2"))
			.thenThrow(new BatchRecoverableException("Recoverable exception from unit test #3 - over the limit"));
		// ConsumerNonRecoverableException should be thrown out
		assertThrows(ConsumerNonRecoverableException.class, () ->
				consumerWorker.processPoll() );
	}

	/**
	 * Use case: call to beforeCommitCallBack() throws more than a configured MAX limit of
	 * recoverable exceptions, and ignoreOverlimitRecoverableErrors = TRUE  ==> 
	 * offsets should not be committed, but the execution flow should not fail
	 * 
	 * @throws Exception
	 */
	@Test()
	public void testProcessPoll_beforeCommitCall_RecoverableException_overlimit_ignored() throws Exception {
		long expectedCommittedOffset = startOffset + numberOfRecords + 1;		
		long nextToReadOffset = expectedCommittedOffset;		
		int pollRetryLimit = 2;
		long pollRetryIntervalMs = 2l;
		consumerWorker.setIgnoreOverlimitRecoverableErrors(true);
		consumerWorker.setPollRetryLimit(pollRetryLimit);
		consumerWorker.setPollRetryIntervalMs(pollRetryIntervalMs);
	    for (ConsumerRecord<String, String> consumerRecord: testRecords) {
			Mockito.when(mockedBatchMessageProcessor.processMessage(consumerRecord, consumerInstanceId)).thenReturn(true);
	    }
		Mockito.when(mockedBatchMessageProcessor.completePoll(Mockito.anyInt(), Mockito.anyMap()))
			.thenThrow(new BatchRecoverableException("Recoverable exception from unit test #1"))
			.thenReturn(true);
		consumerWorker.processPoll();
		// committed and next to read offsets should be the same in this case
		assertEquals(nextToReadOffset, mockedConsumer.position(topicPartition0));
		OffsetAndMetadata committedOffsetInfo = mockedConsumer.committed(topicPartition0);
		assertNotNull(committedOffsetInfo);
		assertEquals(expectedCommittedOffset, committedOffsetInfo.offset());
	}

}
