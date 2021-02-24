package org.bigdatadevs.kafkabatch.consumer;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by Vitalii Cherniak on 6/8/18.
 */
public class ConsumerManagerTest {
	private static final String TOPIC = "testTopic";
	private static final ConsumerManager CONSUMER_MANAGER = Mockito.spy(ConsumerManager.class);
	private MockConsumer<String, String> mockedConsumer ;
	private static final Set<TopicPartition> PARTITIONS = new HashSet<>();
	private static final Map<TopicPartition, Long> BEGINNING_OFFSETS = new HashMap<>();
	private static final Map<TopicPartition, Long> END_OFFSETS = new HashMap<>();
	private static final long BEGINNING_OFFSET_POSITION = 0L;
	private static final long END_OFFSET_POSITION = 100000L;

	@BeforeEach
	public void setUp() {
		mockedConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
			@Override
			public synchronized void close() {
			}
		};
		CONSUMER_MANAGER.setKafkaPollIntervalMs(100L);
		CONSUMER_MANAGER.setKafkaTopic(TOPIC);
		CONSUMER_MANAGER.setConsumerCustomStartOptionsFilePath(null);

		PARTITIONS.add(new TopicPartition(TOPIC, 0));
		PARTITIONS.add(new TopicPartition(TOPIC, 1));
		PARTITIONS.add(new TopicPartition(TOPIC, 2));
		PARTITIONS.add(new TopicPartition(TOPIC, 3));
		PARTITIONS.add(new TopicPartition(TOPIC, 4));
		mockedConsumer.subscribe(Arrays.asList(TOPIC));
		PARTITIONS.forEach(topicPartition -> BEGINNING_OFFSETS.put(topicPartition, BEGINNING_OFFSET_POSITION));
		PARTITIONS.forEach(topicPartition -> END_OFFSETS.put(topicPartition, END_OFFSET_POSITION));
		mockedConsumer.updateBeginningOffsets(BEGINNING_OFFSETS);
		mockedConsumer.updateEndOffsets(END_OFFSETS);

		Mockito.doReturn(mockedConsumer).when(CONSUMER_MANAGER).getConsumerInstance(Mockito.anyObject());
	}

	// this test is no longer working after upgrading to Kafka 2.5.1 - if run with static mockedConsumer
	// Most likely consumer is not initialized anymore before calling the rebalance() for the first time;
	// TODO review and fix if needed
	//@Test
	public void testRestartOffsets() {
		Map<TopicPartition, Long> offsetBeforeSeek = new HashMap<>();
		for (TopicPartition topicPartition: PARTITIONS) {
			offsetBeforeSeek.put(topicPartition, mockedConsumer.position(topicPartition));
		}
		mockedConsumer.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.RESTART);
		for (TopicPartition topicPartition: offsetBeforeSeek.keySet()) {
			assertEquals(offsetBeforeSeek.get(topicPartition).longValue(), mockedConsumer.position(topicPartition));
		}
	}

	@Test
	public void testAllLatestOffsets() {
		mockedConsumer.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.LATEST);
		for (TopicPartition topicPartition: PARTITIONS) {
			assertEquals(END_OFFSET_POSITION, mockedConsumer.position(topicPartition));
		}
	}

	@Test
	public void testAllEarliestOffsets() {
		mockedConsumer.rebalance(PARTITIONS);
		mockedConsumer.seekToEnd(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.EARLIEST);
		for (TopicPartition topicPartition: PARTITIONS) {
			assertEquals(BEGINNING_OFFSET_POSITION, mockedConsumer.position(topicPartition));
		}
	}

	@Test
	public void testCustomOffsetsNoConfig() {
		mockedConsumer.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.CUSTOM);
		for (TopicPartition topicPartition: PARTITIONS) {
			assertEquals(BEGINNING_OFFSET_POSITION, mockedConsumer.position(topicPartition));
		}
	}

	@Test
	public void testCustomOffsetsFromFileNotEnoughPartitions() {
		//Test custom start options with not enough partitions defined, so 'RESTART' option should be used for all partitions
		CONSUMER_MANAGER.setConsumerCustomStartOptionsFilePath("src/test/resources/test-start-options-custom.properties");
		mockedConsumer.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.CUSTOM);
		for (TopicPartition topicPartition: PARTITIONS) {
			assertEquals(BEGINNING_OFFSET_POSITION, mockedConsumer.position(topicPartition));
		}
	}

	@Test
	public void testCustomOffsetsFromFile() {
		Map<Integer, Long> expectedOffsets = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom-5-partitions.properties");
		CONSUMER_MANAGER.setConsumerCustomStartOptionsFilePath("src/test/resources/test-start-options-custom-5-partitions.properties");
		mockedConsumer.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.CUSTOM);
		for (TopicPartition topicPartition: PARTITIONS) {
			assertEquals(expectedOffsets.get(topicPartition.partition()).longValue(), mockedConsumer.position(topicPartition));
		}
	}
}
