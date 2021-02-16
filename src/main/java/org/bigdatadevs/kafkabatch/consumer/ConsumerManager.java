package org.bigdatadevs.kafkabatch.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bigdatadevs.kafkabatch.CommonKafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author marinapopova
 *         Apr 14, 2016
 */
public class ConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

    @Value("${kafka.consumer.source.topic:testTopic}")
    private String kafkaTopic;
    @Value("${application.id:app1}")
    private String consumerInstanceName;
    @Value("${kafka.consumer.poll.interval.ms:10000}")
    private long kafkaPollIntervalMs;
    @Value("${kafka.consumer.property.prefix:consumer.kafka.property.}")
    private String consumerKafkaPropertyPrefix;
    @Value("${kafka.consumer.pool.count:3}")
    private int kafkaConsumerPoolCount;
    @Resource(name = "applicationProperties")
    private Properties applicationProperties;
    @Autowired
    private ObjectFactory<IConsumerWorker> consumerWorkerObjectFactory;

    private String consumerStartOption;
    private String consumerCustomStartOptionsFilePath;

    private ExecutorService consumersThreadPool = null;
    protected AtomicBoolean running = new AtomicBoolean(false);
    private int initCount = 0;

    public ConsumerManager() {
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownConsumers));
    }

    @PostConstruct
    public void postConstruct() {
        logger.info("postConstruct() is starting ....");
        if (!running.getAndSet(true)) {
            init();
        } else {
            logger.warn("Already running");
        }
    }

    @PreDestroy
    public void preDestroy() {
        logger.info("preDestroy() is starting ....");
        if (running.getAndSet(false)) {
            shutdownConsumers();
        } else {
            logger.warn("Already stopped");
        }
    }

	protected void init() {
        logger.info("init() is starting ....");
        if (consumerWorkerObjectFactory == null) {
            logger.error("No consumerWorkerObjectFactory is found - exiting");
            throw new RuntimeException ("No consumerWorkerObjectFactory is found - exiting");
        }

        initCount++;
        if (initCount > 1) {
            // reset consumerStartOptions to RESTART if this is not the first time we init the Manager 
            // this can happen if it was automatically restarted due to some recoverable issues
            determineOffsetForAllPartitionsAndSeek(StartOption.RESTART);
        } else {
            determineOffsetForAllPartitionsAndSeek(StartOptionParser.getStartOption(consumerStartOption));
        }
        initConsumers(kafkaConsumerPoolCount);
    }

    private void initConsumers(int consumerPoolCount) {
        logger.info("initConsumers() started, consumerPoolCount={}", consumerPoolCount);
        consumersThreadPool = new org.bigdatadevs.kafkabatch.consumer.ConsumerThreadPool(consumerPoolCount);
        for (int consumerCounter = 0; consumerCounter < consumerPoolCount; consumerCounter++) {
            IConsumerWorker consumerWorker = consumerWorkerObjectFactory.getObject();
            consumerWorker.initConsumerInstance(consumerCounter);
            consumersThreadPool.execute(consumerWorker);
        }
    }
    
    public void shutdownConsumers() {
        logger.info("shutdownConsumers() started ....");
        if (consumersThreadPool != null) {
            consumersThreadPool.shutdown();
            consumersThreadPool = null;
        }
        logger.info("shutdownConsumers() finished");
    }

    /**
     * Determines start offsets for kafka partitions and seek to that offsets
     * @param startOption start option
     */
    public void determineOffsetForAllPartitionsAndSeek(StartOption startOption) {
        logger.info("in determineOffsetForAllPartitionsAndSeek(): ");
        if (startOption == StartOption.RESTART) {
        	logger.info("startOption is empty or set to RESTART - consumers will start from RESTART for all partitions");
        	return;
        }

        Properties kafkaProperties = CommonKafkaUtils.extractKafkaProperties(applicationProperties, consumerKafkaPropertyPrefix);
        // add non-configurable properties
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        String consumerClientId = consumerInstanceName + "-offset-reset";
        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId);

        Consumer<String, String> consumer = getConsumerInstance(kafkaProperties);
        consumer.subscribe(Arrays.asList(kafkaTopic));

        //Make init poll to get assigned partitions
        consumer.poll(Duration.ofMillis(kafkaPollIntervalMs));

        Set<TopicPartition> assignedTopicPartitions = consumer.assignment();
        Map<TopicPartition, Long> offsetsBeforeSeek = new HashMap<>();
        for (TopicPartition topicPartition : assignedTopicPartitions) {
            offsetsBeforeSeek.put(topicPartition, consumer.position(topicPartition));
        }

        switch (startOption) {
            case CUSTOM:
                Map<Integer, Long> customOffsetsMap = StartOptionParser.getCustomStartOffsets(consumerCustomStartOptionsFilePath);

                //apply custom start offset options to partitions from file
                if (customOffsetsMap.size() == assignedTopicPartitions.size()) {
                    for (TopicPartition topicPartition : assignedTopicPartitions) {
                        Long startOffset = customOffsetsMap.get(topicPartition.partition());
                        if (startOffset == null) {
                            logger.error("There is no custom start option for partition {}. Consumers will start from RESTART for all partitions", topicPartition.partition());
                            consumer.close();
                            return;
                        }
                        consumer.seek(topicPartition, startOffset);
                    }
                } else {
                    logger.error("Defined custom consumer start options has missed partitions. Expected {} partitions but was defined {}. Consumers will start from RESTART for all partitions",
                            assignedTopicPartitions.size(), customOffsetsMap.size());
                    consumer.close();
                    return;
                }
                break;
            case EARLIEST:
                consumer.seekToBeginning(assignedTopicPartitions);
                break;
            case LATEST:
                consumer.seekToEnd(assignedTopicPartitions);
                break;
            default:
                consumer.close();
                return;
        }
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        assignedTopicPartitions.forEach(partition -> offsetsToCommit.put(
            partition, new OffsetAndMetadata(consumer.position(partition))));
        consumer.commitSync(offsetsToCommit);
        for (TopicPartition topicPartition : assignedTopicPartitions) {
            logger.info("Offset for partition: {} is moved from : {} to {} with startOption: {}",
                topicPartition.partition(), offsetsBeforeSeek.get(topicPartition), 
                consumer.position(topicPartition), startOption);
            logger.info("Offset position during the startup for consumerId : {}, partition : {}, " + 
                "offset : {},  startOption: {}", consumerClientId, 
                topicPartition.partition(), consumer.position(topicPartition), startOption);
        }
        consumer.close();
    }

    public Consumer<String, String> getConsumerInstance(Properties properties) {
        return new KafkaConsumer<>(properties);
    }

    public void setConsumerStartOption(String consumerStartOption) {
        this.consumerStartOption = consumerStartOption;
    }

    public void setConsumerCustomStartOptionsFilePath(String consumerCustomStartOptionsFilePath) {
        this.consumerCustomStartOptionsFilePath = consumerCustomStartOptionsFilePath;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public long getKafkaPollIntervalMs() {
        return kafkaPollIntervalMs;
    }

    public void setKafkaPollIntervalMs(long kafkaPollIntervalMs) {
        this.kafkaPollIntervalMs = kafkaPollIntervalMs;
    }

}
