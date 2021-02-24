package org.bigdatadevs.kafkabatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by dhyan on 1/28/16.
 */
public class BatchProcessStarter {
    private static final Logger logger = LoggerFactory.getLogger(BatchProcessStarter.class);
    public static void main(String[] args) throws Exception {
        logger.info("Starting Kafka batch consumer and producer: BatchProcessStarter ");
        ClassPathXmlApplicationContext kafkaBatchContext = new ClassPathXmlApplicationContext("spring/kafkabatch-context-public.xml");
        kafkaBatchContext.registerShutdownHook();

        logger.info("BatchProcessStarter is started OK");

    }
}
