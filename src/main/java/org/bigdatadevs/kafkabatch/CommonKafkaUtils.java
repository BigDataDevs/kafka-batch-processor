/**
  * @author marinapopova
  * Jul 17, 2019
 */
package org.bigdatadevs.kafkabatch;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class CommonKafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonKafkaUtils.class);
    public static final String PROPERTY_SEPARATOR = ".";

    /**
     * Create a new kafkaProperties map and add application properties that start 
     * with a configured prefix (kafkaPropertyPrefix) to it; 
     * Prefix is removed from the final Kafka property name added to the map
     * 
     * @param applicationProperties
     * @param kafkaPropertyPrefix
     */
    public static Properties extractKafkaProperties(Properties applicationProperties, String kafkaPropertyPrefix) {
        Properties kafkaProperties = new Properties();
        if(applicationProperties == null || applicationProperties.isEmpty()){
            LOGGER.info("No application kafka properties are found to set");
            return kafkaProperties;
        }
        // normalize kafka properties prefix
        kafkaPropertyPrefix = kafkaPropertyPrefix.endsWith(PROPERTY_SEPARATOR) ? kafkaPropertyPrefix : kafkaPropertyPrefix + PROPERTY_SEPARATOR;

        for(Map.Entry <Object,Object> currentPropertyEntry :applicationProperties.entrySet()){
            String propertyName = currentPropertyEntry.getKey().toString();
            if(StringUtils.isNotBlank(propertyName) && propertyName.contains(kafkaPropertyPrefix)){
                String validKafkaConsumerProperty = propertyName.replace(kafkaPropertyPrefix, StringUtils.EMPTY);
                kafkaProperties.put(validKafkaConsumerProperty, currentPropertyEntry.getValue());
                LOGGER.info("Adding kafka property with prefix: {}, key: {}, value: {}", kafkaPropertyPrefix, validKafkaConsumerProperty, currentPropertyEntry.getValue());
            }
        }
        return kafkaProperties;
    }

}
