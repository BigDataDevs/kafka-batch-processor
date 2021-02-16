package org.bigdatadevs.kafkabatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailedEventsLogger {

	private static final Logger logger = LoggerFactory.getLogger(FailedEventsLogger.class);

	public static void logFailedEvent(String errorMsg, String event){
		logger.error("General Error Processing Event: ERROR: {}, EVENT: {}", errorMsg, event);
	}

	public static void logFailedEvent(String errorMsg, String event, long eventOffset){
	    logger.error("Error Processing Event: ERROR: {}, EVENT: {}, OFFSET: {}", errorMsg, event, eventOffset);
	}

	public static void logFailedEventWithException(String errorMsg, String event, long eventOffset, Throwable e){
	    logger.error("Error Processing Event: ERROR: {}, EVENT: {}, OFFSET: {}", 
	        errorMsg, event, eventOffset, e);
	}

}
