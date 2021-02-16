/**
  * @author marinapopova
  * Sep 28, 2019
 */
package org.bigdatadevs.kafkabatch.processor;

public class BatchRecoverableException extends Exception {

	/**
	 * This exception type should be thrown from the IBatchProcessor.completePoll() method
	 * if there was an issue that might be temporary in nature and is believed to become resolved
	 * after a few reties of the action. For example - timeouts calling third-party services like DBs,
	 * Kafka, etc.
	 */
	public BatchRecoverableException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public BatchRecoverableException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public BatchRecoverableException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public BatchRecoverableException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public BatchRecoverableException(String message, Throwable cause,
									 boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
