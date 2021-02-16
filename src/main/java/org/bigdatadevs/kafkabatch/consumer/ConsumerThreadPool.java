package org.bigdatadevs.kafkabatch.consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Vitalii Cherniak on 5/31/18.
 */
public class ConsumerThreadPool extends ThreadPoolExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerThreadPool.class);
	private static final String KAFKA_CONSUMER_THREAD_NAME_FORMAT = "kafka-consumer-thread-%d";
	private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(KAFKA_CONSUMER_THREAD_NAME_FORMAT).build();
	private List<Runnable> consumersThreads;

	public ConsumerThreadPool(int nThreads) {
		super(nThreads, nThreads,
				0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>(),
				threadFactory);
		consumersThreads = new ArrayList<>(nThreads);
	}

	@Override
	public void execute(Runnable command) {
		consumersThreads.add(command);
		super.execute(command);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);

		if (t == null && r instanceof Future<?>) {
			try {
				Object result = ((Future<?>) r).get();
			} catch (CancellationException e) {
				t = e;
			} catch (ExecutionException e) {
				t = e.getCause();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		if (t != null) {
			LOGGER.error("Shutting down application because consumer thread got exception", t);
			System.exit(0);
		}
	}

	@Override
	public void shutdown() {
		super.shutdown();
		// ... Stop all consumers
		consumersThreads.forEach(consumerThread -> {
			try {
				((AutoCloseable)consumerThread).close();
			} catch (Exception e) {
				LOGGER.error("Unable to shutdown {}", consumerThread, e);
			}
		});
		try {
			awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOGGER.warn("Got InterruptedException while shutting down consumers, aborting");
		}
	}
}
