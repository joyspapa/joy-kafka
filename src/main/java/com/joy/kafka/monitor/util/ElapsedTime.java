package com.joy.kafka.monitor.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElapsedTime {

	private static final Logger logger = LoggerFactory.getLogger(ElapsedTime.class);

	/**
	 * 서비스의 전 처리
	 *
	 * @throws Exception
	 */
	public static void setUp(String serviceName) {
		ThreadLocalContext.elapsedTime.set(System.currentTimeMillis());
		ThreadLocalContext.serviceName.set(serviceName);
	}

	/**
	 * 서비스의 후 처리
	 *
	 * @throws Exception
	 */
	public static void tearDown() {
		long elapsedTime = System.currentTimeMillis() - ThreadLocalContext.elapsedTime.get();
		
		if (elapsedTime > 99 && logger.isInfoEnabled()) {
			logger.info("# [{}] Elapsed Time : {} ms", ThreadLocalContext.serviceName.get(),
					(System.currentTimeMillis() - ThreadLocalContext.elapsedTime.get()));
		}
		
		// TODO - logging, transaction closing, cache clear
		ThreadLocalContext.elapsedTime.remove();
		ThreadLocalContext.serviceName.remove();
	}
}
