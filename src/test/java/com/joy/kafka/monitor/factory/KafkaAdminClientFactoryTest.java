package com.joy.kafka.monitor.factory;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAdminClientFactoryTest {
	private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClientFactoryTest.class);

	public static void main(String[] args) {
		String brokers = "192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092";
		//brokers = "192.168.10.82:9092,192.168.10.83:9092";
		brokers = "192.168.10.81:9092";

		KafkaAdminClient adminClient = KafkaAdminClientFactory.getAdminClient(brokers);
		try {
			//logger.debug("find1 : " + adminClient.);
			//Object find = adminClient.findAllBrokers();
			//logger.debug("find : " + find);
		} catch (Throwable ex) {
			logger.warn("During testing AdminClient : ", ex);
		} finally {
			//KafkaAdminClientFactory.closeAdminClient();
		}
	}

}
