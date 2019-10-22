package com.joy.kafka.monitor.factory;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminClient;

public class KafkaAdminClientFactory {
	private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClientFactory.class);

	private static AdminClient adminClient = null;

	public static AdminClient getAdminClient(String brokers) {
		if (adminClient == null) {
			createAdminClient(brokers);
		}
		return adminClient;
	}

	private static void createAdminClient(String brokers) {
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(1000));
		props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, String.valueOf(1000));
		
		try {
			adminClient = AdminClient.create(props);
		} catch (Throwable ex) {
			logger.warn("Kafka AdminClient connection failed : ", ex);
		}

		logger.info("AdminClient has been created ! ");
	}

	public static void closeAdminClient() {
		if (adminClient != null) {
			try {
				adminClient.close();
				logger.info("AdminClient has been closed ! ");
			} catch (Throwable ex) {
				logger.warn("During closing AdminClient warn : ", ex);
			} finally {
				adminClient = null;
			}
		}
	}

	/*
	public static Set<String> getTopicListByAdminClient() {
		try {
		    org.apache.kafka.clients.admin.AdminClient adminClientSide = org.apache.kafka.clients.admin.AdminClient.create(props);
			Set<String> topicSet = adminClientSide.listTopics().names().get();
			topicSet.remove("__consumer_offsets");
			logger.debug("getTopicList : {}", topicSet);
			
			return topicSet;
		} catch (InterruptedException | ExecutionException e) {
			logger.warn("During getTopicList AdminClient warn : ", e);
		}
		return null;
	}
	*/
}
