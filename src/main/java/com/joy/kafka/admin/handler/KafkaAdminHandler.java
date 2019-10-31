package com.joy.kafka.admin.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAdminHandler {

	private static final Logger logger = LoggerFactory.getLogger(KafkaAdminHandler.class);
	private String borkers;

	private KafkaAdminHandler(String brokers) {
		this.borkers = brokers;
	}

	public static KafkaAdminHandler create(String brokers) {
		return new KafkaAdminHandler(brokers);
	}

	private AdminClient connect() throws Exception {
		Properties props = new Properties();
		props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.borkers);
		props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(500));
		props.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, String.valueOf(500));
		
		return AdminClient.create(props);
	}

	public List<String> listTopics() throws Exception {
		List<String> topicList = new ArrayList<String>();

		try (AdminClient conn = connect()) {
			ListTopicsResult result = conn.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(false));
			Iterator<TopicListing> topics = result.listings().get().iterator();

			TopicListing topic = null;

			while (topics.hasNext()) {
				topic = (TopicListing) topics.next();
				topicList.add(topic.name());
				//logger.debug("topicName : " + topic.name());
			}
		}

		return topicList;
	}

	public void createTopic(String topicName) throws Exception {
		createTopic(topicName, -1, (short)-1);
	}
	
	public void createTopic(String topicName, int numPartitions, short replicaFactor) throws Exception {

		try (AdminClient conn = connect()) {
			// Define topic
			final NewTopic newTopic = new NewTopic(topicName, numPartitions, replicaFactor);
			final CreateTopicsOptions option = new CreateTopicsOptions().timeoutMs(500);

			// Since the call is Async, Lets wait for it to complete.
			conn.createTopics(Collections.singleton(newTopic), option).all().get();

		} catch (InterruptedException | ExecutionException e) {
			if (!(e.getCause() instanceof TopicExistsException)) {
				logger.warn("createTopic : " + topicName, e);
				throw new RuntimeException(e.getMessage(), e);
			} else {
				logger.info("createTopic : " + topicName + ", skipped exception : " + e.toString());
			}
			// TopicExistsException - Swallow this exception, just means the topic already exists.
		}
	}

	public void deleteTopic(String topicName) throws Exception {

		try (AdminClient conn = connect()) {
			// Define topic
			final DeleteTopicsOptions option = new DeleteTopicsOptions().timeoutMs(500);

			// Since the call is Async, Lets wait for it to complete.
			conn.deleteTopics(Arrays.asList(topicName.split(",")), option).all().get();

		} catch (InterruptedException | ExecutionException e) {
			if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
				logger.warn("deleteTopic : " + topicName, e);
				throw new RuntimeException(e.getMessage(), e);
			} else {
				logger.info("createTopic : " + topicName + ", skipped exception : " + e.toString());
			}
			// UnknownTopicOrPartitionException: This server does not host this topic-partition
		}
	}

}
