package com.joy.kafka.monitor.factory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerFactory {
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerFactory.class);

	private static Map<String, KafkaConsumer<String, String>> kafkaConsumerMap = new HashMap<String, KafkaConsumer<String, String>>();
	private static final String DEFAULT_GROUP_ID = "default_KafkaConsumerFactory_GroupID";
	
	public static KafkaConsumer<String, String> getKafkaConsumer(String brokers) {
		return getKafkaConsumer(brokers, DEFAULT_GROUP_ID);
	}
	
	public static KafkaConsumer<String, String> getKafkaConsumer(String brokers, String groupID) {
		if(groupID == null) {
			groupID = DEFAULT_GROUP_ID;
		}
		
		if (kafkaConsumerMap.get(groupID) == null) {
			kafkaConsumerMap.put(groupID, createNewConsumer(brokers, groupID));
		}
		return kafkaConsumerMap.get(groupID);
	}

	private static KafkaConsumer<String, String> createNewConsumer(String brokers, String groupID) {

		KafkaConsumer<String, String> kafkaConsumer = null;
		try {

			Properties consumerProps = getDefaultConsumerConfig();
			consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
			if (groupID != null) {
				consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
			}
			
			kafkaConsumer = new KafkaConsumer<String, String>(consumerProps);

			//if (logger.isInfoEnabled()) {
			//	logger.info(">>> Created a new Kafka Consumer for host : {}, groupId : {}", brokers, groupID);
			//}
			
		} catch (Exception ex) {
			logger.error("Connection [KafkaConsumer] has failed. please check brokers.", ex);
		}

		return kafkaConsumer;
	}

	private static Properties getDefaultConsumerConfig() {
		Properties consumerProps = new Properties();

		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-offser-monitor-client-id");
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
		consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "4000");
		
		// http://ilovebambi.com/221265067750
		// 컨슈머가 브로커에 하트비트를 날리는 주기로 session.timeout.ms의 1/3보다 작게 설정하는게 일반적. 기본값은 3초
		consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		return consumerProps;
	}

	public static void closeKafkaConsumer() {
		if (!kafkaConsumerMap.isEmpty()) {
			for (String groupID : kafkaConsumerMap.keySet()) {
				closeKafkaConsumer(groupID);
			}
			kafkaConsumerMap.clear();
		}
	}

	private static void closeKafkaConsumer(String groupID) {
		KafkaConsumer<String, String> kafkaConsumer = kafkaConsumerMap.get(groupID);
		try {
			kafkaConsumer.close();
			logger.info("kafkaConsumer has been closed! (groupID={})", groupID);
		} catch (Exception ex) {
			logger.warn("During closing kafkaConsumer. (groupID={}) warn : ", groupID, ex);
			kafkaConsumer = null;
		}
	}
}
