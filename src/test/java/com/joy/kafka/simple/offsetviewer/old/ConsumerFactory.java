package com.joy.kafka.simple.offsetviewer.old;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerFactory {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerFactory.class);

	private static KafkaConsumer<String, String> createNewConsumer(String brokers, String groupID) throws Exception {

		KafkaConsumer<String, String> kafkaConsumer = null;
		try {

			Properties consumerProps = new Properties();
			consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

			if (groupID != null && !groupID.isEmpty()) {
				consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
			} else {
				consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "logplanet-kafka-monitor-consumer-group-id");
			}

			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
			consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "logplanet-kafka-monitor-consumer-client-id");
			consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");

			// http://ilovebambi.com/221265067750
			// 컨슈머가 브로커에 하트비트를 날리는 주기로 session.timeout.ms의 1/3보다 작게 설정하는게 일반적. 기본값은 3초
			consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");

			kafkaConsumer = new KafkaConsumer<String, String>(consumerProps);

		} catch (Exception ex) {
			logger.error("Connection [KafkaConsumer] has failed,please check brokers.", ex);
			throw ex;
		}

		return kafkaConsumer;
	}

	public static KafkaConsumer<String, String> getKafkaConsumer(String brokers, String groupId) throws Exception {

		KafkaConsumer<String, String> kafkaConsumer = createNewConsumer(brokers, groupId);
		if (logger.isDebugEnabled()) {
			logger.debug(">>> Created a new Kafka Consumer for host : " + brokers + ", groupId : " + groupId);
		}

		return kafkaConsumer;
	}

	//	public static void closeKafkaConsumer(KafkaConsumer<String, String> kafkaConsumer) {
	//		if (kafkaConsumer != null) {
	//
	//			try {
	//				kafkaConsumer.close();
	//			} catch (Exception ex) {
	//				logger.warn("closeConnection error : ", ex);
	//			} finally {
	//				kafkaConsumer = null;
	//			}
	//
	//			logger.info("KafkaConsumer.close() : ");
	//		} else {
	//			logger.warn("KafkaConsumer.close() Not found KafkaConsumer Connection : ");
	//		}
	//	}

	/*
	public static void closeConnection(String requestHost) {
		if (kafkaConsumerMap != null && kafkaConsumerMap.get(requestHost) != null) {
	
			try {
				kafkaConsumerMap.get(requestHost).close();
			} catch (Exception ex) {
				logger.warn("closeConnection error : ", ex);
			}
	
			kafkaConsumerMap.remove(requestHost);
			logger.info("closeConnection and remove : ");
		} else {
			logger.warn("closeConnection Not found requestHost : " + requestHost);
		}
	}
	
	public static void closeAllConnection() {
		for (KafkaConsumer<String, String> consumer : kafkaConsumerMap.values()) {
			//logger.info("Closing connection for: " + consumer.host());
			try {
				consumer.close();
			} catch (Exception ex) {
				logger.warn("closeAllConnection error : ", ex);
			}
		}
	
		kafkaConsumerMap.clear();
		logger.info("closeAllConnection : ");
	}
	*/

	public static AdminClient createAdminClient(String brokers) throws Exception {

		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
		AdminClient adminClient = AdminClient.create(props);

		ConsumerFactory.healthCheckConnect(adminClient);

		return adminClient;
	}

	public static void closeAdminClient(AdminClient adminClient) {
		try {
			if (adminClient != null) {
				adminClient.close();
			} else {
				logger.warn("[closeAdminClient] AdminClient is NULL. ");
			}
		} catch (Exception ex) {
			logger.error("[closeAdminClient] close error : ", ex);
		} finally {
			adminClient = null;
		}
	}

	public static void healthCheckConnect(AdminClient adminClient) throws Exception {

		try {
			adminClient.listTopics();

		} catch (Exception ex) {
			logger.warn("[healthCheckConnect] error : ", ex);
			throw ex;
		} /*finally {
			
			try {
				if (adminClient != null) {
					adminClient.close();
				}
			} catch (Exception ex) {
				logger.warn("[healthCheckConnect] Closing a adminClient Connection. Error : " + ex.toString());
				adminClient = null;
			}
			}
			*/
		logger.info("[healthCheckConnect] OK");
	}

}
