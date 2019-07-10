package com.joy.kafka.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * https://dzone.com/articles/kafka-producer-and-consumer-example
 */
public class KafkaConsumerTest {
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTest.class);

	private Consumer<String, String> consumer;
	private boolean isRunning;
	//private List<Tuple2<String, Integer>> kafkaHostPortList;

	public void startConsumer() throws Exception {
		logger.debug("start consumer...");

		consumer = createConsumer();
		int loopExitCount = 100;
		int noMessageFound = 0;
		
		isRunning = true;
		while (isRunning) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			// 1000 is the time in milliseconds consumer will wait if no record is found at broker.
			if (consumerRecords.count() == 0) {
				noMessageFound++;
				if (noMessageFound > loopExitCount/*IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT*/) {
					// If no message found count is reached to threshold exit loop.
					logger.warn("If no message found count is reached to threshold exit loop.");
					break;
				} else {
					logger.debug(loopExitCount + " is the time in milliseconds consumer will wait if no record is found at broker.. count : " + noMessageFound);
					continue;
				}
				
			}
			
			// noMessageFound initialize
			noMessageFound = 0;
			
			//print each record. 
			consumerRecords.forEach(record -> {
				logger.debug("Record Key " + record.key());
				logger.debug("Record value " + record.value());
				//logger.debug("Record partition " + record.partition());
				//logger.debug("Record offset " + record.offset());
			});
			// commits the offset of record to broker. 
			consumer.commitAsync();
		}
		
		consumer.close();

		isRunning = true;
	}

	public void stopConsumer() {
		logger.debug("stop consumer...");
		consumer.close();
		isRunning = false;
	}

	private Consumer<String, String> createConsumer() {
		logger.debug("create consumer...");

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id_KafkaConsumer");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-id_KafkaConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		//props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("TEST-COMMIT-IN-TOPIC"/*"TEST-IN-TOPIC"/*"CHJUNGLOG-IN-TOPIC"/*"STREAMS-TEST-IN-TOPIC"*/));
		
		return consumer;

	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		KafkaConsumerTest test = new KafkaConsumerTest();
		test.startConsumer();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.warn("Gracefully stopping Kafka Consumer Application");
				test.stopConsumer();
				logger.warn("Application stopped");
			}
		});

		logger.info("Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
	}

}
