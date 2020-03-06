package com.joy.kafka.simple.producer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerSSGTest {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerSSGTest.class);
	DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	String append_delimiter = "\t"; // tab
	KafkaProducer<Void, String> producer;

	public static void main(String[] args) {

		KafkaProducerSSGTest logGenerator = new KafkaProducerSSGTest();
				
		logGenerator.testSSG_01();
	}

	private Properties getKafkaProperties(String brokerHosts) {

		Properties props = new Properties();
		props.put("bootstrap.servers", brokerHosts);
		props.put("acks", "1"/*"all"*/);
		//props.put("buffer.memory", "33554432");
		//props.put("batch.size", "16384");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	private void testSSG_01() {
		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092"));
		
		String topic = "SSG-TEST-IN-TOPIC";
		int bulkSize = 1;

		String message = "";
		Random roundRandom = new Random();

		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			int key = roundRandom.nextInt(100) + 1;
			key = i;
			message = makeMessage(key);
			
			producer.send(new ProducerRecord<>(topic, message));
			
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.flush();
		producer.close();
	}
	
	private String makeMessage(int key) {
		StringBuilder logSB = new StringBuilder();
		logSB.append(System.currentTimeMillis());
		logSB.append(append_delimiter);
		logSB.append("http://www.ssg.net/productKey=").append(key);
		logSB.append(append_delimiter);
		//logSB.append("10.88.10.141");
		logSB.append(append_delimiter);
		logSB.append("{\"baseProperties\":\"jsonType\"}");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("2020ver");
		
		return logSB.toString();
	}
}