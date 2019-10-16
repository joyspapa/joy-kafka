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

public class KafkaProducerTest {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerTest.class);

	KafkaProducer<Void, String> producer;

	public static void main(String[] args) {

		KafkaProducerTest logGenerator = new KafkaProducerTest();
		//logGenerator.testCase01();
		//logGenerator.testCase02();
		//logGenerator.testCaseMagento();
		logGenerator.testHcs();

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

	private void testHcs() {

		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092"));
		String topic = "OBZ-TEST-IN-TOPIC";
		topic = "TEST-COMMIT-IN-TOPIC";
		int bulkSize = 9;
		DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

		String message;
		Random roundRandom = new Random();

		String uidPrefix = "C00000";
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			message = "{\"evt_time\" : \"" + nanoDf.format(new Date(System.currentTimeMillis())) 
					+ "\",\"cid\" : \"112233445566\",\"dl\" : \"http://www.naver.com\",\"ul\" : \"ko-KR\",\"de\" : \"windows-1252\","
					+ "\"dt\" : \"PageTitle\",\"dr\" : \"http://www.naver.com\",\"sd\" : 24,\"sr\" : \"1920x1080\","
					+ "\"vp\" : \"958x195\",\"cs\" : \"facebook\",\"cm\" : \"social\",\"cn\" : \"1월 프로모션\",\"ck\" : \"펠리세이드\","
					+ "\"cc\" : \"모델있음\","
					+ "\"event\" : {\"ec\" : \"App_공통\",\"ea\" : \"이벤트배너\",\"el\" : \"처음 목록으로 이동\",\"ev\" : 2000},"
					+ "\"uid\" : \"" + uidPrefix + String.format("%03d", roundRandom.nextInt(499) + 1) +"\","
					+ "\"dimension\" : {\"cd1\" : \"mobileweb\",\"cd2\" : \"OUT\",\"cd3\" : \"MAIN\",\"cd4\" : \"N\"},"
					+ "\"metric\" : {\"cm1\" : 1,\"cm2\" : 2,\"cm3\" : 3,\"cm4\" : 4}}";

			//message = "{ \"birth_day\":\"20190521\", \"name\" : \"chjung\", \"age\" : 24, \"year\" : 2019 }";
			
			message = "" + i;
			producer.send(new ProducerRecord<>(topic, message));
			/*
			if (i % 20000 == 0) {
				try {
					Thread.sleep(600);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else if (i % 2000 == 0) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			*/
//			try {
//				Thread.sleep(100);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.flush();
		producer.close();
	}
	
	private void testCase02() {

		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.82:9092,192.168.10.83:9092"));
		String topic = "SP-TEST-IN-TOPIC";

		int bulkSize = 100;
		DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

		String message;
		Random roundRandom = new Random();

		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			message = "{" + "\"event_timestamp\":\"" + nanoDf.format(new Date(System.currentTimeMillis())) + "\","
					+ "\"session_id\":\"123456789" + roundRandom.nextInt(1000) + "\"," + "\"uuid\":\"123456789"
					+ roundRandom.nextInt(100) + "\"," + "\"cust_id\":\"" + String.valueOf(i) + "\"" + "}";

			producer.send(new ProducerRecord<>(topic, message));

			//			if (i % 300 == 0) {
			//				try {
			//					Thread.sleep(5000);
			//				} catch (InterruptedException e) {
			//					e.printStackTrace();
			//				}
			//logger.info("# count for sleep : " + i);
			//			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.close();
	}

	private void testCase01() {

		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.211:9092,192.168.10.212:9092"));
		String topic = "NO_OPEN_CONNECTION_TEST";

		int bulkSize = 100000000;
		//    	DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		//		String nanoDate = nanoDf.format(new Date(System.currentTimeMillis()));

		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {
			producer.send(new ProducerRecord<>(topic, String.valueOf(i)));

			if (i % 300 == 0) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//logger.info("# count for sleep : " + i);
			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.close();
	}

	private void testCaseMagento() {

		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.211:9092,192.168.10.212:9092,192.168.10.213:9092"));
		String topic = "MAGENTO-LOG-IN-TOPIC";
		//topic = "CJO-GC-LOG2-IN-TOPIC";
		
		int bulkSize = 100000000;
		DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmss");

		String message;
		Random roundRandom = new Random();

		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			message = "{  \"Itemview\": {    \"event_timestamp\": \""
					+ nanoDf.format(new Date(System.currentTimeMillis()))
					+ "\",    \"referrer_uri\": \"http://192.168.10.84/magento/\", "
					+ "   \"curr_page_uri\": \"http://192.168.10.84/magento/tori-tank-590.html\",    \"curr_page_name\": \"Tori Tank\", "
					+ "   \"cust_id\": \""+ roundRandom.nextInt(100) +"\",    \"session_id\": \"820190423192550\",    \"cookie_id\": \"OBZ.8.4.20190423192550\", "
					+ "   \"country\": \"South Korea\",    \"city\": \"Seoul\",    \"user_ip\": \"222.110.141.3\",    \"continent\": \"Asia\", "
					+ "   \"subcontinent\": \"Eastern Asia\",    \"os_type\": \"Windows\",    \"manufacturer\": \"Else\",    \"device_type\": \"PC\", "
					+ "   \"app_web_type\": \"Web\",    \"browser_type\": \"Chrome\",    \"inbound_chnl_medium\": \"\",    \"inbound_chnl_campaign\": \"\", "
					+ "   \"Itemview_detail\": {      \"product_cd\": \"418\",      \"product_nm\": \"Tori Tank\",      \"product_catg_l\": \"4\",  "
					+ "    \"product_catg_m\": \"10\"    }  }}";

			producer.send(new ProducerRecord<>(topic, message));

			if (i % 1000 == 0) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
////				try {
////					Thread.sleep(50);
////				} catch (InterruptedException e) {
////					e.printStackTrace();
////				}
			}

//			try {
//				Thread.sleep(100);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.close();
	}

}