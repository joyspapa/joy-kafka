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
	DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	DateFormat nanoDfCJO = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	KafkaProducer<Void, String> producer;

	public static void main(String[] args) {

		KafkaProducerTest logGenerator = new KafkaProducerTest();
		//logGenerator.testCase01();
		//logGenerator.testCase02();
		//logGenerator.testCaseMagento();
		//logGenerator.testHcs();
		
		//logGenerator.testPerformance();
		
		//logGenerator.testHcs_Esp01();
		
		logGenerator.testCJO_Esp01();
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

	private void testCJO_Esp01() {
		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.87:9092,192.168.10.88:9092,192.168.10.89:9092"));
		String topic = "CLICK_MOBILE_WEB-IN-TOPIC";
		//topic = "CORE-LOG-IN-TOPIC";
		int bulkSize = 1;

		String message = "";
		Random roundRandom = new Random();

		String uidPrefix = "C00000";
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			int key = roundRandom.nextInt(100) + 1;
			if (bulkSize == 1 || i % 100 == 0) {
				key = 39101930;
			} else if (i % 150 == 0) {
				key = 39101929;
			}
			
			message = makeMessageCJO(key, "-", "-", "-", "-");
			
			producer.send(new ProducerRecord<>(topic, message));
			
			
			if (i % 30000 == 0) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				message = makeMessage(key, "MW_신용대출_DL", "한도조회", "나가기");
			} else if (i % 1000 == 0) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.flush();
		producer.close();
	}
	
	private String makeMessageCJO(int key, String ec, String ea, String el, String cd1) {
		StringBuilder sb = new StringBuilder();

		sb.append(nanoDfCJO.format(new Date(System.currentTimeMillis())));
		sb.append(", ");
		sb.append("CLIENT_IP=\"211.176.135.22\", UID=\"00NVOF35jgK3xwkgqT11495903433317\", SID=\"U0b2f3e9168c85c414648df9449e63fef76a11527309431\",");
		sb.append(" WEB_CATE_L_CD=\"G00040\", WEB_CATE_G_CD=\"G00007\", REF_PG=\"http://www.todayfortune.co.kr/bbs/search.php?stx=서광맥스3단접이식매트리스^M&s=&ptime=&where=naver\",");
		sb.append(" CUST_EMPL_FL=\"N\", PRD_TP=\"NML\", INFL_GRP_CD=\"G0001\", INFL_CD=\"I0619\", CLICK_CD=\"DTAI____buy__buy__\",");
		sb.append(" MD_CD=\"1394\", WEB_CATE_S_CD=\"G01688\", CUR_PG_NM=\"Oshopping Smart - CJmall\", CHN_CD=\"50001002\", STD_CATE_CD=\"30040205\",");
		sb.append(" WEB_CATE_M_CD=\"G00276\", BRAND_CD=\"45887001\", PRD_CD=\"35674923\", PARTNER_CD=\"411891\", APP_CD=\"PDA\", PRD_NM=\"4단 접이식 매트리스 소파베드싱글\",");

		sb.append(" CUR_PG=\"http://display.cjmall.com/m/item/35674923\", DEVICE_SCR_SIZE=\"360x640\", CLICK_DEPTH5=\"buy\", CLICK_DEPTH7=\"buy\",");
		sb.append(" CLICK_DEPTH1=\"DTAI\", FID=\"RCzpH0k0g1JCe7Yx2b11517847291007\", USERAGENT=\"Mozilla/5.0 (Linux; Android 7.0; SM-A520S Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Crosswalk/20.50.533.51 Mobile Safari/537.36 NAVER(inapp; search; 590; 8.7.3)\",");

		sb.append(" VISIT_LOGIN_FL=\"N\", REF_URL_DOMAIN=\"www.todayfortune.co.kr\", REF_URL_DOMAIN_TP=\"www\", REF_URL_CHN_TP=\"o\", REF_FILE=\"/bbs\",");
		sb.append(" CUR_PG_INFO1=\"/item\", BROWSER_TP=\"Crosswalk\", BROWSER_VER=\"20.50.533\", OS=\"Android\", OS_VER=\"7.0\",");
		sb.append(" MOBILE_DEVICE_BRAND_NM=\"Samsung\", MOBILE_DEVICE_MODEL_CD=\"SM-A520S\", SRC_CHN_TP=\"MW\"");
		
		logger.debug(sb.toString());
		return sb.toString();
	}
	
	private void testHcs_Esp01() {
		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092"));
		String topic = "HC-IN-TOPIC";
		//topic = "CORE-LOG-IN-TOPIC";
		
		int bulkSize = 1;

		String message = "";
		Random roundRandom = new Random();

		String uidPrefix = "C00000";
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			int key = roundRandom.nextInt(100) + 1;
			if (bulkSize == 1 || i % 100 == 0) {
				key = 39101930;
			} else if (i % 150 == 0) {
				key = 39101929;
			}
			
			message = makeMessage(key, "MW_신용대출_DL", "한도 및 금리 확인", "한도조회");
			
			//message = makeMessage(key, "MW_신용대출_DL", "한도조회", "나가기");
			
			//message = makeMessage(key, "a", "한도조회", "나가기");
			//message = makeMessage(key, "d", "한도조회", "나가기");
			
			producer.send(new ProducerRecord<>(topic, message));
			
			
			if (i % 30000 == 0) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				message = makeMessage(key, "MW_신용대출_DL", "한도조회", "나가기");
			} else if (i % 1000 == 0) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.flush();
		producer.close();
	}
	
	private void testPerformance() {

		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092"));
		String topic = "OBZ-TEST-IN-TOPIC";
		topic = "CORE-LOG-IN-TOPIC";
		int bulkSize = 1;

		String message = "";
		Random roundRandom = new Random();

		String uidPrefix = "C00000";
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			int key = roundRandom.nextInt(499) + 1;
			key = 39101929;
			// 시나리오 1 
			if (i % 100 == 0) {
				// in
				message = makeMessage(key, "MW_신용대출_DL", "한도 및 금리 확인", "한도조회2", "W");
			} else if (i % 10000 == 0) {
				// complete
				message = makeMessage(key, "MW_신용대출_DL", "한도조회", "다음", "W");
				// out
				//message = makeMessage("MW_신용대출_DL", "한도조회", "나가기", "W");
			} else {
				message = makeMessage(key, "MW_일반로그", "한도 및 금리 확인", "한도조회2", "Q");
			}
			producer.send(new ProducerRecord<>(topic, message));
			
			// 시나리오 2
			if (i % 100 == 0) {
				// in
				message = makeMessage(key, "MW_자이프_메인", "구매프로그램", "준비되셨나요?", "A");
			} else if (i % 100000 == 0) {
				// complete
				//message = makeMessage("MW_자이프_메인", "본인확인", "다음", "A");
				// out
				message = makeMessage(key, "MW_자이프_메인", "본인확인", "나가기", "A");
			} else {
				message = makeMessage(key, "MW_일반로그", "한도 및 금리 확인", "한도조회2", "Q");
			}
			producer.send(new ProducerRecord<>(topic, message));
			
			
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
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.flush();
		producer.close();
	}

	private String makeMessage(int key, String ec, String ea, String el) {
		return makeMessage(key, ec, ea, el, "0001");
	}
	
	private String makeMessage(int key, String ec, String ea, String el, String cd1) {
		StringBuilder sb = new StringBuilder();

		sb.append("{\"evt_time\" : \"" + nanoDf.format(new Date(System.currentTimeMillis())));
		sb.append(
				"\",\"cid\" : \"" + key + "\",\"dl\" : \"http://www.naver.com\",\"ul\" : \"ko-KR\",\"de\" : \"windows-1252\",");
		sb.append("\"dt\" : \"PageTitle\",\"dr\" : \"http://www.naver.com\",\"sd\" : 24,\"sr\" : \"1920x1080\",");
		sb.append(
				"\"vp\" : \"958x195\",\"cs\" : \"facebook\",\"cm\" : \"social\",\"cn\" : \"1월 프로모션\",\"ck\" : \"펠리세이드\",");
		sb.append("\"cc\" : \"모델있음\",");
		sb.append("\"uid\" : \"" + key + "\","); // "\"uid\" : \"" + uidPrefix + String.format("%03d", roundRandom.nextInt(499) + 1) +"\","

		sb.append(
				"\"event\" : {\"ec\" : \"" + ec + "\",\"ea\" : \"" + ea + "\",\"el\" : \"" + el + "\",\"ev\" : 2000},");
		sb.append("\"dimension\" : {\"cd1\" : \"" + cd1 + "\",");

		sb.append("\"cd2\" : \"OUT\",\"cd3\" : \"MAIN\",\"cd4\" : \"N\"},");
		sb.append("\"metric\" : {\"cm1\" : 1,\"cm2\" : 2,\"cm3\" : 3,\"cm4\" : 4}}");

		return sb.toString();
	}

	private void testHcs() {

		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092"));
		String topic = "OBZ-TEST-IN-TOPIC";
		topic = "CORE-LOG-IN-TOPIC";
		int bulkSize = 1000;

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
					+ "\"uid\" : \"" + uidPrefix + String.format("%03d", roundRandom.nextInt(499) + 1) + "\","
					+ "\"dimension\" : {\"cd1\" : \"mobileweb\",\"cd2\" : \"OUT\",\"cd3\" : \"MAIN\",\"cd4\" : \"N\"},"
					+ "\"metric\" : {\"cm1\" : 1,\"cm2\" : 2,\"cm3\" : 3,\"cm4\" : 4}}";

			message = "{\"evt_time\" : \"" + nanoDf.format(new Date(System.currentTimeMillis()))
					+ "\",\"cid\" : \"112233445566\",\"dl\" : \"http://www.naver.com\",\"ul\" : \"ko-KR\",\"de\" : \"windows-1252\","
					+ "\"dt\" : \"PageTitle\",\"dr\" : \"http://www.naver.com\",\"sd\" : 24,\"sr\" : \"1920x1080\","
					+ "\"vp\" : \"958x195\",\"cs\" : \"facebook\",\"cm\" : \"social\",\"cn\" : \"1월 프로모션\",\"ck\" : \"펠리세이드\","
					+ "\"cc\" : \"모델있음\"," + "\"Uid\" : \"39101929\","

					//			+ "\"event\" : {\"ec\" : \"MW_신용대출_DL\",\"ea\" : \"한도 및 금리 확인\",\"el\" : \"한도조회2\",\"ev\" : 2000},"
					//			+ "\"dimension\" : {\"cd1\" : \"W\","

					+ "\"event\" : {\"ec\" : \"MW_자이프_메인\",\"ea\" : \"구매프로그램\",\"el\" : \"준비되셨나요?\",\"ev\" : 2000},"
					+ "\"dimension\" : {\"cd1\" : \"A\","

					+ "\"cd2\" : \"OUT\",\"cd3\" : \"MAIN\",\"cd4\" : \"N\"},"
					+ "\"metric\" : {\"cm1\" : 1,\"cm2\" : 2,\"cm3\" : 3,\"cm4\" : 4}}";

			// 이탈 조건
			//			message = "{\"evt_time\" : \"" + nanoDf.format(new Date(System.currentTimeMillis())) 
			//			+ "\",\"cid\" : \"112233445566\",\"dl\" : \"http://www.naver.com\",\"ul\" : \"ko-KR\",\"de\" : \"windows-1252\","
			//			+ "\"dt\" : \"PageTitle\",\"dr\" : \"http://www.naver.com\",\"sd\" : 24,\"sr\" : \"1920x1080\","
			//			+ "\"vp\" : \"958x195\",\"cs\" : \"facebook\",\"cm\" : \"social\",\"cn\" : \"1월 프로모션\",\"ck\" : \"펠리세이드\","
			//			+ "\"cc\" : \"모델있음\","
			//			
			//			+ "\"event\" : {\"ec\" : \"MW_신용대출_DL\",\"ea\" : \"한도조회\",\"el\" : \"나가기\",\"ev\" : 2000},"
			//			
			//			+ "\"Uid\" : \"39101929\","
			//			+ "\"dimension\" : {\"cd1\" : \"W\",\"cd2\" : \"OUT\",\"cd3\" : \"MAIN\",\"cd4\" : \"N\"},"
			//			+ "\"metric\" : {\"cm1\" : 1,\"cm2\" : 2,\"cm3\" : 3,\"cm4\" : 4}}";

			// Complete 조건
			//			message = "{\"evt_time\" : \"" + nanoDf.format(new Date(System.currentTimeMillis())) 
			//			+ "\",\"cid\" : \"112233445566\",\"dl\" : \"http://www.naver.com\",\"ul\" : \"ko-KR\",\"de\" : \"windows-1252\","
			//			+ "\"dt\" : \"PageTitle\",\"dr\" : \"http://www.naver.com\",\"sd\" : 24,\"sr\" : \"1920x1080\","
			//			+ "\"vp\" : \"958x195\",\"cs\" : \"facebook\",\"cm\" : \"social\",\"cn\" : \"1월 프로모션\",\"ck\" : \"펠리세이드\","
			//			+ "\"cc\" : \"모델있음\","
			//			
			//			+ "\"event\" : {\"ec\" : \"MW_신용대출_DL\",\"ea\" : \"한도조회\",\"el\" : \"다음\",\"ev\" : 2000},"
			//			
			//			+ "\"Uid\" : \"39101929\","
			//			+ "\"dimension\" : {\"cd1\" : \"W\",\"cd2\" : \"OUT\",\"cd3\" : \"MAIN\",\"cd4\" : \"N\"},"
			//			+ "\"metric\" : {\"cm1\" : 1,\"cm2\" : 2,\"cm3\" : 3,\"cm4\" : 4}}";

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

		producer = new KafkaProducer<>(
				getKafkaProperties("192.168.10.211:9092,192.168.10.212:9092,192.168.10.213:9092"));
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
					+ "   \"cust_id\": \"" + roundRandom.nextInt(100)
					+ "\",    \"session_id\": \"820190423192550\",    \"cookie_id\": \"OBZ.8.4.20190423192550\", "
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