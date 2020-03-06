package com.joy.kafka.simple.producer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerCJOTest {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerCJOTest.class);
	DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	DateFormat nanoDfCJO = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	KafkaProducer<Void, String> producer;

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

	@Test
	public void testCJO_Esp01() {
		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.87:9092,192.168.10.88:9092,192.168.10.89:9092"));
		String topic = "CLICK_MOBILE_WEB-IN-TOPIC";
		topic = "STT-DEPLOY-IN-TOPIC";
		topic = "STATEFUL-CJO-IN-TOPIC";
		
		int bulkSize = 1;

		String message = "";
		Random roundRandom = new Random();

		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			int key = roundRandom.nextInt(100) + 1;
			key = 1;
			// 감지 시작 조건
			message = makeMessageCJO(key, "A", "B", "C");
			
			// 종료 조건
			//message = makeMessageCJO(key, "D", "E", "F");
			// 발생 조건
			//message = makeMessageCJO(key, "G", "H", "I");
						
			producer.send(new ProducerRecord<>(topic, message));
						
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		logger.info("# Elapsed Time : " + (System.currentTimeMillis() - startTime) + " ms");
		producer.flush();
		producer.close();
	}
		
	private String makeMessageCJO(int key, String cond01, String cond02, String cond03) {
		StringBuilder sb = new StringBuilder();

		sb.append(nanoDfCJO.format(new Date(System.currentTimeMillis())));
		sb.append(", ");
		sb.append("CLIENT_IP=\"211.176.135.22\", UID=\"00NVOF35jgK3xwkgqT11495903433317\", SID=\"U0b2f3e9168c85c414648df9449e63fef76a11527309431" +key + "\",");
		sb.append(" WEB_CATE_L_CD=\"G00040\", WEB_CATE_G_CD=\"G00007\", REF_PG=\"http://www.todayfortune.co.kr/bbs/search.php?stx=서광맥스3단접이식매트리스^M&s=&ptime=&where=naver\",");
		sb.append(" CUST_EMPL_FL=\"N\", PRD_TP=\"NML\", INFL_GRP_CD=\"G0001\", INFL_CD=\"I0619\", CLICK_CD=\"DTAI____buy__buy__\",");
		sb.append(" MD_CD=\"1394\", WEB_CATE_S_CD=\"G01688\", CUR_PG_NM=\"Oshopping Smart - CJmall\", CHN_CD=\""+cond03+"\", STD_CATE_CD=\"30040205\",");
		sb.append(" WEB_CATE_M_CD=\"G00276\", BRAND_CD=\""+cond02+"\", PRD_CD=\"35674923\", PARTNER_CD=\"411891\", APP_CD=\""+cond01+"\", PRD_NM=\"4단 접이식 매트리스 소파베드싱글\",");
		//sb.append(" CUST_CD=\"202001024266\",");
		sb.append(" CUST_CD=\"\",");
		sb.append(" CUR_PG=\"http://display.cjmall.com/m/item/35674923\", DEVICE_SCR_SIZE=\"360x640\", CLICK_DEPTH5=\"buy\", CLICK_DEPTH7=\"buy\",");
		sb.append(" CLICK_DEPTH1=\"DTAI\", FID=\"RCzpH0k0g1JCe7Yx2b11517847291007\", USERAGENT=\"Mozilla/5.0 (Linux; Android 7.0; SM-A520S Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Crosswalk/20.50.533.51 Mobile Safari/537.36 NAVER(inapp; search; 590; 8.7.3)\",");
		sb.append(" VISIT_LOGIN_FL=\"N\", REF_URL_DOMAIN=\"www.todayfortune.co.kr\", REF_URL_DOMAIN_TP=\"www\", REF_URL_CHN_TP=\"o\", REF_FILE=\"/bbs\",");
		sb.append(" CUR_PG_INFO1=\"/mocode\", BROWSER_TP=\"Crosswalk\", BROWSER_VER=\"20.50.533\", OS=\"Android\", OS_VER=\"7.0\",");
		sb.append(" MOBILE_DEVICE_BRAND_NM=\"Samsung\", MOBILE_DEVICE_MODEL_CD=\"SM-A520S\", SRC_CHN_TP=\"MW\"");
		
		logger.debug(sb.toString());
		return sb.toString();
	}
}