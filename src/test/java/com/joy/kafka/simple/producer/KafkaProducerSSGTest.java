package com.joy.kafka.simple.producer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerSSGTest {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerSSGTest.class);
	private static final DateFormat nanoDf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	private static final String formatZ = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	private static final SimpleDateFormat simpleDateFormatZ = new SimpleDateFormat(formatZ);
	
	private static final String append_delimiter = "\t"; // tab
	KafkaProducer<Void, String> producer;
	int randomInt = 50;

	public static void main(String[] args) {

		KafkaProducerSSGTest logGenerator = new KafkaProducerSSGTest();

		logGenerator.testSSG_01();
		
		//logGenerator.testSSG_02();
		//logGenerator.testRandom();
		//logGenerator.convertNanoToMicros();
		//logGenerator.performanceTest();
	}

	private Properties getKafkaProperties(String brokerHosts) {

		Properties props = new Properties();
		props.put("bootstrap.servers", brokerHosts);
		props.put("acks", "0"); //1:leader, -1:all, 0:none"
		//props.put("buffer.memory", "33554432");
		//props.put("linger.ms", "100");
		props.put("batch.size", "163840"); // 1638400 , 3276800
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	private void testSSG_01() {
		producer = new KafkaProducer<>(getKafkaProperties("192.168.10.57:9092,192.168.10.58:9092,192.168.10.59:9092"));

		String topic = "SSG-TRK-BASE-IN-TOPIC";
		int bulkSize = 20_000_000;
		bulkSize = 100;
		String message = "";
		Random roundRandom = new Random();
		int checkCnt = 1;
		
		long startTime = System.currentTimeMillis();
		long forTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			int key = roundRandom.nextInt(randomInt + 1);
			//key = i;

			message = makeMessage(key);
			if(key%7 == 0) {
				checkCnt++;
			}
			//logger.info("# message : {} ", message);
			
			producer.send(new ProducerRecord<>(topic, null, message));
			
//			if (i % 100 == 0) {
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
			
			if (i % 1000 == 0) {
				logger.info("# sending count : {} , elapsed : {} ms", i, (System.currentTimeMillis() - forTime));
				forTime = System.currentTimeMillis();
			}
		}

		logger.info("CheckCnt : {} ", checkCnt);
		logger.info("# Elapsed Time : {} ms", (System.currentTimeMillis() - startTime));
		producer.flush();
		producer.close();
	}
	
	private String makeMessage(int key) {
		long visitDtsVal = System.currentTimeMillis();
		String mbrID = "obzen";
		if(key%3 == 0) {
			mbrID = "ssg";
		} else if(key%7 == 0) {
			mbrID = "";
			//logger.info("mbr_id empty : {}", key);
		}
		String mbrTypeCD = "intern";
		String fromID = "seoul";
//		if(key%33 == 0) {
//			fromID = "seoul22222";
//		}
		String brandID = "manager";
		String fsid = "fsid000000" + key;
		
		StringBuilder logSB = new StringBuilder();
		logSB.append("{");
		
		logSB.append("\"VISIT_DTS_VAL\": \""+ visitDtsVal +"\",");
		logSB.append("\"MBR_TYPE_CD\": \""+ mbrTypeCD +"\",");
		logSB.append("\"MBR_ID\": \""+ mbrID +"\",");
		logSB.append("\"PROM_ID\": \""+ fromID +"\",");
		logSB.append("\"BRAND_ID\": \""+ brandID +"\",");
		
		logSB.append("\"SID\": \"\",");
		logSB.append("\"FSID\": \""+ fsid +"\",");
		
		
		logSB.append("\"UA_DVIC_KIND_CD\": \"30\",");
		logSB.append("\"DISP_CTG_ID\": \"\",");
		logSB.append("\"MOBIL_APP_VER\": \"\",");
		logSB.append("\"INFLO_SITE_NO\": \"7009\",");
		logSB.append("\"FST_VISIT_FSID\": \"1\",");
		
		logSB.append("\"SSG_DOM_COOKIE_VAL\": \"\",");
		logSB.append("\"BF_PAGE_VISIT_DTS_VAL\": \"\",");
		logSB.append("\"SRCHWD\": \"\",");
		logSB.append("\"RES_DTL_VAL\": \"1\",");
		logSB.append("\"CONN_IP\": \"110.8.182.199\",");
		logSB.append("\"EMART_SALESTR_NO\": \"2034\",");
		logSB.append("\"FST_VISIT_PCID\": \"1\",");
		logSB.append("\"ITEM_ID\": \"\",");
		logSB.append("\"CNTRY_CURR\": \"2451\",");
		
		logSB.append("\"BEST_PRC\": \"\",");
		logSB.append("\"FST_VISIT_PUSH_ID\": \"\",");
		logSB.append("\"ZIPCD\": \"04781\",");
		logSB.append("\"AGE_VAL\": \"\",");
		logSB.append("\"PNSHOP_ID\": \"\",");
		logSB.append("\"CART_REFE_URL\": \"\",");
		logSB.append("\"COMM_SITE_NO\": \"SSG\",");
		logSB.append("\"CONN_CNTRY_CD\": \"KR\",");
		logSB.append("\"SALESTR_NO\": \"\",");
		logSB.append("\"UA_CNTT\": \"Mozilla/5.0 (Linux; Android 8.1.0; SM-J710K Build/M1AJQ; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/81.0.4044.111 Mobile Safari/537.36\",");
		logSB.append("\"FST_VISIT_CKWHERE\": \"1\",");
		logSB.append("\"MOBIL_DOM_YN\": \"1\",");
		logSB.append("\"MOBIL_APP_NO\": \"\",");
		logSB.append("\"URL\": \"http://m.earlymorning.ssg.com/comm/app/appLinkOpenUrl.ssg?mobilAppSvcNo=18&store=Y&internal=&retURL=http%3A%2F%2Fm.earlymorning.ssg.com%2Fmnweb%2Fcommon%2Ferror%2F401.ssg%3FpromId%3D1100518706%26ckwhere%3Dgenius_em1%26abx_tid%3D1587532410854%3A86c55df5-5b7d-4600-a98c-a097c839a68c\",");
		logSB.append("\"SITE_NO\": \"\",");
		logSB.append("\"UA_OS_NM\": \"Android Mobile\",");
		logSB.append("\"PUSH_ID\": \"\",");
		logSB.append("\"REFE_SRCHWD\": \"\",");
		logSB.append("\"REFE_URL\": \"\",");
		logSB.append("\"GOOGLE_CD\": \"\",");
		logSB.append("\"BKWHERE\": \"\",");
		logSB.append("\"CACHE_YN\": \"NoViewCache\",");
		logSB.append("\"MBR_GRD_CD\": \"\",");
		logSB.append("\"TAREA\": \"\",");
		logSB.append("\"PUSH_DTL_ID\": \"\",");
		logSB.append("\"MOBIL_APP_SVC_CNTT\": \"\",");
		logSB.append("\"STG_CTG_ID\": \"\",");
		logSB.append("\"MOBIL_APP_DIV_CD\": \"\",");
		logSB.append("\"GEN_VAL\": \"\",");
		logSB.append("\"ITEM_DTL_SELLPRC\": \"\",");
		logSB.append("\"LOGTYPE\": \"EVERYCLICK\",");
		logSB.append("\"PCID\": \"15875324111850526811144\",");
		logSB.append("\"UA_BROW_NM\": \"CHROME_MOBILE\",");
		logSB.append("\"SRCH_FAIL_YN\": \"\",");
		logSB.append("\"CORNR_CMPT_TGT_ID\": \"\",");
		logSB.append("\"DM_ID\": \"\",");
		logSB.append("\"DOM_ADDR\": \"m.earlymorning.ssg.com\",");
		logSB.append("\"CKWHERE\": \"genius_em1\",");
		logSB.append("\"TRADRS_SALESTR_NO\": \"2154\",");
		logSB.append("\"DUMMY4\": \"\",");
		logSB.append("\"PGM_APL_VER\": \"END-20180207\",");
		logSB.append("\"RST_ITEM_LST\": \"\",");
		logSB.append("\"MOBIL_OS_NM\": \"\",");
		logSB.append("\"CHNL_ID\": \"0000017667\",");
		logSB.append("\"BF_TAREA\": \"\"");
		logSB.append("}");
		return logSB.toString();
	}
	
	private String makeMessage_202004(int key) {
		long curTime = System.currentTimeMillis();
		//logger.info("# time : " + time);
		StringBuilder logSB = new StringBuilder();
		logSB.append("{\"MBR_TYPE_CD\": \"B2C\",\"UA_DVIC_KIND_CD\": \"30\",\"DISP_CTG_ID\": null,\"MOBIL_APP_VER\": null,\"INFLO_SITE_NO\": \"6001\",\"PROM_ID\": null,\"FST_VISIT_FSID\": \"1\",");
		logSB.append("\"FSID\": \"q9c1z9642d1x4580r4d6");
		logSB.append(key);
		logSB.append("\",\"type\": \"tsv\",\"SSG_DOM_COOKIE_VAL\": null,\"BF_PAGE_VISIT_DTS_VAL\": null,");
		logSB.append("\"MBR_ID\": null,\"path\": \"/data02/tsv_logs/moneymall/collection-logger-tsv.log\",\"SRCHWD\": null,\"host\": \"bdtracklog001\",\"RES_DTL_VAL\": \"1\",\"CONN_IP\": \"220.85.69.149\",\"EMART_SALESTR_NO\": \"2034\",\"FST_VISIT_PCID\": \"1\",\"ITEM_ID\": null,\"CNTRY_CURR\": \"2451\",\"SID\": null,\"BEST_PRC\": null,\"FST_VISIT_PUSH_ID\": null,\"ZIPCD\": \"04781\",\"AGE_VAL\": null,\"PNSHOP_ID\": null,\"CART_REFE_URL\": null,\"COMM_SITE_NO\": \"SSG\",\"CONN_CNTRY_CD\": \"KR\",\"SALESTR_NO\": null,\"UA_CNTT\": \"Mozilla/5.0 (Linux; Android 10; SM-G973N Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/81.0.4044.111 Mobile Safari/537.36\",\"FST_VISIT_CKWHERE\": \"1\",\"MOBIL_DOM_YN\": \"1\",\"MOBIL_APP_NO\": null,\"URL\": \"http://m.emart.ssg.com/comm/app/appLinkOpenUrl.ssg?mobilAppSvcNo=1&store=Y&internal=&retURL=http%3A%2F%2Fm.emart.ssg.com%2Fplan%2FplanShop.ssg%3FplanShopId%3D6000282748%26ckwhere%3Demax_cps5%26ck%3D9715469%26sn%3D114606581371%26cid%3D79054177_114606581371%26cb_param1%3DWyQMO9gJg-5963-6416-3326-AADn9UNhrCa3g6eVmVul0HygCgVT5rCjQx-bfe3e25bd83b00f1cf582ea04cf8ff9c041021de9f93bec259af85b667e0ce7a2f67a2fb1559ce22b341759e9fd7f1a5075f2d29a9ed77f37bfa5c8c601fbdeee5e95dae6c40cb6b9f64b87525fe19fe3d1091f79f7ed5981f9bfcab45ef51d385b09f9f8c295e9d8791a662689ab16ab7fd021378c850ae74a2ab3c65a55d00ead3c5cc425cf3824e06045f06001aad\",\"SITE_NO\": null,\"BRAND_ID\": null,\"UA_OS_NM\": \"Android 1.x\",\"PUSH_ID\": null,\"REFE_SRCHWD\": null,\"@version\": \"1\",\"REFE_URL\": null,\"GOOGLE_CD\": null,\"BKWHERE\": null,\"CACHE_YN\": \"NoViewCache\",\"MBR_GRD_CD\": null,\"TAREA\": null,\"PUSH_DTL_ID\": null,\"MOBIL_APP_SVC_CNTT\": null,\"STG_CTG_ID\": null,\"MOBIL_APP_DIV_CD\": null,\"GEN_VAL\": null,\"ITEM_DTL_SELLPRC\": null,\"LOGTYPE\": \"EVERYCLICK\",");
		logSB.append("\"PCID\": \"15877995573977903252");
		logSB.append(key);
		logSB.append("\",\"UA_BROW_NM\": \"CHROME_MOBILE\",\"SRCH_FAIL_YN\": null,");
		logSB.append("\"@timestamp\": \"");
		logSB.append(convertTime(curTime));
		logSB.append("\",\"CORNR_CMPT_TGT_ID\": null,\"DM_ID\": null,\"DOM_ADDR\": \"m.emart.ssg.com\",");
		logSB.append("\"VISIT_DTS_VAL\": \"");
		logSB.append(curTime);
		logSB.append("\",\"CKWHERE\": \"emax_cps5\",\"TRADRS_SALESTR_NO\": \"2154\",\"DUMMY4\": \"null\",\"PGM_APL_VER\": \"END-20180207\",\"RST_ITEM_LST\": null,\"MOBIL_OS_NM\": null,\"CHNL_ID\": \"0000017684\",\"BF_TAREA\": null}");

		return logSB.toString();
	}
	
	private String convertTime(long curTime) {
		return simpleDateFormatZ.format(new Date(curTime));
	}
	
	private void testSSG_01_old() {
		producer = new KafkaProducer<>(getKafkaProperties("192.168.20.201:9092,192.168.20.202:9092,192.168.20.203:9092"));

		String topic = "SSG-TEST-IN-TOPIC";
		int bulkSize = 20_000_000;
		bulkSize = 1_000_000;
		bulkSize = 1;
		String message = "";
		Random roundRandom = new Random();

		long startTime = System.currentTimeMillis();
		long forTime = System.currentTimeMillis();
		for (int i = 1; i <= bulkSize; i++) {

			int key = roundRandom.nextInt(randomInt + 1);
			//key = i;

			message = makeMessage_old(key);

			logger.info("# message : {} ", message);
			
			//performanceTest(message);

			//producer.send(new ProducerRecord<>(topic, message));

			if (i % 10000 == 0) {
				logger.info("# sending count : {} , elapsed : {} ms", i, (System.currentTimeMillis() - forTime));
				forTime = System.currentTimeMillis();
			}
		}

		logger.info("# Elapsed Time : {} ms", (System.currentTimeMillis() - startTime));
		producer.flush();
		producer.close();
	}
	
	private void testSSG_02_Thread() {

		int threadCount = 5;
		for (int i = 1; i <= threadCount; i++) {
			Thread thread = new Thread("Producer Thread # " + i) {
				public void run() {
					testSSG_01_Thread("SSG-TEST-IN-TOPIC", 1_000_000);
				}
			};
			thread.start();
		}
	}

	private void testSSG_01_Thread(String topic, int sendCount) {
		KafkaProducer<Void, String> producerThread = new KafkaProducer<>(
				getKafkaProperties("192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092"));
		String message = "";
		Random roundRandom = new Random();

		long startTime = System.currentTimeMillis();
		long forTime = System.currentTimeMillis();
		for (int i = 1; i <= sendCount; i++) {

			int key = roundRandom.nextInt(99);
			//key = i;
			message = makeMessage_old(key);
			//logger.info("# sending key : {}", key);

			producerThread.send(new ProducerRecord<>(topic, message));

			if (i % 10000 == 0) {
				logger.info("# sending count : {} , elapsed : {} ms", i, (System.currentTimeMillis() - forTime));
				forTime = System.currentTimeMillis();
			}

			//			try {
			//				Thread.sleep(2000);
			//			} catch (InterruptedException e) {
			//				e.printStackTrace();
			//			}
		}

		logger.info("# Elapsed Time : {} ms", (System.currentTimeMillis() - startTime));
		producerThread.flush();
		producerThread.close();
	}

	private void performanceTest(String msg) {
		//logger.info("# msg : {} ", msg);
		String[] data = msg.split("\t", -1);
		try {
			if (data.length != 71) {
				logger.warn("[parseData] (Skipped) Input data may be bad or malformed! expected size(), input size("
						+ data.length + ")");

				return;
			}
			StringBuilder timestampStringValueSB = new StringBuilder();
			StringBuilder groupKeySB = new StringBuilder();
			String groupKey = null;
			boolean hasGroupKey = false;
			Object[] values = new Object[data.length];
			for (int i = 0; i < values.length; i++) {
				values[i] = convertValueWithType(data[i], "STRING");
				
				// timestamp
				if (data[i].contains("15845055")) {
					timestampStringValueSB.append(values[i]);
				}
	
				// groupKey
				if (data[i].contains("ssg")) {
					groupKeySB.append((String) values[i]);
					hasGroupKey = true;
				}
			}
			data = new String[1];
			
			long originalTimestamp = setTimestampInExtEvent("none", timestampStringValueSB.toString());
			if (originalTimestamp == -1) {
				return;
			}
			
			if (hasGroupKey) {
				groupKey = groupKeySB.toString();
			}
			
		} catch (Throwable _th) {
			logger.error("[parseData] (Skipped) error in parsing and sending data. original data: ");
			return;
		}
		//logger.info("# length : {} ", data.length);
	}

	/*
	public Tuple2<String, ExtEvent> parseData(String delimitedData) {
		//long startT = System.nanoTime();
	
		String[] data = delimitedData.split("/t", -1);
	
		try {
	
			if (data.length != 10) {
				logger.warn("[parseData] (Skipped) Input data may be bad or malformed! expected size("
						+ bodyTableInfo.getColumnInfo().size() + "), input size(" + data.length + "). \ninput data: "
						+ delimitedData);
	
				return null;
			}
	
			Object[] values = new Object[bodyTableInfo.getColumnInfo().size()];
	
			ColumnInfo columnInfo = null;
			StringBuilder timestampStringValueSB = new StringBuilder();
			StringBuilder groupKeySB = new StringBuilder();
			String groupKey = null;
			boolean hasGroupKey = false;
	
			for (int i = 0; i < values.length; i++) {
				columnInfo = bodyTableInfo.getColumnInfo().get(i);
				values[i] = convertValueWithType(data[i], columnInfo.getColType());
	
				// timestamp
				if (this.timestampInfo != null
						&& this.timestampInfo.getTimestampFields().contains(columnInfo.getKeyField())) {
					timestampStringValueSB.append(values[i]);
				}
	
				// groupKey
				if (1 > 0
						&& true) {
					groupKeySB.append((String) values[i]);
					hasGroupKey = true;
				}
			}
			// 필요한가?
			data = new String[1];
	
			long originalTimestamp = setTimestampInExtEvent(null, timestampStringValueSB.toString());
			if (originalTimestamp == -1) {
				return null;
			}
	
			if (hasGroupKey) {
				groupKey = groupKeySB.toString();
			}
	
			return new Tuple2<>(groupKey, new ExtEvent(originalTimestamp, values));
	
		} catch (Throwable _th) {
			logger.error("[parseData] (Skipped) error in parsing and sending data. original data: " + delimitedData);
			return null;
		}
	}
	*/
	
	private String makeMessage_old(int key) {
		long time = System.currentTimeMillis();
		//logger.info("# time : " + time);
		StringBuilder logSB = new StringBuilder();
		logSB.append(time);
		logSB.append(append_delimiter);
		logSB.append("http://www.ssg.net/productKey=");
		if (key == randomInt) {
			logSB.append(time + "_");
		}
		logSB.append(key);
		logSB.append(append_delimiter);
		//logSB.append("10.88.10.141");
		logSB.append(append_delimiter);
		logSB.append(getJSON());
		logSB.append(append_delimiter);
		logSB.append(key);

		makeDummyColValue(logSB);

		logSB.append(append_delimiter);
		logSB.append("2020ver");

		return logSB.toString();
	}

	private StringBuilder makeDummyColValue(StringBuilder logSB) {
		for (int i = 1; i <= 65; i++) {
			logSB.append(append_delimiter);
			logSB.append("VALUE_").append(i);
		}

		return logSB;
	}

	private String getJSON() {
		//return "";
		//return "{\"baseProperties\":{\"siteNo\":\"01\"";
		return "{\"baseProperties\":{\"siteNo\":\"01\",\"pcid\":\"1580238754192087541820760\",\"mbrId\":\"00109843769871\",\"ckWjere\":\"e_ggbr\",\"dmId\":\"\",\"sessionId\":\"7201554ffc8648c287196b531e1fcb5c\",\"referer\":\"http://192.168.10.231/magento/checkout/cart/\",\"ip\":\"103.12.220.0\",\"browser\":\"Mozilla/5.0 (Windows NT 10.0;Win64;x64)\",\"eventTyp\":\"ORFER_VIEW_EVENT\",\"currentpageUrl\":\"/order/orderProcess.ssg\",\"tgMediaCd\":\"10\",\"version\":\"1.1\",\"timestamp\":1583978846962,\"previousPageTIId\":\"\"},\"sudOrderItemList\":[{\"ordItemSeq\":\"1\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"2\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"3\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"4\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"5\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"6\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"7\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"8\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"9\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"},{\"ordItemSeq\":\"10\",\"unitPrice\":1480,\"cartItemSiteNo\":\"7009\",\"masterCartId\":\"30330333372\",\"cartIf\":\"30330333372\",\"uitemId\":\"00000\",\"ordQty\":1,\"itemId\":\"1000021368978984\",\"infoSiteNo\":\"70090\",\"ordItemReqTypeCd\":\"10\",\"ordItemTypeCd\":\"10\",\"itemChrcDtlCd\":\"10\"}],\"ordNo\":\"2020021169B3F8\",\"ordCmpIDts\":\"2020-02-11 17:00:03\"}";
	}

	private void testRandom() {
		Random roundRandom = new Random();
		int key = roundRandom.nextInt(100) + 1;
		logger.info("# key : {}", key);
	}

	private void convertNanoToMicros() {
		long startT = System.nanoTime();
		System.out.println("[sendMessagesToKafka] eventSize : {} , elapsed : "
				+ TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startT));
	}

	protected Object convertValueWithType(String value, String columnType) {
		switch (columnType.toUpperCase()) {
		case "VARCHAR":
		case "STRING":
			if (value == null || value.trim().equals(""))
				value = "";
			return value;
		case "INT":
			if (value == null || value.trim().equals(""))
				value = "0";
			return Integer.parseInt(value);
		case "LONG":
			if (value == null || value.trim().equals(""))
				value = "0";
			return Long.parseLong(value);
		case "FLOAT":
			if (value == null || value.trim().equals(""))
				value = "0";
			return Float.parseFloat(value);
		case "DOUBLE":
			if (value == null || value.trim().equals(""))
				value = "0";
			return Double.parseDouble(value);
		case "BOOLEAN":
			if (value == null || value.trim().equals(""))
				value = "false";
			return Boolean.parseBoolean(value);
		default:
			throw new IllegalArgumentException("Invalid field type: " + columnType);
		}
	}
	
	protected long setTimestampInExtEvent(String timestampInfo, String timestampStringValue) {
		long extEventTimestamp = System.currentTimeMillis();

		try {
			if (timestampInfo != null && timestampStringValue.length() > 0) {
				if (timestampInfo.equals("yyyy")) {
					//extEventTimestamp = timestampInfo.getTimestampDateFormat().parse(timestampStringValue.toString())
					//		.getTime();
				} else {
					extEventTimestamp = Long.parseLong(timestampStringValue);
				}
			} else {
				// SP Processing
				extEventTimestamp = System.currentTimeMillis();
			}
		} catch (Exception pe) {
			logger.warn("▶[setTimestampInExtEvent] Invalid (DateFormat-ParseException): [pattern] ");

			extEventTimestamp = -1;
		}

		return extEventTimestamp;
	}
	
	private void performanceTest() {
		int initCnt = 100;
		performanceParseData(new ArrayList<String>(), initCnt);
		performanceParseData(new LinkedList<String>(), initCnt);
		logger.info("=======================================");
		performanceParseData(new ArrayList<String>(), initCnt * 10);
		performanceParseData(new LinkedList<String>(), initCnt * 10);
		logger.info("=======================================");
		performanceParseData(new ArrayList<String>(), initCnt * 100);
		performanceParseData(new LinkedList<String>(), initCnt * 100);
		logger.info("============= end ====================");
	}
	private void performanceParseData(List<String> outList, int limitCnt) {
		long startT = System.nanoTime();
		
		int cnt = 0;
		String msg = makeMessage(1);
		while(cnt < limitCnt) {
			performanceTest(msg);
			outList.add(msg);
			cnt++;
		}
		outList.iterator();
		logger.info("[LinkedList] limitCnt : {} , elapsed : {} ms", limitCnt,
				(System.nanoTime() - startT) / 1_000_000D);
	}
}