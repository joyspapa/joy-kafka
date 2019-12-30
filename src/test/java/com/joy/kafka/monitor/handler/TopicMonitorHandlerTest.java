package com.joy.kafka.monitor.handler;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;

public class TopicMonitorHandlerTest {
	private static final Logger logger = LoggerFactory.getLogger(TopicMonitorHandlerTest.class);

	String brokers = "192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092";
	String topicName = "WEBCLICKBASETOPOLOGY-ENT7724-SP-OUT01-TOPIC";

	@Test
	public void getOffsetCountForTopic() {
		
		for (int i = 0; i < 5; i++) {
			ConsumerGroupVO result = new TopicMonitorHandler(null, brokers).getTopicOffsets(topicName);
			if (result != null) {
				logger.debug("offsetCount : {}", result.getEndOffsetAll());
			}
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
