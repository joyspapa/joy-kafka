package com.joy.kafka.monitor.rest.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.handler.TopicMonitorHandler;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.util.ViewHandler;

import io.vertx.core.AbstractVerticle;

public class ResponseService extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(ResponseService.class);
	
	@Override
	public void start() {
		vertx.eventBus().consumer("service-to-response", message -> {
			logger.debug("a message received from RequestService : " + message.body());

			try {
				String brokers = "192.168.10.87:9092";
				TopicMonitorHandler topicMonitor = new TopicMonitorHandler(brokers);
				List<ConsumerGroupVO> topicOffsetList = topicMonitor.getTopicOffsets();
				
				String responseString = ViewHandler.showByTopicOffsetList(topicOffsetList);
				//System.out.print(responseString);
				
				//logger.debug("a result-string reply to RequestService : " + responseString);

				message.reply(responseString);
			} catch (Exception e) {
				logger.error("*** convert error.", e);
				message.fail(500, e.toString());
			}
		});
	}
}
