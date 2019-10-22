package com.joy.kafka.monitor.rest.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.handler.ConsumerMonitorHandler;
import com.joy.kafka.monitor.handler.TopicMonitorHandler;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.rest.vo.ResponseVO;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

public class ResponseService extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(ResponseService.class);

	@Override
	public void start() {
		vertx.eventBus().<JsonObject> consumer(Constant.bus_response_type_json, message -> {
			try {
				String viewtype = message.body().getString("viewtype");
				String id = message.body().getString("id");
				String responseString = "";
				
				switch (viewtype) {
				case "consumer":
					if(!id.equals("none")) {
						List<ConsumerGroupVO> list = new ArrayList<ConsumerGroupVO>();
						list.add(new ConsumerMonitorHandler(Constant.brokers).getConsumerOffsets(id));
						
						responseString = new ResponseVO(list).toString();
					} else {
						responseString = new ResponseVO(new ConsumerMonitorHandler(Constant.brokers).getConsumerListOffsets())
								.toString();
					}
					break;
				case "consumerAll":
					responseString = new ResponseVO(new ConsumerMonitorHandler(Constant.brokers).getConsumerListOffsets(true))
							.toString();
					break;
				case "deploy":
					if(!id.equals("none")) {
						responseString = new ResponseVO(new ConsumerMonitorHandler(Constant.brokers).getConsumerListOffsetsByDeploy(id))
								.toString();
					} else {
						responseString = new ResponseVO(new ConsumerMonitorHandler(Constant.brokers).getDeployList())
								.toString();
					}
					break;
				default:
					if(!id.equals("none")) {
						List<ConsumerGroupVO> list = new ArrayList<ConsumerGroupVO>();
						list.add(new TopicMonitorHandler(Constant.brokers).getTopicOffsets(id));
						
						responseString = new ResponseVO(list).toString();
					} else {
						responseString = new ResponseVO(new TopicMonitorHandler(Constant.brokers).getTopicOffsets())
								.toString();
					}
					break;
				}
				
				//logger.debug("a result-string reply to ResponseService : \n" + responseString);
				message.reply(responseString);

			} catch (Throwable e) {
				logger.error("{} error.", Constant.bus_response_type_json, e);
				message.fail(500, e.toString());
			}
		});
	}
}
