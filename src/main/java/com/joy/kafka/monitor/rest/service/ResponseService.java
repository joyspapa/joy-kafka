package com.joy.kafka.monitor.rest.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.config.ViewType;
import com.joy.kafka.monitor.handler.ConsumerMonitorHandler;
import com.joy.kafka.monitor.handler.TopicMonitorHandler;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.report.ReportHandler;
import com.joy.kafka.monitor.rest.vo.MonitorResponseVO;
import com.joy.kafka.monitor.rest.vo.ReportResponseVO;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

public class ResponseService extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(ResponseService.class);

	@Override
	public void start() {
		vertx.eventBus().<JsonObject> consumer(Constant.bus_response_type_json, message -> {
			try {
				String clientip = message.body().getString("clientip");
				ViewType viewtype = ViewType.findViewType(message.body().getString("viewtype"));
				String id = message.body().getString("id");
				String responseString = "";
				
				if(viewtype != ViewType.Report) {
					ReportHandler.clearOffsetReport(clientip);
				}
				
				switch (viewtype) {
				case Consumer:
					if(!id.equals("none")) {
						List<ConsumerGroupVO> list = new ArrayList<ConsumerGroupVO>();
						list.add(new ConsumerMonitorHandler(clientip, Constant.brokers).getConsumerOffsets(id));
						
						responseString = new MonitorResponseVO(list).toString();
					} else {
						responseString = new MonitorResponseVO(new ConsumerMonitorHandler(clientip, Constant.brokers).getConsumerListOffsets())
								.toString();
					}
					break;
				case ConsumerAll:
					responseString = new MonitorResponseVO(new ConsumerMonitorHandler(clientip, Constant.brokers).getConsumerListOffsets(true))
							.toString();
					break;
				case Report:
					responseString = new ReportResponseVO(new ConsumerMonitorHandler(clientip, Constant.brokers).getConsumerOffsetsReport(id))
							.toString();
					break;
				case Deploy:
					if(!id.equals("none")) {
						responseString = new MonitorResponseVO(new ConsumerMonitorHandler(clientip, Constant.brokers).getConsumerListOffsetsByDeploy(id))
								.toString();
					} else {
						responseString = new MonitorResponseVO(new ConsumerMonitorHandler(clientip, Constant.brokers).getDeployList())
								.toString();
					}
					break;
				default:
					if(!id.equals("none")) {
						List<ConsumerGroupVO> list = new ArrayList<ConsumerGroupVO>();
						list.add(new TopicMonitorHandler(clientip, Constant.brokers).getTopicOffsets(id));
						
						responseString = new MonitorResponseVO(list).toString();
					} else {
						responseString = new MonitorResponseVO(new TopicMonitorHandler(clientip, Constant.brokers).getTopicOffsets())
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
