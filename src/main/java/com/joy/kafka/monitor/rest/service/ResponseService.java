package com.joy.kafka.monitor.rest.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.admin.handler.KafkaAdminHandler;
import com.joy.kafka.monitor.config.AdminCommand;
import com.joy.kafka.monitor.config.Constant;
import com.joy.kafka.monitor.config.ViewType;
import com.joy.kafka.monitor.handler.ConsumerMonitorHandler;
import com.joy.kafka.monitor.handler.TopicMonitorHandler;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.report.ReportHandler;
import com.joy.kafka.monitor.rest.vo.AdminResponseVO;
import com.joy.kafka.monitor.rest.vo.MonitorResponseVO;
import com.joy.kafka.monitor.rest.vo.ReportResponseVO;
import com.joy.kafka.monitor.rest.vo.ResponseVO;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

public class ResponseService extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(ResponseService.class);

	@Override
	public void start() {
		monitorService();
		adminService();
	}
	
	private void monitorService() {
		vertx.eventBus().<JsonObject> consumer(Constant.bus_kafka_monitor, message -> {
			String responseString = null;
			try {
				String clientIp = message.body().getString("clientIp");
				ViewType viewType = ViewType.findViewType(message.body().getString("viewType"));
				String id = (message.body().getString("id") == null)? "none":message.body().getString("id");
				
				if(viewType != ViewType.Report && viewType != ViewType.ReportChart) {
					ReportHandler.clearOffsetReport(clientIp);
				}
				
				switch (viewType) {
				case Consumer:
					if(!id.equals("none")) {
						List<ConsumerGroupVO> list = new ArrayList<ConsumerGroupVO>();
						list.add(new ConsumerMonitorHandler(clientIp, Constant.brokers).getConsumerOffsets(id));
						
						responseString = new MonitorResponseVO(viewType, list).toString();
					} else {
						responseString = new MonitorResponseVO(viewType, new ConsumerMonitorHandler(clientIp, Constant.brokers).getConsumerListOffsets())
								.toString();
					}
					break;
				case ConsumerAll:
					responseString = new MonitorResponseVO(viewType, new ConsumerMonitorHandler(clientIp, Constant.brokers).getConsumerListOffsets(true))
							.toString();
					break;
				case Report:
				case ReportChart:
					responseString = new ReportResponseVO(viewType, new ConsumerMonitorHandler(clientIp, Constant.brokers).getConsumerOffsetsReport(id))
							.toString();
					break;
				case Deploy:
					if(!id.equals("none")) {
						responseString = new MonitorResponseVO(ViewType.ConsumerAll, new ConsumerMonitorHandler(clientIp, Constant.brokers).getConsumerListOffsetsByDeploy(id))
								.toString();
					} else {
						responseString = new MonitorResponseVO(viewType, new ConsumerMonitorHandler(clientIp, Constant.brokers).getDeployList())
								.toString();
					}
					break;
				case Topic:
					if(!id.equals("none")) {
						List<ConsumerGroupVO> list = new ArrayList<ConsumerGroupVO>();
						list.add(new TopicMonitorHandler(clientIp, Constant.brokers).getTopicOffsets(id));
						
						responseString = new MonitorResponseVO(viewType, list).toString();
					} else {
						responseString = new MonitorResponseVO(viewType, new TopicMonitorHandler(clientIp, Constant.brokers).getTopicOffsets())
								.toString();
					}
					break;
				default:
					ResponseVO resVO = new ResponseVO(ViewType.None);
					resVO.setSuccess(false);
					resVO.setViewType("the viewType must be one of the following " + ViewType.values() + "!, Request viewType=" + message.body().getString("viewtype"));
					responseString = resVO.toString();
					
					break;
				}
			} catch (Throwable e) {
				logger.error("{} error.", Constant.bus_kafka_monitor, e);
				ResponseVO resVO = new ResponseVO(ViewType.None);
				resVO.setSuccess(false);
				resVO.setViewType(e.toString());
				responseString = resVO.toString();
				//message.fail(500, e.toString());
			}
			message.reply(responseString);
		});
	}
	
	private void adminService() {
		vertx.eventBus().<JsonObject> consumer(Constant.bus_kafka_admin, message -> {
			AdminResponseVO resVO = new AdminResponseVO(ViewType.Admin);
			try {
				//String clientip = message.body().getString("clientip");
				AdminCommand command = AdminCommand.findViewType(message.body().getString("command"));
				String topicName = message.body().getString("topicName");
				
				switch (command) {
				case Create:
					short numPartitions = Short.parseShort(message.body().getString("numPartitions"));
					short replicaFactor = Short.parseShort(message.body().getString("replicaFactor"));
					
					KafkaAdminHandler.create(Constant.brokers).createTopic(topicName, numPartitions, replicaFactor);
					resVO.setCommand(command);
					resVO.setMessage(String.format("Created Topic : %s , partiton : %d , replicaFactor : %d", topicName,  numPartitions, replicaFactor));
					break;
				case Delete:
					KafkaAdminHandler.create(Constant.brokers).deleteTopic(topicName);
					resVO.setMessage(String.format("Deleted Topic : %s", topicName));
					resVO.setCommand(command);
					break;
				case List:
					resVO.setListTopic(KafkaAdminHandler.create(Constant.brokers).listTopics());
					resVO.setMessage("Topic List");
					resVO.setCommand(command);
					break;
				default:
					resVO.setSuccess(false);
					resVO.setMessage("the type must be one of the following [ create | delete ]. type=" + message.body().getString("command"));
					resVO.setCommand(AdminCommand.None);
					break;
				}
			} catch (Throwable e) {
				logger.error("{} error.", Constant.bus_kafka_admin, e);
				resVO.setSuccess(false);
				resVO.setMessage(e.toString());
				//message.fail(500, e.toString());
			}
			
			message.reply(resVO.toString());
		});
	}
}
