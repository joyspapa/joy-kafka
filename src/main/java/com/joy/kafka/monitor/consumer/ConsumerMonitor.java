package com.joy.kafka.monitor.consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminClient;
import kafka.admin.ConsumerGroupCommand;
import kafka.coordinator.group.GroupOverview;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class ConsumerMonitor {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerMonitor.class);

	/*
	 * https://stackoverflow.com/questions/50527319/kafka-consumer-list-api
	 */
	public List<String> getConsumersByShellCommand(String brokers) {
		String[] arg = new String[3];
		arg[0] = "--bootstrap-server";
		arg[1] = brokers; //" localhost:9092";
		arg[2] = "--list";

		ConsumerGroupCommand.ConsumerGroupCommandOptions checkArgs = new ConsumerGroupCommand.ConsumerGroupCommandOptions(
				arg);
		checkArgs.checkArgs();
		scala.collection.immutable.List<String> result = new ConsumerGroupCommand.KafkaConsumerGroupService(checkArgs)
				.listGroups();

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Iterator<String> re = JavaConverters.asJavaIterator(result.iterator());
		while (re.hasNext()) {
			logger.debug("groupID : " + re.next());
		}

		return null;
	}

	/*
	 * https://github.com/rusonding/kafka-monitor/blob/master/common/src/main/java/com/kafka/monitor/common/ConsumerGroupCommand.java
	 *
	 */
	public List<String> getConsumersByAdminClient(String brokers) {
		List<String> list = new ArrayList<String>();
		AdminClient adminClient = null;
		scala.collection.immutable.Map<Node, scala.collection.immutable.List<GroupOverview>> nodeListMap = null;
		try {
			adminClient = createAdminClient(brokers);
			nodeListMap = adminClient.listAllConsumerGroups();

		} finally {
			adminClient.close();
		}

		Map<Node, scala.collection.immutable.List<GroupOverview>> map = JavaConverters.mapAsJavaMap(nodeListMap);

		for (Node keyNode : map.keySet()) {
			logger.debug("Node : " + keyNode.toString());
			
			List<GroupOverview> re = JavaConverters.bufferAsJavaList(map.get(keyNode).toBuffer());
			for (GroupOverview obj : re) {
				logger.debug("groupID : " + obj.groupId());
				list.add(obj.groupId());
			}
		}

		return list;
	}

	private AdminClient createAdminClient(String brokers) {
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
		AdminClient adminClient = AdminClient.create(props);
		return adminClient;
	}
}
