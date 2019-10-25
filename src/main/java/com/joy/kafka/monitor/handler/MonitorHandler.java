package com.joy.kafka.monitor.handler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.factory.KafkaAdminClientFactory;
import com.joy.kafka.monitor.factory.KafkaConsumerFactory;

import kafka.admin.ConsumerGroupCommand;
import kafka.coordinator.group.GroupOverview;
import scala.collection.JavaConverters;

public class MonitorHandler {
	private static final Logger logger = LoggerFactory.getLogger(MonitorHandler.class);

	private String clientID;
	private String brokers;
	
	public MonitorHandler(String clientID, String brokers) {
		this.clientID = clientID;
		this.brokers = brokers;
	}
	
	public String getClientID() {
		return clientID;
	}
	
	public String getBrokers() {
		return brokers;
	}
	
	protected long getLogEndOffset(String groupID, String topic, int partition) {
		TopicPartition topicPartition = new TopicPartition(topic, partition);
		List<TopicPartition> partitions = new ArrayList<TopicPartition>();
		partitions.add(topicPartition);
		KafkaConsumerFactory.getKafkaConsumer(getBrokers(), groupID).assign(partitions);
		KafkaConsumerFactory.getKafkaConsumer(getBrokers(), groupID).seekToEnd(partitions);
		long logEndOffset = KafkaConsumerFactory.getKafkaConsumer(getBrokers(), groupID).position(topicPartition);

		return logEndOffset;
	}
	
	protected List<PartitionInfo> getPartitionInfo(String topic) {
		return KafkaConsumerFactory.getKafkaConsumer(getBrokers()).partitionsFor(topic);
	}
	
	protected String getTopicNamebyGroupID(String groupID) {
		scala.collection.immutable.Map<TopicPartition, Object> nodeListMap = KafkaAdminClientFactory
				.getAdminClient(getBrokers()).listGroupOffsets(groupID);
		
		// scala 2.12
		// JavaConverters.mapAsJavaMap(nodeListMap).keySet()
		for (TopicPartition t : JavaConverters.mapAsJavaMapConverter(nodeListMap).asJava().keySet()) {
			return t.topic();
		}
		return "none";
	}
	
	/*
	 * https://github.com/rusonding/kafka-monitor/blob/master/common/src/main/java/com/kafka/monitor/common/ConsumerGroupCommand.java
	 *
	 */
	@Deprecated
	private List<String> getConsumersByAdminClient_old(String brokers) {
		List<String> list = new ArrayList<String>();

		scala.collection.immutable.Map<Node, scala.collection.immutable.List<GroupOverview>> nodeListMap = null;
		try {
			nodeListMap = KafkaAdminClientFactory.getAdminClient(brokers).listAllConsumerGroups();

			// scala 2.12
			// JavaConverters.mapAsJavaMap(nodeListMap).keySet()
			Map<Node, scala.collection.immutable.List<GroupOverview>> map = JavaConverters.mapAsJavaMapConverter(nodeListMap).asJava();

			for (Node keyNode : map.keySet()) {
				logger.debug("Node : " + keyNode.toString());
				
				// scala 2.12
				// List<GroupOverview> re = JavaConverters.bufferAsJavaList(map.get(keyNode).toBuffer());
				//for (GroupOverview obj : re) {
				//	logger.debug("groupID : " + obj.groupId());
				//	list.add(obj.groupId());
				//}
				List<Object> re = JavaConverters.bufferAsJavaListConverter(map.get(keyNode).toBuffer()).asJava();
				for (Object obj : re) {
					logger.debug("groupID : " + ((GroupOverview)obj).groupId());
					list.add(((GroupOverview)obj).groupId());
				}
			}
		} catch (Throwable ex) {
			logger.warn("AdminClient creating warn : ", ex);
		}

		return list;
	}

	/*
	 * https://stackoverflow.com/questions/50527319/kafka-consumer-list-api
	 */
	private List<String> getConsumerListByShellCommand(String brokers) {
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

		// scala 2.12
		// Iterator<String> re = JavaConverters.asJavaIterator(result.iterator());
		Iterator<String> re = JavaConverters.asJavaIteratorConverter(result.iterator()).asJava();
		while (re.hasNext()) {
			logger.debug("groupID : " + re.next());
		}

		return null;
	}
}
