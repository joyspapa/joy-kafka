package com.joy.kafka.monitor.handler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.factory.KafkaAdminClientFactory;
import com.joy.kafka.monitor.factory.KafkaConsumerFactory;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.handler.vo.OffsetVO;

import kafka.admin.ConsumerGroupCommand;
import kafka.coordinator.group.GroupOverview;
import scala.collection.JavaConverters;

public abstract class MonitorAbstract {
	private static final Logger logger = LoggerFactory.getLogger(MonitorAbstract.class);

	private String brokers;
	
	public MonitorAbstract(String brokers) {
		this.brokers = brokers;
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

			Map<Node, scala.collection.immutable.List<GroupOverview>> map = JavaConverters.mapAsJavaMap(nodeListMap);

			for (Node keyNode : map.keySet()) {
				logger.debug("Node : " + keyNode.toString());

				List<GroupOverview> re = JavaConverters.bufferAsJavaList(map.get(keyNode).toBuffer());
				for (GroupOverview obj : re) {
					logger.debug("groupID : " + obj.groupId());
					list.add(obj.groupId());
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

		Iterator<String> re = JavaConverters.asJavaIterator(result.iterator());
		while (re.hasNext()) {
			logger.debug("groupID : " + re.next());
		}

		return null;
	}
}
