package com.joy.kafka.monitor.handler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.factory.KafkaAdminClientFactory;
import com.joy.kafka.monitor.factory.KafkaConsumerFactory;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.handler.vo.OffsetVO;
import com.joy.kafka.monitor.util.DateTimeUtils;

import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerGroupSummary;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.coordinator.group.GroupOverview;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

public class ConsumerMonitorHandler extends MonitorAbstract {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerMonitorHandler.class);

	public ConsumerMonitorHandler(String brokers) {
		super(brokers);
	}

	public List<ConsumerGroupVO> getConsumerOffsets() {
		List<String> groupIDList = getConsumerList();
		List<ConsumerGroupVO> consumerOffsetList = new ArrayList<ConsumerGroupVO>();
		
		if (!groupIDList.isEmpty()) {
			ConsumerGroupVO consumerGroupVO = null;
			for (String groupID : groupIDList) {
				consumerGroupVO = getConsumerOffsets(groupID);
				if (consumerGroupVO != null) {
					consumerOffsetList.add(consumerGroupVO);
				}
			}
		}
		return consumerOffsetList;
	}

	public ConsumerGroupVO getConsumerOffsets(String groupID) {
		List<AdminClient.ConsumerSummary> runningConsumerList = getConsumerGroupSummary(groupID);

		if (runningConsumerList == null) {
			return null;
		}
		
		//logger.debug("Running Consumer size() : {} of grouptID : {}", runningConsumerList.size(), groupID);

		String topic = null;
		Map<Integer, PartitionInfo> partitionInfoMap = null;
		ConsumerGroupVO consumerGroupVO = new ConsumerGroupVO();
		for (AdminClient.ConsumerSummary summary : runningConsumerList) {
			Buffer<TopicPartition> assignmentBuffer = summary.assignment().toBuffer();
			List<TopicPartition> topicPartitions = JavaConversions.bufferAsJavaList(assignmentBuffer);

			for (TopicPartition topicPartition : topicPartitions) {
				if (topic == null) {
					topic = topicPartition.topic();
					partitionInfoMap = getPartitionInfoMap(topic);
				}

				OffsetAndMetadata committed = KafkaConsumerFactory.getKafkaConsumer(getBrokers(), groupID)
						.committed(new TopicPartition(topic, topicPartition.partition()));
				if (committed != null) {
					long endOffset = getLogEndOffset(groupID, topic, topicPartition.partition());

					OffsetVO offsetVO = new OffsetVO();
					offsetVO.setPartition(topicPartition.partition()).setHost(summary.host()).setEndOffset(endOffset)
							.setCommittedOffset(committed.offset());
					//offsetVO.setConsumerID(summary.consumerId()).setClientID(summary.clientId());

					if (partitionInfoMap != null) {
						offsetVO.setLeader(partitionInfoMap.get(topicPartition.partition()).leader().host())
								.setReplicas(partitionInfoMap.get(topicPartition.partition()).replicas());
					}

					consumerGroupVO.addOffsetList(offsetVO);
					consumerGroupVO.addEndOffsetAll(endOffset);
					consumerGroupVO.addCommittedOffsetAll(committed.offset());
				}
			}
		}

		consumerGroupVO.setTopic(topic);
		consumerGroupVO.setGroupID(groupID);
		consumerGroupVO.setCreateDT(DateTimeUtils.getNormalDate());
		
		return consumerGroupVO;

	}

	/*
	 * https://www.codota.com/code/java/methods/kafka.admin.AdminClient/listAllConsumerGroupsFlattened
	 */
	public List<String> getConsumerList() {
		List<String> groupList = new ArrayList<String>();

		scala.collection.immutable.List<GroupOverview> groupAll = null;
		try {
			groupAll = KafkaAdminClientFactory.getAdminClient(getBrokers()).listAllGroupsFlattened();

			scala.collection.Iterator<GroupOverview> iterator = groupAll.iterator();
			String groupID = null;
			while (iterator.hasNext()) {
				groupID = iterator.next().groupId();
				groupList.add(groupID);
			}

		} catch (Throwable ex) {
			logger.warn("getConsumerList warn : ", ex);
		}

		return groupList;
	}

	/*
	 * 실제 RUNNING중인 consumer 만 리턴된다.
	 * 연결되어 있지 않으면 null 리턴 
	 */
	private List<AdminClient.ConsumerSummary> getConsumerGroupSummary(String groupID) {
		ConsumerGroupSummary ConsumerGroupSummary = KafkaAdminClientFactory.getAdminClient(getBrokers())
				.describeConsumerGroup(groupID, 1000);
		scala.collection.immutable.List<ConsumerSummary> consumerSummaryList = ConsumerGroupSummary.consumers().get();

		if (consumerSummaryList == null || consumerSummaryList.size() == 0) {
			logger.warn("[getConsumerGroupSummary] consumer({}) is not running !", groupID);
			return null;
		}

		Buffer<AdminClient.ConsumerSummary> buffer = consumerSummaryList.toBuffer();
		return JavaConversions.bufferAsJavaList(buffer);
	}
	
	protected Map<Integer, PartitionInfo> getPartitionInfoMap(String topic) {
		return getPartitionInfo(topic).stream()
				.collect(Collectors.toMap(PartitionInfo::partition, partition -> partition));
	}
}
