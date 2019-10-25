package com.joy.kafka.monitor.handler;

import java.util.ArrayList;
import java.util.HashMap;
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
import com.joy.kafka.monitor.report.ReportHandler;
import com.joy.kafka.monitor.report.ReportOffsetVO;
import com.joy.kafka.monitor.report.ReportVO;
import com.joy.kafka.monitor.util.DateTimeUtils;

import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerGroupSummary;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.coordinator.group.GroupOverview;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

public class ConsumerMonitorHandler extends MonitorHandler {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerMonitorHandler.class);

	public ConsumerMonitorHandler(String clientID, String brokers) {
		super(clientID, brokers);
	}

	public List<ConsumerGroupVO> getConsumerListOffsets() {
		return getConsumerListOffsets(false);
	}

	public List<ConsumerGroupVO> getConsumerListOffsets(boolean isGetAllConsumer) {
		List<String> groupIDList = getConsumerList();
		List<ConsumerGroupVO> consumerOffsetList = new ArrayList<ConsumerGroupVO>();

		for (String groupID : groupIDList) {
			ConsumerGroupVO returnConsumerGroupVO = getConsumerOffsets(groupID, isGetAllConsumer, false);
			if (returnConsumerGroupVO != null) {
				consumerOffsetList.add(returnConsumerGroupVO);
			}
		}

		return consumerOffsetList;
	}

	public ConsumerGroupVO getConsumerOffsets(String groupID) {
		return getConsumerOffsets(groupID, false, false);
	}

	public ConsumerGroupVO getConsumerOffsets(String groupID, boolean isGetAllConsumer, boolean isReport) {
		List<AdminClient.ConsumerSummary> runningConsumerList = getConsumerGroupSummary(groupID);

		ConsumerGroupVO consumerGroupVO = new ConsumerGroupVO();

		if (runningConsumerList == null) {
			// Running 상태가 아닌 Consumer 도 포함시킨다.
			if (isGetAllConsumer) {
				String topicName = getTopicNamebyGroupID(groupID);
				consumerGroupVO = new TopicMonitorHandler(getClientID(), getBrokers()).getTopicOffsets(topicName);
				consumerGroupVO.setGroupID(groupID);
			} else {
				return null;
			}

		} else {
			//logger.debug("Running Consumer size() : {} of grouptID : {}", runningConsumerList.size(), groupID);

			String topic = null;
			Map<Integer, PartitionInfo> partitionInfoMap = null;

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
						offsetVO.setPartition(topicPartition.partition()).setHost(summary.host())
								.setEndOffset(endOffset).setCommittedOffset(committed.offset());
						//offsetVO.setConsumerID(summary.consumerId()).setClientID(summary.clientId());

						if (partitionInfoMap != null) {
							offsetVO.setLeader(partitionInfoMap.get(topicPartition.partition()).leader().host())
									.setReplicas(partitionInfoMap.get(topicPartition.partition()).replicas());
						}

						consumerGroupVO.addOffsetList(offsetVO);
						consumerGroupVO.addEndOffsetAll(endOffset);
						consumerGroupVO.addCommittedOffsetAll(committed.offset());

						// for report
						if (isReport) {
							ReportHandler.addOffsetReport(getClientID(), groupID, topicPartition.partition(),
									new ReportOffsetVO(endOffset, committed.offset()));
						}
					}
				}
			}

			consumerGroupVO.setTopic(topic);
			consumerGroupVO.setGroupID(groupID);
			consumerGroupVO.setConsumerRunning(true);
			consumerGroupVO.setCreateDT(DateTimeUtils.getNormalDate());
		}

		return consumerGroupVO;
	}

	public List<ReportVO> getConsumerOffsetsReport(String groupID) {
		getConsumerOffsets(groupID, false, true);
		return ReportHandler.getOffsetReport(getClientID(), groupID);
	}

	public List<ConsumerGroupVO> getConsumerListOffsetsByDeploy(String deployName) {
		List<ConsumerGroupVO> consumerOffsetList = new ArrayList<ConsumerGroupVO>();
		List<String> groupIDList = getConsumerList();
		for (String groupID : groupIDList) {
			if (groupID.contains(deployName)) {
				ConsumerGroupVO returnConsumerGroupVO = getConsumerOffsets(groupID, true, false);
				if (returnConsumerGroupVO != null) {
					consumerOffsetList.add(returnConsumerGroupVO);
				}
			}
		}

		return consumerOffsetList;
	}

	public List<ConsumerGroupVO> getDeployList() {
		List<ConsumerGroupVO> consumerOffsetList = new ArrayList<ConsumerGroupVO>();

		Map<String, Integer> deployMap = new HashMap<String, Integer>();

		List<String> groupList = getConsumerList();
		for (String groupID : groupList) {
			deployMap.put(extractDeploy(groupID), 0);
		}

		for (String deployName : deployMap.keySet()) {
			ConsumerGroupVO consumerGroupVO = new ConsumerGroupVO();
			consumerGroupVO.setCreateDT(DateTimeUtils.getNormalDate());
			consumerGroupVO.setDeployName(deployName);

			consumerOffsetList.add(consumerGroupVO);
		}

		return consumerOffsetList;
	}

	private String extractDeploy(String groupID) {
		// 	groupID-HCS_TEST-DE1570154270-ENT3764-SP
		return groupID.split("-")[1];
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
			//logger.info("groupAll : {}", groupAll);
		} catch (Throwable ex) {
			if (ex instanceof RuntimeException) {
				KafkaAdminClientFactory.closeAdminClient();
				logger.warn("getConsumerList KafkaAdmin Connection warn : ex.getMessage()={}", ex.getMessage());
			}

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
			//logger.warn("[getConsumerGroupSummary] consumer({}) is not running !", groupID);
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
