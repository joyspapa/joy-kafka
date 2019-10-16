package com.joy.kafka.monitor.handler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.joy.kafka.monitor.util.DateTimeUtils;

import scala.collection.JavaConverters;

public class TopicMonitorHandler extends MonitorAbstract {
	private static final Logger logger = LoggerFactory.getLogger(TopicMonitorHandler.class);

	public TopicMonitorHandler(String brokers) {
		super(brokers);
	}

	public List<ConsumerGroupVO> getTopicOffsets() {
		List<ConsumerGroupVO> topicOffsetList = new ArrayList<ConsumerGroupVO>();
		
		Set<String> topicSet = getTopicList();
		for (String topic : topicSet) {
			topicOffsetList.add(getTopicOffsets(topic));
		}
		
		return topicOffsetList;
	}

	public ConsumerGroupVO getTopicOffsets(String topic) {
		ConsumerGroupVO consumerGroupVO = new ConsumerGroupVO();
		
		List<PartitionInfo> partitionInfoList = getPartitionInfo(topic);

		if (partitionInfoList != null && !partitionInfoList.isEmpty()) {

			//if (pratitions.size() > 1) {
			//	sortAscendingPartitionNum(partitionInfoList);
			//}

			//logger.debug("partitionInfoList : " + partitionInfoList);
			for (PartitionInfo partitionInfo : partitionInfoList) {

				long endOffset = getLogEndOffset(null, partitionInfo.topic(), partitionInfo.partition());
				//logger.debug("logEndOffset : " + endOffset);

				OffsetVO offsetVO = new OffsetVO();
				offsetVO.setPartition(partitionInfo.partition()).setEndOffset(endOffset)
						.setLeader(partitionInfo.leader().host()).setReplicas(partitionInfo.replicas());

				consumerGroupVO.addOffsetList(offsetVO);
				//logger.debug("topic : " + partitionInfo.topic());
				//logger.debug("partition : " + partitionInfo.partition());
				//logger.debug("leader : " + partitionInfo.leader().host());

				//printPartitionInfo(partitionInfo);
			}
		}
		
		consumerGroupVO.setTopic(topic);
		consumerGroupVO.setCreateDT(DateTimeUtils.getNormalDate());
				
		return consumerGroupVO;
	}

	public Set<String> getTopicList() {
		Set<String> topicList = new HashSet<String>();
		try {
			topicList = getTopicListWithPartitionInfo().keySet();
			topicList.remove("__consumer_offsets");
		} catch (Throwable th) {
			logger.warn("getTopicList warn : ", th);
		}

		return topicList;
	}

	public String getTopicNamebyGroupID(String groupID) {
		scala.collection.immutable.Map<TopicPartition, Object> nodeListMap = KafkaAdminClientFactory
				.getAdminClient(getBrokers()).listGroupOffsets(groupID);
		for (TopicPartition t : JavaConverters.mapAsJavaMap(nodeListMap).keySet()) {
			return t.topic();
		}
		return "none";
	}

	private Map<String, List<PartitionInfo>> getTopicListWithPartitionInfo() {
		return KafkaConsumerFactory.getKafkaConsumer(getBrokers()).listTopics();
	}

//	private void sortAscendingPartitionNum(List<PartitionInfo> pratitionList) {
//		pratitions = new ArrayList<PartitionInfo>(pratitionList);
//		pratitions.sort(Comparator.comparing(PartitionInfo::partition));
//	}

	private void printPartitionInfo(PartitionInfo partitionInfo) {

		StringBuilder sb = new StringBuilder();
		sb.append(partitionInfo.topic()).append("-").append(partitionInfo.partition());
		sb.append(partitionInfo.topic());

		logger.debug("========================================================");
		logger.debug("topic : " + partitionInfo.topic());
		logger.debug("partition : " + partitionInfo.partition());
		logger.debug("leader : " + partitionInfo.leader().host());
		logger.debug("replicas  [");
		for (Node node : partitionInfo.replicas()) {
			logger.debug("         " + node.host());
		}
		logger.debug("          ]");

		String groupID = "groupID-IGNITE_TEST-DE1559267912-ENT6787-MEMORYGRID,groupID-IGNITE_TEST-DE1559267912-ENT4963-ELASTICSEARCH";

		long logEndOffset = getLogEndOffset(groupID, partitionInfo.topic(), partitionInfo.partition());
		logger.debug("logEndOffset : " + logEndOffset);

		OffsetAndMetadata committed = KafkaConsumerFactory.getKafkaConsumer(getBrokers(), groupID)
				.committed(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
		if (committed != null) {
			logger.debug("처리건수 : " + committed.offset());
			logger.debug("미처리건수 : " + (logEndOffset - committed.offset()));
		} else {
			logger.debug("committed is NULL : ");
		}

		logger.debug("");
		logger.debug("");
	}
}
