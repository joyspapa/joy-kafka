package com.joy.kafka.monitor.handler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.factory.KafkaConsumerFactory;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.handler.vo.OffsetVO;
import com.joy.kafka.monitor.util.DateTimeUtils;

public class TopicMonitorHandler extends MonitorHandler {
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
		List<PartitionInfo> partitionInfoList = getPartitionInfo(topic);
		
		ConsumerGroupVO consumerGroupVO = new ConsumerGroupVO();
		if (partitionInfoList != null && !partitionInfoList.isEmpty()) {
			for (PartitionInfo partitionInfo : partitionInfoList) {
				long endOffset = getLogEndOffset(null, partitionInfo.topic(), partitionInfo.partition());
				
				OffsetVO offsetVO = new OffsetVO();
				offsetVO.setPartition(partitionInfo.partition()).setEndOffset(endOffset)
						.setLeader(partitionInfo.leader().host()).setReplicas(partitionInfo.replicas());

				consumerGroupVO.addEndOffsetAll(endOffset);
				consumerGroupVO.addOffsetList(offsetVO);
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

	private Map<String, List<PartitionInfo>> getTopicListWithPartitionInfo() {
		return KafkaConsumerFactory.getKafkaConsumer(getBrokers()).listTopics();
	}
}
