package com.joy.kafka.monitor.handler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.rest.vo.MonitorResponseVO;
import com.joy.kafka.monitor.util.ViewColumnType;
import com.joy.kafka.monitor.util.ViewHandler;

public class MonitorHandlerTest {
	private static final Logger logger = LoggerFactory.getLogger(MonitorHandlerTest.class);

	public static ConsumerMonitorHandler consumerMonitor = null;
	public static TopicMonitorHandler topicMonitor = null;;
	
	public static void main(String[] args) {
		String brokers = "192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092";
		brokers = "192.168.10.82:9092";
		//brokers = "192.168.10.87:9092,192.168.10.80:9092";
		
		consumerMonitor = new ConsumerMonitorHandler("localhost", brokers);
		topicMonitor = new TopicMonitorHandler("localhost", brokers);
		
		MonitorHandlerTest test = new MonitorHandlerTest();
		test.testGetDeployList();
		
		//test.testGetConsumerListOffsetsByDeploy();
		
		//test.testGetTopicList();
		
		//test.testGetConsumerList();
		
		//test.testGetConsumerOffset();

		//test.testGetPartitionInfo();
		
		//test.testGetConsumerOffsetList();
		
		//test.testGetOffsetListByTopic();
	}

	public void testGetDeployList() {
		List<ConsumerGroupVO> deploySet = consumerMonitor.getDeployList();
		for(ConsumerGroupVO consumerGroupVO : deploySet) {
			System.out.println(consumerGroupVO.getDeployName());
		}
	}
	
	public void testGetConsumerListOffsetsByDeploy() {
		String deploy = "TABLEUSERDATA"; // HCS_TEST , TABLEUSERDATA
		
		MonitorResponseVO vo = new MonitorResponseVO(consumerMonitor.getConsumerListOffsetsByDeploy(deploy));
		System.out.println(vo.toPrettyJson());
		System.out.println(ViewHandler.showByConsumerOffsetList(vo.getResults()));
	}
	
	public void testGetTopicList() {
		Set<String> topicSet = topicMonitor.getTopicList();
		
		ViewColumnType[] showColumn = { ViewColumnType.TOPIC };
		System.out.print(ViewHandler.showList(showColumn, new ArrayList<String>(topicSet)));
	}
	
	public List<String> testGetConsumerList() {
		List<String> list = consumerMonitor.getConsumerList();

		ViewColumnType[] showColumn = { ViewColumnType.GROUP };
		System.out.print(ViewHandler.showList(showColumn, list));
		
		return list;
	}
	
	public void testGetConsumerOffsetList() {
		MonitorResponseVO vo = new MonitorResponseVO(consumerMonitor.getConsumerListOffsets());
		System.out.println(vo.toPrettyJson());
		System.out.println(ViewHandler.showByConsumerOffsetList(vo.getResults()));
	}
	
	private ConsumerGroupVO testGetConsumerOffset() {
		String groupID = "groupID-IGNITE_TEST-DE1559267912-ENT6787-MEMORYGRID";
		
		ConsumerGroupVO consumerGroup = consumerMonitor.getConsumerOffsets(groupID);

		if (consumerGroup == null) {
			logger.debug("groupID : {} does NOT RUNNING State", groupID);
		}

		System.out.print(ViewHandler.showByConsumerOffset(consumerGroup));
		
		return consumerGroup;
	}

	public void testGetOffsetListByTopic() {
		//String groupID = "groupID-IGNITE_TEST-DE1559267912-ENT6787-MEMORYGRID";
		//logger.debug("> topicName : {}", monitor.getTopicNamebyGroupID(groupID));
		
		MonitorResponseVO vo = new MonitorResponseVO(topicMonitor.getTopicOffsets());
		System.out.println(vo.toPrettyJson());
		System.out.println(ViewHandler.showByTopicOffsetList(vo.getResults()));
	}
	
	@Deprecated
	private void testGetPartitionInfo() {
		
		String topic = "IGNITE_TEST-ENT8243-SP-OUT01-TOPIC";
		Map<Integer, PartitionInfo> map = null;//monitor.getPartitionInfoMap(topic);

		for (int partitionNum : map.keySet()) {
			PartitionInfo partition = map.get(partitionNum);
			logger.debug("> partition : {}", partitionNum);
			String leader = (partition.leader() == null) ? "none"
					: (partition.leader().host().contains(".") ? partition.leader().host().split("\\.")[3]
							: partition.leader().host());
			logger.debug("> leader : {}", leader);
			String replica = null;
			for (Node node : partition.replicas()) {
				replica = (node.host() == null) ? "none"
						: (node.host().contains(".") ? node.host().split("\\.")[3] : node.host());
				logger.debug("> replica : {}", replica);
			}
		}
		/*
		List<PartitionInfo> list = monitor.getPartitions(KafkaConsumerFactory.getKafkaConsumer(brokers, null), topic);
		list = sortAscendingPartitionNum(list);
		
		for(PartitionInfo partition : list) {
			logger.debug("> partition : {}", partition.partition());
			String leader = (partition.leader() == null) ? "none"
					: (partition.leader().host().contains(".") ? partition.leader().host().split("\\.")[3]
							: partition.leader().host());
			logger.debug("> leader : {}", leader);
			String replica = null;
			for (Node node : partition.replicas()) {
				replica = (node.host() == null) ? "none"
						: (node.host().contains(".") ? node.host().split("\\.")[3] : node.host());
				logger.debug("> replica : {}", replica);
			}
			logger.debug("===================================");
		}
		*/
	}

	private List<PartitionInfo> sortAscendingPartitionNum(List<PartitionInfo> pratitionList) {
		List<PartitionInfo> pratitions = new ArrayList<PartitionInfo>(pratitionList);
		pratitions.sort(Comparator.comparing(PartitionInfo::partition));

		return pratitions;
	}

	@Deprecated
	private void testGetConsumerGroup_old(String brokers, String groupID) {
		long start = System.currentTimeMillis();
		ConsumerGroupVO consumerGroup = consumerMonitor.getConsumerOffsets(groupID);

		if (consumerGroup == null) {
			logger.debug("groupID : {} does NOT RUNNING State", groupID);
			return;
		}

		logger.debug("> result : {}", consumerGroup.toString());

		/*
		logger.debug("> topic : {}", consumerGroup.getTopic());
		logger.debug("> group : {}", consumerGroup.getGroupID());
		logger.debug("> createDT : {}", consumerGroup.getCreateDT());
		
		//List<OffsetVO> offsetList = consumerGroup.getOffsetList();
		//offsetList = new ArrayList<OffsetVO>(offsetList);
		//offsetList.sort(Comparator.comparing(OffsetVO::partition));
		//consumerGroup.getOffsetList().sort(Comparator.comparing(OffsetVO::partition));
		
		for(OffsetVO offset : consumerGroup.getOffsetList()) {
			logger.debug(">> partition : {}", offset.getPartition());
			logger.debug(">> host : {}", offset.getHost());
			logger.debug(">> endOffset : {}", offset.getEndOffset());
			logger.debug(">> committedOffset : {}", offset.getCommittedOffset());
			logger.debug(">> lag : {}", offset.getLag());
			logger.debug(">> leader : {}", offset.leader());
			logger.debug(">> replicas : {}", offset.getReplicas());
			logger.debug("");
		}
		*/
		logger.debug("> elapsed time : {} ms", System.currentTimeMillis() - start);
	}
}
