package com.joy.kafka.monitor.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportHandler {
	private static final Logger logger = LoggerFactory.getLogger(ReportHandler.class);
	
	public static void addOffsetReport(String clientID, String groupID, int partition, ReportOffsetVO reportOffset) {
		ReportQueue queue = ReportQueueRepo.getInstance().getOffsetQueueMap(clientID, groupID, partition);
		if (queue == null) {
			queue = new ReportQueue();
		}
		queue.addQueue(reportOffset);

		ReportQueueRepo.getInstance().addOffsetQueueMap(clientID, groupID, partition, queue);
	}

	public static List<ReportVO> getOffsetReport(String clientID, String groupID) {
		Map<Integer, ReportQueue> partitionMap = ReportQueueRepo.getInstance().getOffsetPartitionMap(clientID, groupID);
		List<ReportVO> reportList = new ArrayList<>();
		for (int partition : partitionMap.keySet()) {
			reportList.add(new ReportVO(groupID, partition, partitionMap.get(partition).getConsumerGroupVOList()));
		}
		return reportList;
	}

	public static void clearOffsetReport(String clientID) {
		Map<String, Map<Integer, ReportQueue>> groupIDMap = ReportQueueRepo.getInstance()
				.offsetQueueGroupIDMap(clientID);
		if (groupIDMap != null) {
			groupIDMap.clear();
		}
	}
	
	@Deprecated
	private static void clearOffsetReport_old(String clientID) {
		Map<String, Map<Integer, ReportQueue>> groupIDMap = ReportQueueRepo.getInstance()
				.offsetQueueGroupIDMap(clientID);
		if (groupIDMap != null) {
			for (String groupID : groupIDMap.keySet()) {
				Map<Integer, ReportQueue> partitionMap = groupIDMap.get(groupID);
				if (partitionMap != null) {
					for (int partition : partitionMap.keySet()) {
						partitionMap.get(partition).clearQueue();
					}
				}
			}
		}
	}
}
