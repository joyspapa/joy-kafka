package com.joy.kafka.monitor.report;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportQueueRepo {
	private static final Logger logger = LoggerFactory.getLogger(ReportQueueRepo.class);

	private static ReportQueueRepo instance;
	private Map<String, Map<String, Map<Integer, ReportQueue>>> offsetQueueMap = new HashMap<>();

	protected static ReportQueueRepo getInstance() {
		if (instance == null) {
			instance = new ReportQueueRepo();
		}
		return instance;
	}

	protected Map<String, Map<String, Map<Integer, ReportQueue>>> addOffsetQueueMap(String clientID, String groupID, int partition, ReportQueue offsetQueue) {
		if (offsetQueueMap.get(clientID) == null) {
			Map<Integer, ReportQueue> partitionMap = new TreeMap<Integer, ReportQueue>();
			partitionMap.put(partition, offsetQueue);
			
			Map<String, Map<Integer, ReportQueue>> groupIDMap = new HashMap<String, Map<Integer, ReportQueue>>();
			groupIDMap.put(groupID, partitionMap);
			
			offsetQueueMap.put(clientID, groupIDMap);
			
		} else if (offsetQueueMap.get(clientID).get(groupID) == null) {
			Map<Integer, ReportQueue> partitionMap = new TreeMap<Integer, ReportQueue>();
			partitionMap.put(partition, offsetQueue);
			
			offsetQueueMap.get(clientID).put(groupID, partitionMap);
			
		} else {
			offsetQueueMap.get(clientID).get(groupID).put(partition, offsetQueue);
		}
		
		return offsetQueueMap;
	}

	protected ReportQueue getOffsetQueueMap(String clientID, String groupID, int partition) {
		if (offsetQueueMap.get(clientID) == null || offsetQueueMap.get(clientID).get(groupID) == null) {
			return null;
		}
		return offsetQueueMap.get(clientID).get(groupID).get(partition);
	}
	
	protected Map<Integer,ReportQueue> getOffsetPartitionMap(String clientID, String groupID) {
		return offsetQueueMap.get(clientID).get(groupID);
	}
	
	protected Map<String, Map<Integer, ReportQueue>> offsetQueueGroupIDMap(String clientID) {
		return offsetQueueMap.get(clientID);
	}
}
