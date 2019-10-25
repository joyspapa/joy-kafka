package com.joy.kafka.monitor.report;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ReportQueue {

	private Queue<ReportOffsetVO> queue = new LinkedList<ReportOffsetVO>();
	private int queueLimitSize = 10;

	public void addQueue(ReportOffsetVO vo) {
		if (queue.size() >= queueLimitSize) {
			queue.remove();
		}

		queue.add(vo);
	}

	public Queue<ReportOffsetVO> getQueue() {
		return queue;
	}

	public List<ReportOffsetVO> getConsumerGroupVOList() {
		return new ArrayList<ReportOffsetVO>(queue);
	}

	public void clearQueue() {
		if (!queue.isEmpty()) {
			queue.clear();
		}
	}
}
