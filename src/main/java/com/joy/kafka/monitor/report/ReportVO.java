package com.joy.kafka.monitor.report;

import java.util.Comparator;
import java.util.List;

public class ReportVO {

	private String groupID;
	private int partition;
	private List<ReportOffsetVO> offsets;
	
	public ReportVO(String groupID, int partition, List<ReportOffsetVO> offsets) {
		this.groupID = groupID;
		this.partition = partition;
		this.offsets = offsets;
	}

	public String getGroupID() {
		return groupID;
	}

	public int getPartition() {
		return partition;
	}

	public List<ReportOffsetVO> getOffsets() {
		offsets.sort(Comparator.comparing(ReportOffsetVO::getCreateDT).reversed());
		return offsets;
	}
}
