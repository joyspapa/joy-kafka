package com.joy.kafka.monitor.report;

public class ReportOffsetVO {

	private long endOffset;
	private long committedOffset;
	private long createDT;
	
	public ReportOffsetVO(long endOffset, long committedOffset) {
		this.createDT = System.currentTimeMillis();
		this.endOffset = endOffset;
		this.committedOffset = committedOffset;
	}
	
	public long getEndOffset() {
		return endOffset;
	}
	
	public long getCommittedOffset() {
		return committedOffset;
	}
	
	public long getLag() {
		return endOffset - committedOffset;
	}
	
	public long getCreateDT() {
		return createDT;
	}
}
