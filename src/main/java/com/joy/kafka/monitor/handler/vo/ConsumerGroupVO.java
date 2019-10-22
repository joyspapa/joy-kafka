package com.joy.kafka.monitor.handler.vo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.vertx.core.json.JsonObject;

public class ConsumerGroupVO {

	private String topic = "";
	private String groupID = "";
	private long endOffsetAll;
	private long committedOffsetAll;
	private String createDT = "";
	private String deployName = "";
	private boolean isConsumerRunning = false;
	private List<OffsetVO> offsetList = new ArrayList<OffsetVO>();

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getGroupID() {
		return groupID;
	}

	public void setGroupID(String groupID) {
		this.groupID = groupID;
	}

	public long getEndOffsetAll() {
		//return 888888888888L;
		return endOffsetAll;
	}

	public void addEndOffsetAll(long endOffset) {
		this.endOffsetAll += endOffset;
	}

	public long getCommittedOffsetAll() {
		return committedOffsetAll;
	}

	public void addCommittedOffsetAll(long committedOffset) {
		this.committedOffsetAll += (committedOffset == -1L ? 0L : committedOffset);
	}

	public long getLag() {
		if(isConsumerRunning) {
			return endOffsetAll - committedOffsetAll;
		} else {
			return -1;
		}
	}

	public boolean isConsumerRunning() {
		return isConsumerRunning;
	}

	public void setConsumerRunning(boolean isConsumerRunning) {
		this.isConsumerRunning = isConsumerRunning;
	}

	public String getCreateDT() {
		return createDT;
	}

	public void setCreateDT(String createDT) {
		this.createDT = createDT;
	}

	public String getDeployName() {
		return deployName;
	}

	public void setDeployName(String deployName) {
		this.deployName = deployName;
	}

	public List<OffsetVO> getOffsetList() {
		// sort by partitionNum as ASC
		offsetList.sort(Comparator.comparing(OffsetVO::getPartition));
		return offsetList;
	}

	public void setOffsetList(List<OffsetVO> list) {
		this.offsetList = list;
	}

	public void addOffsetList(OffsetVO offset) {
		this.offsetList.add(offset);
	}

	@Override
	public String toString() {
		return JsonObject.mapFrom(this).toString();
		//return new ObjectMapper().writeValueAsString(this);
	}
	
	public String toPrettyJson() {
		return JsonObject.mapFrom(this).encodePrettily();
	}
}
