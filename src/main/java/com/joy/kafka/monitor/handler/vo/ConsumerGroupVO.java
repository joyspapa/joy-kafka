package com.joy.kafka.monitor.handler.vo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.json.JsonObject;

public class ConsumerGroupVO {

	private String topic;
	private String groupID;
	private long endOffsetAll;
	private long committedOffsetAll;
	private String createDT;
	private List<OffsetVO> offsetList;
	
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
		return endOffsetAll - committedOffsetAll;
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
		if(this.offsetList == null) {
			this.offsetList = new ArrayList<OffsetVO>();
		}
		this.offsetList.add(offset);
	}
	
	public String getCreateDT() {
		return createDT;
	}

	public void setCreateDT(String createDT) {
		this.createDT = createDT;
	}

	@Override
    public String toString() {
		try {
			//return new ObjectMapper().writeValueAsString(this);
			return JsonObject.mapFrom(this).toString();
		} catch (Exception e) {
			return "ConsumerGroupVO.toString() Json Parsing Exception warn : " + e.getMessage();
		}
    }
}
