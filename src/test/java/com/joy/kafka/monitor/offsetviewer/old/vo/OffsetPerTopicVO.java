package com.joy.kafka.monitor.offsetviewer.old.vo;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonObject;

/**
 * Created by lixun on 2017/3/22.
 */
public class OffsetPerTopicVO {

	private String entityID;
	private String entityType;
	private String entityDesc;
	private String groupID;
	private String topic;
	private long logSize;
	private long offsets;
	private long lag;
	private String created;
	
	//private long startingOffset;
	private int partitionSize;

	private List<OffsetPerPartitonVO> partitions = new ArrayList<OffsetPerPartitonVO>();

	public OffsetPerTopicVO(String group, String topic, long offsets, String created) {
		this.groupID = group;
		this.topic = topic;
		this.offsets = offsets;
		this.created = created;
	}

	public OffsetPerTopicVO(String group, String topic, long logSize, long offsets, long lag, String created) {
		this.groupID = group;
		this.topic = topic;
		this.logSize = logSize;
		this.offsets = offsets;
		this.lag = lag;
		this.created = created;
	}

	public OffsetPerTopicVO() {
		this.groupID = "";
		this.topic = "";
		this.logSize = 0L;
		this.offsets = 0L;
		this.lag = 0L;
		this.created = "";
	}

	//	public long getStartingOffset() {
	//		return startingOffset;
	//	}
	//
	//	public void setStartingOffset(long startingOffset) {
	//		this.startingOffset = startingOffset;
	//	}

	public String getEntityID() {
		return entityID;
	}

	public void setEntityID(String entityID) {
		this.entityID = entityID;
	}

	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	public String getGroupID() {
		return groupID;
	}

	public void setGroupID(String groupID) {
		this.groupID = groupID;
	}

	public String getEntityDesc() {
		return entityDesc;
	}

	public void setEntityDesc(String entityDesc) {
		this.entityDesc = entityDesc;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public long getLogSize() {
		//this.logSize = -1;
		for (OffsetPerPartitonVO offsetPerPartitonVO : partitions) {
			if (offsetPerPartitonVO.getLogSize() > -1) {
				logSize += offsetPerPartitonVO.getLogSize();
			}
		}
		return logSize;
	}

	public void setLogSize(long logSize) {
		this.logSize = logSize;
	}

	public long getOffsets() {
		//this.offsets = -1;
		for (OffsetPerPartitonVO offsetPerPartitonVO : partitions) {
			if (offsetPerPartitonVO.getOffset() > -1) {
				offsets += offsetPerPartitonVO.getOffset();
			}
		}
		return offsets;
	}

	public void setOffsets(long offsets) {
		this.offsets = offsets;
	}

	public long getLag() {
		if (this.logSize == -1 || this.offsets == -1) {
			return -1;
		}
		return this.logSize - this.offsets;
	}

	public void setLag(long lag) {
		this.lag = lag;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public List<OffsetPerPartitonVO> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<OffsetPerPartitonVO> partitions) {
		this.partitions = partitions;
	}

	public void addPartitions(OffsetPerPartitonVO partition) {
		this.partitions.add(partition);
	}

	public int getPartitionSize() {
		return partitions.size();
	}

	@Override
	public String toString() {
		return (new JsonObject()).mapFrom(this).toString();
	}

	public String printPrettyJson() {
		return (new JsonObject()).mapFrom(this).encodePrettily();
	}
}
