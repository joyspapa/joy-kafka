package com.joy.kafka.monitor.offsetviewer.old.vo;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.vertx.core.json.JsonObject;

@JsonPropertyOrder({ "partition" })
public class OffsetPerPartitonVO {
	private int partition;
	private long logSize;
	private long offset;
	private long lag;
	//private String owner;
	//private String create;
	//private String modify;
	//private String topic;

	private String leader;
	private List<String> replicas = new ArrayList<String>();
	//private List<String> inSyncReplicas = new ArrayList<String>();
	
//	public String getTopic() {
//		return topic;
//	}
//
//	public void setTopic(String topic) {
//		this.topic = topic;
//	}

	public OffsetPerPartitonVO() {
		
	}
	
	public OffsetPerPartitonVO(long logSize, long offset, long lag) {
		this.logSize = logSize;
		this.offset = offset;
		this.lag = lag;
	}
	
	public int getPartition() {
		return this.partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public long getLogSize() {
		return this.logSize;
	}

	public void setLogSize(long logSize) {
		this.logSize = logSize;
	}

	public long getOffset() {
		return this.offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getLag() {
		return this.lag;
	}

	public void setLag(long lag) {
		this.lag = lag;
	}

//	public String getOwner() {
//		return this.owner;
//	}
//
//	public void setOwner(String owner) {
//		this.owner = owner;
//	}
//
//	public String getCreate() {
//		return this.create;
//	}
//
//	public void setCreate(String create) {
//		this.create = create;
//	}
//
//	public String getModify() {
//		return this.modify;
//	}
//
//	public void setModify(String modify) {
//		this.modify = modify;
//	}

	public String getLeader() {
		return leader;
	}

	public void setLeader(String leader) {
		this.leader = leader;
	}

	public List<String> getReplicas() {
		return replicas;
	}

	public void setReplicas(List<String> replicas) {
		this.replicas = replicas;
	}

	public void addReplicas(String replica) {
		this.replicas.add(replica);
	}
	
	public String toString() {
		return (new JsonObject()).mapFrom(this).toString();
	}
	
	public String printPrettyJson() {
		return (new JsonObject()).mapFrom(this).encodePrettily();
	}
}