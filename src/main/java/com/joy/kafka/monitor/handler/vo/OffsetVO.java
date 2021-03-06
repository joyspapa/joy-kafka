package com.joy.kafka.monitor.handler.vo;

import org.apache.kafka.common.Node;

import io.vertx.core.json.JsonObject;

public class OffsetVO {

	private int partition;
	private long endOffset;
	private long committedOffset;
	private String host = "";
	private String consumerID = "";
	private String clientID = "";
	private String leader = "";
	private String replicas = "";
	
	public int getPartition() {
		return partition;
	}

	public OffsetVO setPartition(int partition) {
		this.partition = partition;
		return this;
	}

	public long getEndOffset() {
		//return 888888888888L;
		return endOffset;
	}

	public OffsetVO setEndOffset(long endOffset) {
		this.endOffset = endOffset;
		return this;
	}

	public long getCommittedOffset() {
		return committedOffset;
	}

	public OffsetVO setCommittedOffset(long committedOffset) {
		this.committedOffset = committedOffset;
		//this.committedOffset = (committedOffset == -1L ? 0L : committedOffset);
		return this;
	}

	public long getLag() {
		if(0 < host.length()) {
			return endOffset - committedOffset;
		} else {
			return -1;
		}
	}

	public String getHost() {
		return host;
	}

	public OffsetVO setHost(String host) {
		this.host = host.contains(".") ? host.split("\\.")[3] : host;
		return this;
	}

	public String getConsumerID() {
		return consumerID;
	}

	public OffsetVO setConsumerID(String consumer_id) {
		this.consumerID = consumer_id;
		return this;
	}

	public String getClientID() {
		return clientID;
	}

	public OffsetVO setClientID(String client_id) {
		this.clientID = client_id;
		return this;
	}

	public String getLeader() {
		return leader;
	}

	public OffsetVO setLeader(String leader) {
		this.leader = (leader.contains(".") ? leader.split("\\.")[3] : leader);
		return this;
	}

	public String getReplicas() {
		return replicas;
	}

	public OffsetVO setReplicas(Node[] replicasNode) {
		int nodeCnt = 0;
		for (Node node : replicasNode) {
			replicas += (node.host().contains(".") ? node.host().split("\\.")[3] : node.host());
			if(nodeCnt++ < replicasNode.length - 1) {
				replicas += ",";
			}
		}
		return this;
	}
	
	@Override
	public String toString() {
		return JsonObject.mapFrom(this).toString();
	}
	
	public String toPrettyJson() {
		return JsonObject.mapFrom(this).encodePrettily();
	}
}
