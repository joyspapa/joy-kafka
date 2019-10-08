package com.joy.kafka.monitor.offsetviewer.old.vo;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonObject;

public class OffsetMonitorVO {

	private String deployID;
	private List<OffsetPerTopicVO> consumers = new ArrayList<OffsetPerTopicVO>();
	
	public String getDeployID() {
		return deployID;
	}

	public void setDeployID(String deployID) {
		this.deployID = deployID;
	}

	public List<OffsetPerTopicVO> getConsumers() {
		return consumers;
	}

	public void setConsumers(List<OffsetPerTopicVO> consumers) {
		this.consumers = consumers;
	}

	public void addConsumers(OffsetPerTopicVO consumer) {
		this.consumers.add(consumer);
	}
	
	@Override
	public String toString() {
		return (new JsonObject()).mapFrom(this).toString();
	}
	
	public String printPrettyJson() {
		return (new JsonObject()).mapFrom(this).encodePrettily();
	}
}
