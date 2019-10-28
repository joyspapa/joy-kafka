package com.joy.kafka.monitor.rest.vo;

import com.joy.kafka.monitor.config.ViewType;

import io.vertx.core.json.JsonObject;

public class ResponseVO {

	private boolean success = true;
	private String viewType = "";

	public ResponseVO(ViewType viewType) {
		this.viewType = viewType.getName();
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getViewType() {
		return viewType;
	}

	public void setViewType(String viewType) {
		this.viewType = viewType;
	}

	@Override
	public String toString() {
		return JsonObject.mapFrom(this).toString();
	}

	public String toPrettyJson() {
		return JsonObject.mapFrom(this).encodePrettily();
	}
}
