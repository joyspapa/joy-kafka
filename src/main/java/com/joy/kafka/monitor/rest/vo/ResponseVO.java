package com.joy.kafka.monitor.rest.vo;

import io.vertx.core.json.JsonObject;

public class ResponseVO {

	private boolean success = true;

	public ResponseVO() {
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	@Override
	public String toString() {
		return JsonObject.mapFrom(this).toString();
	}

	public String toPrettyJson() {
		return JsonObject.mapFrom(this).encodePrettily();
	}
}
