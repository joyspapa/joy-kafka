package com.joy.kafka.monitor.rest.vo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;

import io.vertx.core.json.JsonObject;

public class ResponseVO {

	private boolean success = true;
	private List<ConsumerGroupVO> results = new ArrayList<ConsumerGroupVO>();
	private boolean hasGroupID = false;

	public ResponseVO(List<ConsumerGroupVO> resultList) {
		this.results = resultList;
		hasGroupID(resultList);
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public List<ConsumerGroupVO> getResults() {
		if (hasGroupID) {
			results.sort(Comparator.comparing(ConsumerGroupVO::getGroupID));
		} else {
			results.sort(Comparator.comparing(ConsumerGroupVO::getTopic));
		}
		return results;
	}

	@Override
	public String toString() {
		return JsonObject.mapFrom(this).toString();
	}

	public String toPrettyJson() {
		return JsonObject.mapFrom(this).encodePrettily();
	}

	private void hasGroupID(List<ConsumerGroupVO> resultList) {
		if (resultList != null && !resultList.isEmpty()) {
			if (resultList.get(0).getGroupID().length() > 0) {
				hasGroupID = true;
			}
		}
	}
}
