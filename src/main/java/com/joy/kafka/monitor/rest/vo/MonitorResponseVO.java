package com.joy.kafka.monitor.rest.vo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.joy.kafka.monitor.config.ViewType;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;

public class MonitorResponseVO extends ResponseVO {

	private List<ConsumerGroupVO> results = new ArrayList<ConsumerGroupVO>();
	private boolean hasGroupID = false;
	
	public MonitorResponseVO(ViewType viewType, List<ConsumerGroupVO> resultList) {
		super(viewType);
		this.results = resultList;
		hasGroupID(resultList);
	}
	
	public List<ConsumerGroupVO> getResults() {
		if (hasGroupID) {
			results.sort(Comparator.comparing(ConsumerGroupVO::getGroupID));
		} else {
			results.sort(Comparator.comparing(ConsumerGroupVO::getTopic));
		}
		return results;
	}

	private void hasGroupID(List<ConsumerGroupVO> resultList) {
		if (resultList != null && !resultList.isEmpty()) {
			if (resultList.get(0).getGroupID().length() > 0) {
				hasGroupID = true;
			}
		}
	}
}
