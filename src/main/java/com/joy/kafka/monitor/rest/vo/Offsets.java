package com.joy.kafka.monitor.rest.vo;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;

public class Offsets {

	private List<ConsumerGroupVO> offsetList;

	public List<ConsumerGroupVO> getOffsetList() {
		return offsetList;
	}

	public void setOffsetList(List<ConsumerGroupVO> offsetList) {
		this.offsetList = offsetList;
	}
	
	@Override
    public String toString() {
		try {
			return new ObjectMapper().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return "Offsets.toString() JsonProcessingException warn : " + e.getMessage();
		}
    }
}
