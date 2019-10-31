package com.joy.kafka.monitor.rest.vo;

import java.util.ArrayList;
import java.util.List;

import com.joy.kafka.monitor.config.AdminCommand;
import com.joy.kafka.monitor.config.ViewType;

public class AdminResponseVO extends ResponseVO {

	private String message;
	private String command;
	private List<String> listTopic;
	
	public AdminResponseVO(ViewType viewType) {
		super(viewType);
	}

	public String getMessage() {
		return (message == null)? "":message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getCommand() {
		return (command == null)? "":command;
	}

	public void setCommand(AdminCommand cmd) {
		this.command = cmd.getName();
	}

	public List<String> getListTopic() {
		return (listTopic == null)? new ArrayList<>():listTopic;
	}

	public void setListTopic(List<String> listTopic) {
		this.listTopic = listTopic;
	}
}
