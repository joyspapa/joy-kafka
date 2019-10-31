package com.joy.kafka.monitor.config;

public enum AdminCommand {
	Create     	("create"),
	Delete    	("delete"),
	List    	("list"),
	None    	("none")
	;
	
	private String name;
	
	AdminCommand(String name) {
		this.name = name;
	}
	
	public String getName() {
		return this.name;
	}
	
	public static AdminCommand findViewType(String viewType) {
		if(viewType == null) {
			return AdminCommand.None;
		}
		
		for (AdminCommand view : AdminCommand.values()) {
			if (view.name.equals(viewType)) {
				return view;
			}
		}
		return AdminCommand.None;
	}
}
