package com.joy.kafka.monitor.config;

public enum ViewType {
	Consumer     	("consumer"),
	ConsumerAll    	("consumerAll"),
	Report    		("report"),
	Deploy    		("deploy"),
	None    		("none")
	;
	
	private String name;
	
	ViewType(String name) {
		this.name = name;
	}
	
	public String getName() {
		return this.name;
	}
	
	public static ViewType findViewType(String viewType) {
		for (ViewType view : ViewType.values()) {
			if (view.name.equals(viewType)) {
				return view;
			}
		}
		return ViewType.None;
	}
}
