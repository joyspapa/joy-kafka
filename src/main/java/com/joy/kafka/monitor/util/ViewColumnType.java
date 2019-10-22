package com.joy.kafka.monitor.util;

public enum ViewColumnType {

	DT               ("DateTime", 20),
	TOPIC            ("Topic", 55),
	GROUP            ("Consumer", 60),
	PARTITION        ("Partition", 10),	
	HOST             ("Host", 10),
	COMMITTED_OFFSET ("CommittedOffset", 18),
	END_OFFSET       ("EndOffset", 18),
	LAG              ("Lag", 18),
	LEADER           ("Leader", 9),
	REPLICAS         ("Replicas", 20)
	;
	
	private String colName;
	private int colLength;
	
	ViewColumnType(String colName, int colLength) {
		this.colName = colName;
		this.colLength = colLength;
	}
	
	public String colName() {
		return this.colName;
	}
	
	public int colLength() {
		return this.colLength;
	}
}
