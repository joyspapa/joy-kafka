package com.joy.kafka.monitor.config;

public class Constant {
	public static final String bus_kafka_monitor = ConfigHelper.getValue("synapse.kafka.offset.viewer.monitor.bus");
	public static final String bus_kafka_admin = ConfigHelper.getValue("synapse.kafka.offset.viewer.admin.bus");
	public static final String brokers = ConfigHelper.getValue("synapse.kafka.offset.viewer.broker");
}
