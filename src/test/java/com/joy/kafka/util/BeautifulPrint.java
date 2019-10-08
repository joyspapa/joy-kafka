package com.joy.kafka.util;

import com.joy.kafka.monitor.consumer.ConsumerMonitor;

public class BeautifulPrint {

	public static void main(String[] args) {
		System.out.format("%-25s %9.7f%n", "K and Burstner", 0.055170);
		BeautifulPrint print = new BeautifulPrint();
		//print.print1();
		//print.print2();
		print.print3();
	}

	// https://stackoverflow.com/questions/29052615/nice-looking-table-with-stringbuilder
	private void print1() {
		StringBuilder s = new StringBuilder();

	    s.append(String.format("%-50s%-10s%-18s%-18s%-18s%-8s%-10s\n","TOPIC","PARTITION","CURRENT-OFFSET","END-OFFSET","LAG","leader","replicas"));
	    s.append(String.format("==================================================================================================================================\n"));
	    s.append(String.format("%-50s%-10s%,-18d%,-18d%,-18d%-8s%-10s\n","IGNITE_TEST-ENT6787-MEMORYGRID-OUT01-TOPIC","0",400000000,500000000,100000000,"82","82,83"));
	    s.append(String.format("%-50s%-10s%,-18d%,-18d%,-18d%-8s%-10s\n","IGNITE_TEST-ENT6787-MEMORYGRID-OUT01-TOPIC","2",400000000,500000000,100000000,"83","83,84"));
	    System.out.println(s.toString());
	}
	
	private void print2() {
		String output = String.format("%s = %d", "joe", 35);
		System.out.println(output);
		System.out.printf("My name is: %s%n", "joe");
	}
	
	private void print3() {
		ConsumerMonitor monitor = new ConsumerMonitor();
		monitor.getConsumersByAdminClient("192.168.10.82:9092");
	}
}
