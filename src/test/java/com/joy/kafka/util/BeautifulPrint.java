package com.joy.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.handler.ConsumerMonitorHandler;

public class BeautifulPrint {
	private static final Logger logger = LoggerFactory.getLogger(BeautifulPrint.class);
	
	public static void main(String[] args) {
		BeautifulPrint print = new BeautifulPrint();
		//print.print1();
		//print.print2();
		//print.print4();
		print.print5();
	}

	// https://stackoverflow.com/questions/29052615/nice-looking-table-with-stringbuilder
	private void print1() {
		System.out.format("%-25s %9.7f%n", "K and Burstner", 0.055170);
		
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
		ConsumerMonitorHandler monitor = new ConsumerMonitorHandler("192.168.10.82:9092");
		monitor.getConsumerList();
	}
	/*
	 * https://stackoverflow.com/questions/15215326/how-can-i-create-table-using-ascii-in-a-console
	 */
	private void print4() {
		String leftAlignFormat = "| %-15s | %-4d |%n";

		System.out.format("+-----------------+------+%n");
		System.out.format("| Column name     | ID   |%n");
		System.out.format("+-----------------+------+%n");
		for (int i = 0; i < 5; i++) {
		    System.out.format(leftAlignFormat, "some data" + i, i * i);
		}
		System.out.format("+-----------------+------+%n");
	}
	
	private void print5() {
		//System.out.println(ShowTable.head());
	}
}
