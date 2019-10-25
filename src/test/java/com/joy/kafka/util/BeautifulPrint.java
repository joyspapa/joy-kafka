package com.joy.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joy.kafka.monitor.handler.ConsumerMonitorHandler;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class BeautifulPrint {
	private static final Logger logger = LoggerFactory.getLogger(BeautifulPrint.class);

	public static void main(String[] args) throws Exception {
		BeautifulPrint print = new BeautifulPrint();
		print.test();
		//print.print1();
		//print.print2();
		//print.print4();
		//print.printVertxJson();
		//print.printJacksonJson();
	}

	private void test() {
		for(int i = 0; i < 10; i++) {
			System.out.println(i%2);
		}
	}
	
	// https://stackoverflow.com/questions/29052615/nice-looking-table-with-stringbuilder
	private void print1() {
		System.out.format("%-25s %9.7f%n", "K and Burstner", 0.055170);

		StringBuilder s = new StringBuilder();

		s.append(String.format("%-50s%-10s%-18s%-18s%-18s%-8s%-10s\n", "TOPIC", "PARTITION", "CURRENT-OFFSET",
				"END-OFFSET", "LAG", "leader", "replicas"));
		s.append(String.format(
				"==================================================================================================================================\n"));
		s.append(String.format("%-50s%-10s%,-18d%,-18d%,-18d%-8s%-10s\n", "IGNITE_TEST-ENT6787-MEMORYGRID-OUT01-TOPIC",
				"0", 400000000, 500000000, 100000000, "82", "82,83"));
		s.append(String.format("%-50s%-10s%,-18d%,-18d%,-18d%-8s%-10s\n", "IGNITE_TEST-ENT6787-MEMORYGRID-OUT01-TOPIC",
				"2", 400000000, 500000000, 100000000, "83", "83,84"));
		System.out.println(s.toString());
	}

	private void print2() {
		String output = String.format("%s = %d", "joe", 35);
		System.out.println(output);
		System.out.printf("My name is: %s%n", "joe");
	}

	private void print3() {
		ConsumerMonitorHandler monitor = new ConsumerMonitorHandler("localhost", "192.168.10.82:9092");
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

	private void printVertxJson() {
		String jsonString = "{\"success\":true,\"results\":[{\"topic\":\"ENT6713-OUT01-topic\",\"groupID\":\"etc\",\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":3,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube88\",\"replicas\":\"ecube88,ecube87\",\"lag\":3}],\"lag\":0},{\"topic\":\"ENT3591-OUT01-topic\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":0,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube88\",\"replicas\":\"ecube88,ecube89\",\"lag\":0}],\"lag\":0},{\"topic\":\"CLICK-MOBILE-WEB-IN-TOPIC\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":3,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube89\",\"replicas\":\"ecube89,ecube87\",\"lag\":3}],\"lag\":0},{\"topic\":\"ENT6587-OUT01-topic\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":2,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube88\",\"replicas\":\"ecube88,ecube87\",\"lag\":2}],\"lag\":0},{\"topic\":\"CJO-IN-TOPIC\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":70,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube89\",\"replicas\":\"ecube89,ecube87\",\"lag\":70}],\"lag\":0},{\"topic\":\"ENT5741-OUT01-topic\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":1,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube89\",\"replicas\":\"ecube89,ecube88\",\"lag\":1}],\"lag\":0}]}";
		JsonObject json = new JsonObject(jsonString);
		System.out.println(json.getBoolean("success"));

		JsonArray offsetsArray = json.getJsonArray("results");

		int resultsCount = offsetsArray.size();

		if (resultsCount > 0) {
			boolean hasGroupID = false;
			String checkGroupID = offsetsArray.getJsonObject(0).getString("groupID");
			if (checkGroupID != null) {
				hasGroupID = true;
			}

			JsonObject offsetsObj = null;
			for (int i = 0; i < resultsCount; i++) {
				offsetsObj = offsetsArray.getJsonObject(i);

				if (hasGroupID) {
					System.out.println(offsetsObj.getJsonArray("offsetList").getJsonObject(0).getString("replicas"));
				} else {
					System.out.println("groupID is NULL");
				}

			}
			//System.out.println(offsetsArray.getJsonObject(0).getString("groupID"));

		} else {
			System.out.println("results is empty!");
		}
	}

	private void printJacksonJson() throws Exception {
		String jsonString = "{\"success\":true,\"results\":[{\"topic\":\"ENT6713-OUT01-topic\",\"groupID\":\"etc\",\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":3,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube88\",\"replicas\":\"ecube88,ecube87\",\"lag\":3}],\"lag\":0},{\"topic\":\"ENT3591-OUT01-topic\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":0,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube88\",\"replicas\":\"ecube88,ecube89\",\"lag\":0}],\"lag\":0},{\"topic\":\"CLICK-MOBILE-WEB-IN-TOPIC\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":3,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube89\",\"replicas\":\"ecube89,ecube87\",\"lag\":3}],\"lag\":0},{\"topic\":\"ENT6587-OUT01-topic\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":2,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube88\",\"replicas\":\"ecube88,ecube87\",\"lag\":2}],\"lag\":0},{\"topic\":\"CJO-IN-TOPIC\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":70,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube89\",\"replicas\":\"ecube89,ecube87\",\"lag\":70}],\"lag\":0},{\"topic\":\"ENT5741-OUT01-topic\",\"groupID\":null,\"endOffsetAll\":0,\"committedOffsetAll\":0,\"createDT\":\"2019-10-17 14:15:28\",\"offsetList\":[{\"partition\":0,\"endOffset\":1,\"committedOffset\":0,\"host\":null,\"consumerID\":null,\"clientID\":null,\"leader\":\"ecube89\",\"replicas\":\"ecube89,ecube88\",\"lag\":1}],\"lag\":0}]}";

		JsonNode jsonNode = new ObjectMapper().readTree(jsonString);
		System.out.println(jsonNode.get("success").asBoolean());

		JsonNode resultsArrayNode = jsonNode.path("results");

		int resultsCount = resultsArrayNode.size();
		if (resultsCount > 0) {
			boolean hasGroupID = false;
			String checkGroupID = resultsArrayNode.get(0).get("groupID").asText();
			if (checkGroupID != null) {
				hasGroupID = true;
			}

			JsonNode offsetListNode = null;
			for (int resultPos = 0; resultPos < resultsCount; resultPos++) {

				if (hasGroupID) {
					offsetListNode = resultsArrayNode.get(resultPos).path("offsetList");
					int offsetListNodeCount = offsetListNode.size();

					for (int offsetListPos = 0; offsetListPos < offsetListNodeCount; offsetListPos++) {
						System.out.println(offsetListNode.get(offsetListPos).get("replicas").asText());
					}
					
				} else {
					System.out.println("groupID is NULL");
				}

			}
		} else {
			System.out.println("results is empty!");
		}
	}
}
