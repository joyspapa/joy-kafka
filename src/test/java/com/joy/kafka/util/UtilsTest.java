package com.joy.kafka.util;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class UtilsTest {
	private static final Logger logger = LoggerFactory.getLogger(UtilsTest.class);
	
	String baseUrl = "";
    String synapseURL = "http://192.168.10.57:9081/kafka/monitor/";
    
    @Test
    public void testTeeMap() {
    	Map<Integer, String> tree = new TreeMap();
    	tree.put(2, "2");
    	tree.put(0, "0");
    	tree.put(1, "1");
    	
    	logger.debug("tree={}", tree);
    }
    
	@Test
	public void testQueue() {
		// https://blog.naver.com/pjok1122/221504688603
		Queue<String> queue = new LinkedList<String>();
		for(int i=0;i<3;i++) {
			queue.add("데이터-"+i); //큐에 데이터를 삽입한다.
		}
		//queue = ["데이터-0", "데이터-1", "데이터-2"], front=0, rear=2
		
		System.out.println("before : " + queue);
		System.out.println(queue.peek()); //큐의 front가 가리키는 값을 반환한다.         데이터-0
		//System.out.println(queue.poll()); //큐의 front가 가리키는 값을 반환하고 삭제한다.    데이터-0
		System.out.println("remove : " + queue.remove());
		System.out.println(queue.isEmpty()); // false
		System.out.println("after : " + queue);			//[데이터-1, 데이터-2]
		
		int a = 0;
		int b = 0;
		if(a <= b) {
			
		}
	}
	
	public String makeDeployViewHtml(JsonNode resultNode) {
		StringBuilder deployView = new StringBuilder();
		deployView.append("<p>");
		deployView.append("<table class='GeneratedTable3'>");
		deployView.append("  <tbody>");
		deployView.append("    <tr>");
		deployView.append("      <td>");
		deployView.append(resultNode.get(0).get("createDT").asText());
		deployView.append("      </td>");
		deployView.append("    </tr>");
		
		String bgcolor = "";
		String checkDeployName = "";
		int resultsCount = resultNode.size();
		for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
			bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";
			checkDeployName = resultNode.get(resultPos).get("deployName").asText();
			
		
			deployView.append("    <tr " + bgcolor + ">");
			deployView.append("      <td>");
			deployView.append("        <a href='" + baseUrl + "&viewtype=deploy&id=" + checkDeployName +"' style='text-decoration:none;color:black;'>");
			deployView.append(checkDeployName);
			deployView.append("        </a>");
			deployView.append("      </td>");
			deployView.append("    </tr>");
		}
		
		deployView.append("  </tbody>");
		deployView.append("</table");
		
		logger.warn("makeDeployViewHtml={}", deployView.toString());
		return deployView.toString();
	}
	
	public String makeTopicViewHtml(JsonNode resultNode) {
		StringBuilder topicView = new StringBuilder();
	
		topicView.append("<p>");
		topicView.append("<table class='GeneratedTable'>");
		topicView.append("	<tbody>");
		topicView.append("		<tr>");
		topicView.append("<td colspan='5'>" + resultNode.get(0).get("createDT").asText() + "</td>");
		topicView.append("</tr>");
		topicView.append("<tr>");
		topicView.append("<td align='center'>Topic</td>");
		topicView.append("<td align='center'>Partition</td>");
		topicView.append("<td align='center'>Offset</td>");
		topicView.append("<td align='center'>Leader</td>");
		topicView.append("<td align='center'>Replicas</td>");
		topicView.append("</tr>");
		
		String bgcolor = "";
		int resultsCount = resultNode.size();
		JsonNode offsetListNode = null;
		for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
			offsetListNode = resultNode.get(resultPos).path("offsetList");
			int offsetListNodeCount = offsetListNode.size();
			bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";

			String topicName = null;
			for (int offsetListPos = 0; offsetListPos < offsetListNodeCount; offsetListPos++) {
				topicName = resultNode.get(resultPos).get("topic").asText();
			
			topicView.append("    <tr " + bgcolor + ">");
			topicView.append("      <td>");
		
			if(offsetListPos == 0) {
				topicView.append("<a href='"+ baseUrl + "&viewtype=topic&id=" + topicName + "' style='text-decoration:none;color:black;'>");
				topicView.append(topicName);
				topicView.append("</a>");
			}
			topicView.append("      </td>");
			
			topicView.append("<td align='center'>" + offsetListNode.get(offsetListPos).get("partition").asInt() + "</td>");
			topicView.append("<td align='right'>>" + String.format("%,10d", offsetListNode.get(offsetListPos).get("endOffset").asLong()) + "</td>");
			topicView.append("<td align='center'>>" + offsetListNode.get(offsetListPos).get("leader").asText() + "</td>");
			topicView.append("<td align='center'>>" + offsetListNode.get(offsetListPos).get("replicas").asText() + "</td>");
			topicView.append("</tr>");
			}
		}
		
		topicView.append("</tbody>");
		topicView.append("</table>");
		
		logger.warn("makeTopicViewHtml={}", topicView.toString());
		return topicView.toString();
	}
	
	public String makeConsumerViewHtml(JsonNode resultNode) {
		StringBuilder consumerView = new StringBuilder();
	
		consumerView.append("<p>");
		consumerView.append("<table class='GeneratedTable2'>");
		consumerView.append("	<tbody>");
		consumerView.append("		<tr>");
		consumerView.append("<td colspan='9'>" + resultNode.get(0).get("createDT").asText() + "</td>");
		consumerView.append("</tr>");
		consumerView.append("<tr>");
		consumerView.append("<td align='center'>Topic</td>");
		consumerView.append("<td align='center'>Consumer</td>");
		consumerView.append("<td align='center'>Partition</td>");
		consumerView.append("<td align='center'>Host</td>");
		consumerView.append("<td align='center'>EndOffset</td>");
		consumerView.append("<td align='center'>CommittedOffset</td>");
		consumerView.append("<td align='center'>Lag</td>");
		consumerView.append("<td align='center'>Leader</td>");
		consumerView.append("<td align='center'>Replicas</td>");
		consumerView.append("</tr>");
	
		String bgcolor = "";
		int resultsCount = resultNode.size();
		JsonNode offsetListNode = null;
		for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
			offsetListNode = resultNode.get(resultPos).path("offsetList");
			int offsetListNodeCount = offsetListNode.size();
			bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";
			
			String groupID = null;
			for (int offsetListPos = 0; offsetListPos < offsetListNodeCount; offsetListPos++) {
				long lag = offsetListNode.get(offsetListPos).get("lag").asLong();
				String lagString = "";
				String tdColor = "";
				if(lag < 0) {
					tdColor = "style='color:#909090'";
					lagString = "STOPPED";
				} else {
					lagString = String.format("%,10d", offsetListNode.get(offsetListPos).get("lag").asLong());
				}
							
				groupID = resultNode.get(resultPos).get("groupID").asText();
				
				consumerView.append("    <tr " + bgcolor + ">");
				consumerView.append("      <td " + tdColor + ">");
				consumerView.append((offsetListPos == 0)? resultNode.get(resultPos).get("topic").asText():"");
				consumerView.append("      </td>");
				
				consumerView.append("      <td " + tdColor + ">");
				if(tdColor.length() == 0) {
					consumerView.append("<a href='"+ baseUrl + "&viewtype=report&id=" + groupID + "' style='text-decoration:none;color:black;'>");
					consumerView.append(groupID);
					consumerView.append("</a>");
				} else {
					consumerView.append(groupID);
				}
				consumerView.append("      </td>");
				
				consumerView.append("<td align='center'" + tdColor + " >" + offsetListNode.get(offsetListPos).get("partition").asInt() + "</td>");
				consumerView.append("<td align='center'" + tdColor + " >" + offsetListNode.get(offsetListPos).get("host").asText() + "</td>");
				consumerView.append("<td align='right'" + tdColor + " >" + String.format("%,10d", offsetListNode.get(offsetListPos).get("endOffset").asLong()) + "</td>");
				consumerView.append("<td align='right'" + tdColor + " >" + String.format("%,10d", offsetListNode.get(offsetListPos).get("committedOffset").asLong()) + "</td>");
				consumerView.append("<td align='right'" + tdColor + " >" + lagString + "</td>");
				consumerView.append("<td align='center'" + tdColor + " >" + offsetListNode.get(offsetListPos).get("leader").asText() + "</td>");
				consumerView.append("<td align='center'" + tdColor + " >" + offsetListNode.get(offsetListPos).get("replicas").asText() + "</td>");
				
				consumerView.append("</tr>");
			}
		}		
		
		consumerView.append("</tbody>");
		consumerView.append("</table>");
		
		logger.warn("makeConsumerViewHtml={}", consumerView.toString());
		return consumerView.toString();
	}
	
	public String makeReportViewHtml(JsonNode resultNode) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int refreshSec = 0;
		StringBuilder reportView = new StringBuilder();
	
		reportView.append("<p>");
		reportView.append("<table class='GeneratedTable2'>");
		reportView.append("	<tbody>");
		reportView.append("		<tr>");
		reportView.append("<td colspan='12'>Consumer Lag &nbsp;&nbsp;[ " + df.format(System.currentTimeMillis()) + " ]</td>");
		reportView.append("</tr>");
		reportView.append("<tr>");
		reportView.append("<td width='36%' align='center'>Consumer</td>");
		reportView.append("<td width='4%' align='center'>Partition</td>");
		reportView.append("<td width='6%' align='center'>T</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*2  +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*3  +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*4  +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*5  +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*6  +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*7  +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*8  +"</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec*9  +"</td>");
		reportView.append("</tr>");
		
	
		//JsonNode resultsArrayNode = jsonNode.path("results");
		//int resultsCount = resultsArrayNode.size();
		
		int resultsCount = resultNode.size();
		//JsonNode offsetListNode = null;
		
		if (resultsCount > 0) {
			JsonNode offsetListNode = null;
			String bgcolor = "";
			for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
				String tdColor = "";
				bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";
				
				reportView.append("<tr " + bgcolor + ">");
				reportView.append("<td " + tdColor + " >" + resultNode.get(resultPos).get("groupID").asText() + "</td>");
				reportView.append("<td align='center' " + tdColor + " >" + resultNode.get(resultPos).get("partition").asInt() + "</td>");
				
				offsetListNode = resultNode.get(resultPos).path("offsets");
				int offsetListNodeCount = offsetListNode.size();
				
				String lag = null;
				for (int offsetListPos = 0; offsetListPos < 10; offsetListPos++) {
					if(offsetListPos < offsetListNodeCount) {
						lag = String.format("%,10d", offsetListNode.get(offsetListPos).get("lag").asLong());
					} else {
						lag = "-";
					}
					
					reportView.append("<td align='right' " + tdColor + " >" + lag + "</td>");
				
				}
				reportView.append("</tr>");
				
			}
		}
		
		reportView.append("</tbody>");
		reportView.append("</table>");
		
		logger.warn("makeReportViewHtml={}", reportView.toString());
		return reportView.toString();
	}
}
