package com.joy.kafka.util;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UtilsTest {
	private static final Logger logger = LoggerFactory.getLogger(UtilsTest.class);

	String baseUrl = "";
	String synapseURL = "http://192.168.10.57:9081/kafka/monitor/";
	//String hostAddress = InetAddress.getLocalHost().getHostAddress();

	@Test
	public void testEncoding() throws UnsupportedEncodingException {
		String s = "移댄��怨�由�";
		String conv = new String(s.getBytes(), "ksc5601");
		System.out.println(conv);
	}
	
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
		for (int i = 0; i < 3; i++) {
			queue.add("데이터-" + i); //큐에 데이터를 삽입한다.
		}
		//queue = ["데이터-0", "데이터-1", "데이터-2"], front=0, rear=2

		System.out.println("before : " + queue);
		System.out.println(queue.peek()); //큐의 front가 가리키는 값을 반환한다.         데이터-0
		//System.out.println(queue.poll()); //큐의 front가 가리키는 값을 반환하고 삭제한다.    데이터-0
		System.out.println("remove : " + queue.remove());
		System.out.println(queue.isEmpty()); // false
		System.out.println("after : " + queue); //[데이터-1, 데이터-2]

		int a = 0;
		int b = 0;
		if (a <= b) {

		}
	}

	public String makeMenuHtml(boolean isAdminView) {
		StringBuilder menuView = new StringBuilder();
		menuView.append("<h4>");
		menuView.append("<a href='" + baseUrl + "' style='text-decoration: none'>Offset Viewer</a>");
		menuView.append("&nbsp;&nbsp;");
		menuView.append("<a href='" + baseUrl + "&viewtype=admin' style='text-decoration: none'>>Topic 생성/삭제</a>");
		menuView.append("</h4>");

		if (!isAdminView) {
			menuView.append("<h5>");
			menuView.append("<a href='" + baseUrl + "&viewtype=topic' style='text-decoration: none'>Topic 별 현황</a>");
			menuView.append("&nbsp;&nbsp;&nbsp;&nbsp;");
			menuView.append("<a href='" + baseUrl + "&viewtype=consumer' style='text-decoration: none'>ConsumerGroup 별 현황</a>");
			menuView.append("(<a href='" + baseUrl + "&viewtype=consumerAll' style='text-decoration: none'>ALL</a>)");
			menuView.append("&nbsp;&nbsp;&nbsp;&nbsp;");
			menuView.append("<a href='" + baseUrl + "&viewtype=deploy' style='text-decoration: none'>Deploy 별 현황</a>");
			menuView.append("</h5>");
		} else {
			menuView.append(makeAdminHtml());
		}

		logger.warn("makeMenuHtml={}", menuView.toString());
		return menuView.toString();
	}

	public String makeAdminHtml() {
		StringBuilder adminView = new StringBuilder();
		adminView.append("<script>");
		adminView.append("function callAdminAction (command)");
		adminView.append("{");
		adminView.append("  var topic = document.getElementById(command).value;");
		adminView.append("  var partition = document.adminForm.partition.value;");
		adminView.append("  var replicaFactor = document.adminForm.replicaFactor.value;");
		adminView.append("  if(topic.length == 0) {");
		adminView.append("   alert('TopicName must be filled out:' + topic);");
		adminView.append("   return false;");
		adminView.append("  }");
		adminView.append("  var requestUrl = 'http://192.168.20.178:9081/kafka/admin/192.168.20.131/';");
		adminView.append("  if(command == 'create') {");
		adminView.append("requestUrl = requestUrl + command + '/' + topic + '/' + partition + '/' + replicaFactor;");
		adminView.append("} else if(command == 'delete') {");
		adminView.append("requestUrl = requestUrl + command + '/' + topic;");
		adminView.append("} else {");
		adminView.append("alert('command must be create or delete. :' + command);");
		adminView.append("return false;");
		adminView.append("}");
		adminView.append("var xhr = new XMLHttpRequest();");
		adminView.append("xhr.open('GET', requestUrl, true);");
		adminView.append("xhr.send(null);");
		adminView.append("xhr.onreadystatechange=function(){");
		adminView.append("if(xhr.readyState == 4 && xhr.status == 200){");
	    adminView.append("var json = JSON.parse(xhr.responseText); ");
	    adminView.append("document.getElementById('actionResult').innerHTML = 'result : ' + json.success + ', message : ' + json.message ");
		adminView.append("} else {");
		adminView.append("document.getElementById('actionResult').innerHTML = 'Failed status : ' + xhr.status + ', Contents : ' + xhr.responseText; ");
		adminView.append("}");
		adminView.append("}");
		adminView.append("}");
		adminView.append("</script>");
		adminView.append("<form name='adminForm' >");
		adminView.append("Topic 명 : <input type='text' name='topic-create' id='create' size='20' />&nbsp; &nbsp; ");
		adminView.append("Partition : <input type='text' name='partition' value='1' size='2' />&nbsp; &nbsp;");
		adminView.append("ReplicaFactor : <input type='text' name='replicaFactor' value='1' size='2' />&nbsp; &nbsp;");
		adminView.append("<a href=\"javascript:callAdminAction('create')\">Topic 생성</a>");
		adminView.append("<p>");
		adminView.append("Topic 명 : <input type='text' name='topic-delete' id='delete' size='20' />&nbsp; &nbsp;");
		adminView.append("<a href=\"javascript:callAdminAction('delete')\">Topic 삭제</a>");
		adminView.append("<p>");
		adminView.append("<div id='actionResult'></div>");
		adminView.append("</form>");

		logger.warn("makeAdminHtml={}", adminView.toString());
		return adminView.toString();
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
			bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";
			checkDeployName = resultNode.get(resultPos).get("deployName").asText();

			deployView.append("    <tr " + bgcolor + ">");
			deployView.append("      <td>");
			deployView.append("        <a href='" + baseUrl + "&viewtype=deploy&id=" + checkDeployName
					+ "' style='text-decoration:none;color:black;'>");
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
			bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";

			String topicName = null;
			for (int offsetListPos = 0; offsetListPos < offsetListNodeCount; offsetListPos++) {
				topicName = resultNode.get(resultPos).get("topic").asText();

				topicView.append("    <tr " + bgcolor + ">");
				topicView.append("      <td>");

				if (offsetListPos == 0) {
					topicView.append("<a href='" + baseUrl + "&viewtype=topic&id=" + topicName
							+ "' style='text-decoration:none;color:black;'>");
					topicView.append(topicName);
					topicView.append("</a>");
				}
				topicView.append("      </td>");

				topicView.append(
						"<td align='center'>" + offsetListNode.get(offsetListPos).get("partition").asInt() + "</td>");
				topicView.append("<td align='right'>>"
						+ String.format("%,10d", offsetListNode.get(offsetListPos).get("endOffset").asLong())
						+ "</td>");
				topicView.append(
						"<td align='center'>>" + offsetListNode.get(offsetListPos).get("leader").asText() + "</td>");
				topicView.append(
						"<td align='center'>>" + offsetListNode.get(offsetListPos).get("replicas").asText() + "</td>");
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
			bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";

			String groupID = null;
			for (int offsetListPos = 0; offsetListPos < offsetListNodeCount; offsetListPos++) {
				long lag = offsetListNode.get(offsetListPos).get("lag").asLong();
				String lagString = "";
				String tdColor = "";
				if (lag < 0) {
					tdColor = "style='color:#909090'";
					lagString = "STOPPED";
				} else {
					lagString = String.format("%,10d", offsetListNode.get(offsetListPos).get("lag").asLong());
				}

				groupID = resultNode.get(resultPos).get("groupID").asText();

				consumerView.append("    <tr " + bgcolor + ">");
				consumerView.append("      <td " + tdColor + ">");
				consumerView.append((offsetListPos == 0) ? resultNode.get(resultPos).get("topic").asText() : "");
				consumerView.append("      </td>");

				consumerView.append("      <td " + tdColor + ">");
				if (tdColor.length() == 0) {
					consumerView.append("<a href='" + baseUrl + "&viewtype=report&id=" + groupID
							+ "' style='text-decoration:none;color:black;'>");
					consumerView.append(groupID);
					consumerView.append("</a>");
				} else {
					consumerView.append(groupID);
				}
				consumerView.append("      </td>");

				consumerView.append("<td align='center'" + tdColor + " >"
						+ offsetListNode.get(offsetListPos).get("partition").asInt() + "</td>");
				consumerView.append("<td align='center'" + tdColor + " >"
						+ offsetListNode.get(offsetListPos).get("host").asText() + "</td>");
				consumerView.append("<td align='right'" + tdColor + " >"
						+ String.format("%,10d", offsetListNode.get(offsetListPos).get("endOffset").asLong())
						+ "</td>");
				consumerView.append("<td align='right'" + tdColor + " >"
						+ String.format("%,10d", offsetListNode.get(offsetListPos).get("committedOffset").asLong())
						+ "</td>");
				consumerView.append("<td align='right'" + tdColor + " >" + lagString + "</td>");
				consumerView.append("<td align='center'" + tdColor + " >"
						+ offsetListNode.get(offsetListPos).get("leader").asText() + "</td>");
				consumerView.append("<td align='center'" + tdColor + " >"
						+ offsetListNode.get(offsetListPos).get("replicas").asText() + "</td>");

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
		reportView.append("<tbody>");
		reportView.append("<tr>");
		reportView.append(
				"<td colspan='12'>Consumer Lag &nbsp;&nbsp;[ " + df.format(System.currentTimeMillis()) + " ]</td>");
		reportView.append("</tr>");
		reportView.append("<tr>");
		reportView.append("<td width='36%' align='center'>Consumer</td>");
		reportView.append("<td width='4%' align='center'>Partition</td>");
		reportView.append("<td width='6%' align='center'>T</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 2 + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 3 + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 4 + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 5 + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 6 + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 7 + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 8 + "</td>");
		reportView.append("<td width='6%' align='center'>T+" + refreshSec * 9 + "</td>");
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
				bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";

				reportView.append("<tr " + bgcolor + ">");
				reportView
						.append("<td " + tdColor + " >" + resultNode.get(resultPos).get("groupID").asText() + "</td>");
				reportView.append("<td align='center' " + tdColor + " >"
						+ resultNode.get(resultPos).get("partition").asInt() + "</td>");

				offsetListNode = resultNode.get(resultPos).path("offsets");
				int offsetListNodeCount = offsetListNode.size();

				String lag = null;
				for (int offsetListPos = 0; offsetListPos < 10; offsetListPos++) {
					if (offsetListPos < offsetListNodeCount) {
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

	@Test
	public void testReportChart() throws Exception {
		String jsonString = "{\"success\":true,\"results\":[{\"groupID\":\"groupID-HCS_TEST-DE1570154270-ENT2112-CUSTOMLOGIC_IMDG\",\"partition\":0,\"offsets\":[{\"endOffset\":90219,\"committedOffset\":90219,\"createDT\":1572248784119,\"lag\":0},{\"endOffset\":90219,\"committedOffset\":90219,\"createDT\":1572248782025,\"lag\":0},{\"endOffset\":90219,\"committedOffset\":90219,\"createDT\":1572248779940,\"lag\":0},{\"endOffset\":90219,\"committedOffset\":90219,\"createDT\":1572248777773,\"lag\":0}]},{\"groupID\":\"groupID-HCS_TEST-DE1570154270-ENT2112-CUSTOMLOGIC_IMDG\",\"partition\":1,\"offsets\":[{\"endOffset\":97894,\"committedOffset\":97894,\"createDT\":1572248784116,\"lag\":0},{\"endOffset\":97894,\"committedOffset\":97894,\"createDT\":1572248782021,\"lag\":0},{\"endOffset\":97894,\"committedOffset\":97894,\"createDT\":1572248779937,\"lag\":0},{\"endOffset\":97894,\"committedOffset\":97894,\"createDT\":1572248777770,\"lag\":0}]}],\"viewType\":\"report\",\"report\":true}";
		JsonNode jsonNode = new ObjectMapper().readTree(jsonString);
		makeReportChartViewHtml(jsonNode.path("results"));
	}

	public String makeReportChartViewHtml(JsonNode resultNode) {
		SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
		int refreshSec = 2;

		StringBuilder reportChartView = new StringBuilder();

		short queueSize = 10;
		int resultsCount = resultNode.size();
		if (resultsCount > 0) {

			reportChartView.append("<div id='container' style='width: 40%; height: 30%;'>");
			reportChartView.append("<canvas id='offsetChart'></canvas>");
			reportChartView.append("</div>");

			reportChartView.append("<script>");

			reportChartView.append("var offsetCanvas = document.getElementById('offsetChart');");
			reportChartView.append("Chart.defaults.global.defaultFontFamily = 'Lato';");
			reportChartView.append("Chart.defaults.global.defaultFontSize = 10;");

			String[] lineColor = { "#4169E1", "#6A5ACD", "#483D8B", "#48D1CC", "#4682B4", "#40E0D0", "#3CB371",
					"#5F9EA0", "#66CDAA", "#696969" };
			String[] xDataVar = new String[resultsCount];
			JsonNode offsetListNode = null;
			for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
				String yDataVar = "offsetData" + resultPos;
				xDataVar[resultPos] = yDataVar;

				reportChartView.append("var " + yDataVar + "={");
				reportChartView.append("label: '" + resultNode.path(resultPos).get("groupID").asText() + "-"
						+ resultNode.path(resultPos).get("partition").asInt() + "',");

				reportChartView.append("data: [");
				offsetListNode = resultNode.get(resultPos).path("offsets");
				int offsetListNodeCount = offsetListNode.size();
				int lag = 0;
				for (int offsetListPos = 0; offsetListPos < queueSize; offsetListPos++) {
					if (offsetListPos < offsetListNodeCount) {
						lag = (int) offsetListNode.get(offsetListPos).get("lag").asLong();
					} else {
						lag = 0;
					}
					reportChartView.append(lag);
					if (offsetListPos < (queueSize - 1)) {
						reportChartView.append(",");
					}
				}
				reportChartView.append("],");
				reportChartView.append("lineTension: 0,");
				reportChartView.append("fill: false,");
				reportChartView.append("borderColor: '" + lineColor[resultPos] + "'");
				reportChartView.append("};");
			}

			reportChartView.append("var xData={");
			reportChartView.append("labels: [");
			for (int qCnt = 0; qCnt < queueSize; qCnt++) {
				// >T+" + refreshSec * 2 + "
				if (qCnt == 0) {
					reportChartView.append("'T'");
				} else {
					reportChartView.append("'T+" + refreshSec * qCnt + "'");
				}
				if (qCnt < (queueSize - 1)) {
					reportChartView.append(",");
				}
			}
			reportChartView.append("],");

			reportChartView.append("datasets: [");
			for (int i = 0; i < xDataVar.length; i++) {
				reportChartView.append(xDataVar[i]);
				if (i < (xDataVar.length - 1)) {
					reportChartView.append(",");
				}
			}
			reportChartView.append("]");
			reportChartView.append("};");

			reportChartView.append("var chartOptions = {");
			reportChartView.append("scales: { yAxes: [{ ticks: { beginAtZero: true } }] }");
			reportChartView.append("};");

			reportChartView.append("var lineChart = new Chart(offsetCanvas, {");
			reportChartView.append("type: 'line',");
			reportChartView.append("data: xData,");
			reportChartView.append("options: chartOptions");
			reportChartView.append("});");

			reportChartView.append("</script>");
		}

		logger.warn("makeReportChartViewHtml={}", reportChartView.toString());
		return reportChartView.toString();
	}
}
