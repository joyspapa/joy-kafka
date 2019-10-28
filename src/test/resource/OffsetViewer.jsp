<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ page import="java.util.*"%>
<%@ page import="org.slf4j.Logger,org.slf4j.LoggerFactory"%>
<%@ page
	import="java.net.*,java.io.*,com.fasterxml.jackson.databind.*,java.text.*"%>

<%!// 전역 변수 선언
	final Logger logger = LoggerFactory.getLogger(getClass());

	String baseUrl = "";
	final String synapseURL = "http://192.168.10.171:9081/kafka/monitor/";
	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	int refreshSec = 2;
%>

<%
	StringBuilder sbBaseUrl = new StringBuilder();
	sbBaseUrl.append("http://");
	sbBaseUrl.append(InetAddress.getLocalHost().getHostAddress());
	sbBaseUrl.append(":8080/logplanet-ui-admin/OffsetViewer.jsp");
	//sbBaseUrl.append(request.getRemoteAddr());

	//baseUrl = "http://" + InetAddress.getLocalHost().getHostAddress() +":8080/logplanet-ui-admin/Offset.jsp?clientip="+request.getRemoteAddr();

	//int refreshSec = 2;
	boolean isEarliest = false;
	int kafkaTimeout = 0;
	String includeTopic = "";

	String jsonString = null;
	
	// viewType : consumer | consumerAll | deploy | topic | report
	String viewTypeParam = request.getParameter("viewtype");
	String idParam = request.getParameter("id");

	String refreshParam = request.getParameter("refresh");
	if (refreshParam != null && refreshParam.length() > 0) {
		refreshSec = Integer.parseInt(refreshParam);
	}
	sbBaseUrl.append("?refresh=");
	sbBaseUrl.append(refreshSec);
	
	baseUrl = sbBaseUrl.toString();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<%
	if (viewTypeParam != null) {
		// Rest 호출
		jsonString = call(request.getRemoteAddr(), viewTypeParam, idParam);
		out.write("<meta http-equiv='refresh' content='" + refreshSec + "'>");
	}
%>
<title>Offset Viewer of KAFKA Topic</title>
<!-- CSS Code: Place this code in the document's head (between the 'head' tags) -->
<style>
table.GeneratedTable {
	width: 50%;
	background-color: #ffffff;
	border-collapse: collapse;
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	color: #000000;
	font-size: small;
}

table.GeneratedTable td, table.GeneratedTable th {
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	padding: 2px;
}

table.GeneratedTable thead {
	background-color: #ffcc00;
}

table.GeneratedTable2 {
	width: 80%;
	background-color: #ffffff;
	border-collapse: collapse;
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	color: #000000;
	font-size: small;
}

table.GeneratedTable2 td, table.GeneratedTable2 th {
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	padding: 2px;
}

table.GeneratedTable2 thead {
	background-color: #ffcc00;
}

table.GeneratedTable3 {
	width: 30%;
	background-color: #ffffff;
	border-collapse: collapse;
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	color: #000000;
	font-size: small;
}

table.GeneratedTable3 td, table.GeneratedTable3 th {
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	padding: 2px;
}

table.GeneratedTable3 thead {
	background-color: #ffcc00;
}
</style>
<!-- Request Condition : refresh=3&#38;ip=192.168.10.82&#38;port=9092&#38;timeout=3000&#38;earliest=false&#38;include=topicA,topicB-->
</head>
<body>
	<h4>
		<a href="<%=baseUrl%>" style="text-decoration: none">Offset Viewer</a>
	</h4>
	<h5>
		<a href="<%=baseUrl%>&viewtype=topic" style="text-decoration: none">Topic 별 현황</a> 
		&nbsp;&nbsp;&nbsp;&nbsp; 
		<a href="<%=baseUrl%>&viewtype=consumer" style="text-decoration: none">ConsumerGroup 별 현황</a> 
		(<a href="<%=baseUrl%>&viewtype=consumerAll" style="text-decoration: none">ALL</a>) 
		&nbsp;&nbsp;&nbsp;&nbsp; 
		<a href="<%=baseUrl%>&viewtype=deploy" style="text-decoration: none">Deploy 별 현황</a>
	</h5>

<%
	// consumer | consumerAll | deploy | topic | report
	if(jsonString != null) {
		
		JsonNode jsonNode = new ObjectMapper().readTree(jsonString);
		if (jsonNode.get("success") != null && jsonNode.get("success").asBoolean()) {

			JsonNode resultsArrayNode = jsonNode.path("results");
			switch (jsonNode.get("viewType").asText()) {
			case "topic":
				out.write(makeTopicViewHtml(resultsArrayNode));
				break;
			
			case "consumer":
			case "consumerAll":
				out.write(makeConsumerViewHtml(resultsArrayNode));
				break;
				
			case "deploy":
				out.write(makeDeployViewHtml(resultsArrayNode));
				break;
				
			case "report":
				out.write(makeReportViewHtml(resultsArrayNode));
				break;
			}
		} else {
			out.write("<p>" + jsonString);	
		}
	}
%>

	<P>
</body>
</html>

<%! // Make HTML 메소드 정의
	public String makeDeployViewHtml(JsonNode resultNode) {
		StringBuilder deployView = new StringBuilder();
		deployView.append("<p>");
		deployView.append("<table class='GeneratedTable3'>");
		deployView.append("<tbody>");
		deployView.append("<tr>");
		deployView.append("<td>");
		deployView.append(resultNode.get(0).get("createDT").asText());
		deployView.append("</td>");
		deployView.append("</tr>");

		String bgcolor = "";
		String checkDeployName = "";
		int resultsCount = resultNode.size();
		for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
			bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";
			checkDeployName = resultNode.get(resultPos).get("deployName").asText();

			deployView.append("<tr " + bgcolor + ">");
			deployView.append("<td>");
			deployView.append("<a href='" + baseUrl + "&viewtype=deploy&id=" + checkDeployName
					+ "' style='text-decoration:none;color:black;'>");
			deployView.append(checkDeployName);
			deployView.append("</a>");
			deployView.append("</td>");
			deployView.append("</tr>");
		}

		deployView.append("</tbody>");
		deployView.append("</table");

		logger.warn("makeDeployViewHtml={}", deployView.toString());
		return deployView.toString();
	}

	public String makeTopicViewHtml(JsonNode resultNode) {
		StringBuilder topicView = new StringBuilder();

		topicView.append("<p>");
		topicView.append("<table class='GeneratedTable'>");
		topicView.append("<tbody>");
		topicView.append("<tr>");
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

				topicView.append("<tr " + bgcolor + ">");
				topicView.append("<td>");

				if (offsetListPos == 0) {
					topicView.append("<a href='" + baseUrl + "&viewtype=topic&id=" + topicName
							+ "' style='text-decoration:none;color:black;'>");
					topicView.append(topicName);
					topicView.append("</a>");
				}
				topicView.append("</td>");

				topicView.append(
						"<td align='center'>" + offsetListNode.get(offsetListPos).get("partition").asInt() + "</td>");
				topicView.append("<td align='right'>"
						+ String.format("%,10d", offsetListNode.get(offsetListPos).get("endOffset").asLong())
						+ "</td>");
				topicView.append(
						"<td align='center'>" + offsetListNode.get(offsetListPos).get("leader").asText() + "</td>");
				topicView.append(
						"<td align='center'>" + offsetListNode.get(offsetListPos).get("replicas").asText() + "</td>");
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
		consumerView.append("<tbody>");
		consumerView.append("<tr>");
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

				consumerView.append("<tr " + bgcolor + ">");
				consumerView.append("<td " + tdColor + ">");
				consumerView.append((offsetListPos == 0) ? resultNode.get(resultPos).get("topic").asText() : "");
				consumerView.append("</td>");

				consumerView.append("<td " + tdColor + ">");
				if (tdColor.length() == 0) {
					consumerView.append("<a href='" + baseUrl + "&viewtype=report&id=" + groupID
							+ "' style='text-decoration:none;color:black;'>");
					consumerView.append(groupID);
					consumerView.append("</a>");
				} else {
					consumerView.append(groupID);
				}
				consumerView.append("</td>");

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
		//SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//int refreshSec = 0;
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
	}%>

<%!// 메소드 정의
	public String makeCallUrl(String clientid, String viewType, String id) {
		StringBuilder urlString = new StringBuilder();
		urlString.append(synapseURL);
		urlString.append(clientid);
		urlString.append("/");
		urlString.append(viewType);
		if (id != null) {
			urlString.append("/");
			urlString.append(id);
		}
		logger.warn("makeCallUrl={}", urlString.toString());
		return urlString.toString();
	}

	public String call(String clientid, String viewType, String id) {
		String responseBody = "";
		HttpURLConnection connection = null;
		try {

			URL url = new URL(makeCallUrl(clientid, viewType, (id == null) ? "none" : id));
			connection = (HttpURLConnection) url.openConnection();
			connection.setConnectTimeout(2000);
			connection.setRequestMethod("GET");

			int code = connection.getResponseCode();

			try (BufferedReader input = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				String line;
				StringBuffer buffer = new StringBuffer();
				while ((line = input.readLine()) != null) {
					buffer.append(line);
				}
				responseBody = buffer.toString();
			}

			if (logger.isInfoEnabled()) {
				logger.debug("[call] jsonString : \n" + responseBody);
			}

			//availableConsumerList.add(consumerHost);

		} catch (Exception ex) {
			logger.warn("[call] HttpURLConnection error : " + ex.toString());
			responseBody = "{\"success\": false, \"errormessage\": \"" + ex.toString() + "\" }";
		} finally {
			if (connection != null) {
				try {
					connection.disconnect();
				} catch (Exception e) {
					logger.warn("[call] HttpURLConnection.close(), error=" + e.toString());
				}
			}
		}

		return responseBody;
	}
%>