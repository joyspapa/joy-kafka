<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ page import="java.util.*"%>
<%@ page import="org.slf4j.Logger,org.slf4j.LoggerFactory"%>
<%@ page
	import="java.net.*,java.io.*,com.fasterxml.jackson.databind.*,java.text.*"%>

<%!// 전역 변수 선언
	final Logger logger = LoggerFactory.getLogger(getClass());

	final String[] lineColor = {"#4169E1", "#6A5ACD", "#483D8B", "#48D1CC", "#4682B4", "#40E0D0" ,"#3CB371" ,"#5F9EA0" ,"#66CDAA" ,"#696969"};
	String baseUrl = "";
	final String synapseURL = "http://192.168.10.57:9081/kafka/";
	String hostAddress = null;
	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	int refreshSec = 2;
%>

<%
	//if(!(request.getRemoteAddr().startsWith("0"))) {
	//	logger.warn("OffsetViewerNew Clinet-Addr : " + request.getLocalAddr());
	//	logger.warn("OffsetViewerNew RemoteAddr : " + request.getRemoteAddr() + " , RemoteHost : " + request.getRemoteHost());
	//}

	if(hostAddress == null) {
		hostAddress = InetAddress.getLocalHost().getHostAddress();
		logger.warn("hostAddress is NULL");
	}
	StringBuilder sbBaseUrl = new StringBuilder();
	sbBaseUrl.append("http://");
	sbBaseUrl.append(hostAddress);
	sbBaseUrl.append(":8080/logplanet-ui-admin/OffsetViewer.jsp");
	
	String refreshParam = request.getParameter("refresh");
	if (refreshParam != null && refreshParam.length() > 0) {
		refreshSec = Integer.parseInt(refreshParam);
	}
	sbBaseUrl.append("?refresh=");
	sbBaseUrl.append(refreshSec);
	
	
	boolean isEarliest = false;
	int kafkaTimeout = 0;
	String includeTopic = "";

	String jsonString = null;
	
	// viewType : consumer | consumerAll | deploy | topic | report
	String viewTypeParam = request.getParameter("viewtype");
	String idParam = request.getParameter("id");

	boolean isAdminView = false;
	String refreshMeta = "";
	if (viewTypeParam != null) {
		if(!viewTypeParam.equals("admin")) {
			// Rest 호출
			jsonString = call("monitor", request.getRemoteAddr(), viewTypeParam, idParam);
			
			refreshMeta = "<meta http-equiv='refresh' content='" + refreshSec + "'>";
		} else {
			isAdminView = true;
		}
	}

	baseUrl = sbBaseUrl.toString();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">

<%=refreshMeta%>

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
	width: 40%;
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
<script src="./js/Chart.min.js"></script>
</head>
<body>
<%
	out.write(makeMenuHtml(isAdminView, request.getRemoteAddr()));

	// consumer | consumerAll | deploy | topic | report
	if(jsonString != null) {
		
		JsonNode jsonNode = new ObjectMapper().readTree(jsonString);
		if (jsonNode.get("success") != null && jsonNode.get("success").asBoolean() && jsonNode.path("results").size()>0) {

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
				
			case "reportChart":
				out.write(makeReportChartViewHtml(resultsArrayNode));
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
	public String makeMenuHtml(boolean isAdminView, String clientid) {
		StringBuilder menuView = new StringBuilder();
		menuView.append("<h3>");
		menuView.append("<a href='" + baseUrl + "' style='text-decoration: none'>Offset Viewer</a>");
		menuView.append("&nbsp;&nbsp;&nbsp;");
		menuView.append("<font size='2'>(<a href='" + baseUrl + "&viewtype=admin' style='text-decoration: none'>Topic Management</a>)</font>");
		menuView.append("</h3>");

		if (!isAdminView) {
			menuView.append("<h5>");
			menuView.append("<a href='" + baseUrl + "&viewtype=topic' style='text-decoration: none'>Topic 별 현황</a>");
			menuView.append("&nbsp;&nbsp;&nbsp;&nbsp;");
			menuView.append("<a href='" + baseUrl + "&viewtype=consumer' style='text-decoration: none'>ConsumerGroup 별 현황</a>");
			menuView.append("&nbsp;(<a href='" + baseUrl + "&viewtype=consumerAll' style='text-decoration: none'>ALL</a>)");
			menuView.append("&nbsp;&nbsp;&nbsp;&nbsp;");
			menuView.append("<a href='" + baseUrl + "&viewtype=deploy' style='text-decoration: none'>Deploy 별 현황</a>");
			menuView.append("</h5>");
		} else {
			menuView.append(makeAdminHtml(clientid));
		}

		logger.warn("makeMenuHtml={}", menuView.toString());
		return menuView.toString();
	}

	public String makeAdminHtml(String clientid) {
		StringBuilder adminView = new StringBuilder();
		adminView.append("<script>");
		adminView.append("function callAdminAction (command)");
		adminView.append("{");
		
		adminView.append("  var requestUrl = '" + makeCallUrl("admin" , clientid, null, null) +"';");
		adminView.append("  if(command == 'create') {");
		adminView.append("    document.getElementById(command).value=document.getElementById(command).value.toUpperCase();");
		adminView.append("    var topic = document.getElementById(command).value;");
		adminView.append("    var partition = document.adminForm.partition.value;");
		adminView.append("    var replicaFactor = document.adminForm.replicaFactor.value;");
		adminView.append("    if(topic.length == 0) {");
		adminView.append("      alert('TopicName must be filled out:' + topic);");
		adminView.append("      return false;");
		adminView.append("    }");
		adminView.append("    requestUrl = requestUrl + '/' + command + '/' + topic + '/' + partition + '/' + replicaFactor;");
		adminView.append("  } else if(command != '') {");
		adminView.append("    requestUrl = requestUrl + '/delete/' + command;");
		adminView.append("  } else {");
		adminView.append("    alert('command must be create or delete. :' + command);");
		adminView.append("    return false;");
		adminView.append("  }");
		
		adminView.append("  var xhr = new XMLHttpRequest();");
		adminView.append("  xhr.open('GET', requestUrl, true);");
		adminView.append("  xhr.send(null);");
		adminView.append("  xhr.onreadystatechange=function(){");
		adminView.append("    if(xhr.readyState == 4 && xhr.status == 200){");
	    adminView.append("      var json = JSON.parse(xhr.responseText); ");
	    adminView.append("      document.getElementById('actionResult').innerHTML = '<p>result : ' + json.message + ' (for confirmation, click the Topic_Management link)'");
		adminView.append("    } else {");
		adminView.append("      document.getElementById('actionResult').innerHTML = 'Failed status : ' + xhr.status + ', Contents : ' + xhr.responseText;");
		adminView.append("    }");
		adminView.append("  }");
		adminView.append("}");
		adminView.append("</script>");
		
		adminView.append("<form name='adminForm' >");
		//adminView.append("<font style='font-family:courier;' >");
		adminView.append("Topic 명 : <input type='text' name='topic-create' id='create' size='20' />&nbsp; &nbsp; ");
		adminView.append("Partition : <input type='text' name='partition' value='1' size='2' />&nbsp; &nbsp;");
		adminView.append("ReplicaFactor : <input type='text' name='replicaFactor' value='2' size='2' />&nbsp; &nbsp;");
		adminView.append("<a href=\"javascript:callAdminAction('create')\">Topic 생성</a>");
		adminView.append("<p>");
		
		///////////////////////
		// Topic List View
		String topicListJsonResult = call("admin", clientid, "list", "");
		try {
			JsonNode jsonNode = new ObjectMapper().readTree(topicListJsonResult);
			//logger.warn("topicListJsonResult = {}", topicListJsonResult);
			if (jsonNode.get("success") != null && jsonNode.get("success").asBoolean()) {
				JsonNode listNode = jsonNode.path("listTopic");
				adminView.append("<p>");
				adminView.append("<table class='GeneratedTable3'>");
				adminView.append("<tbody>");
				adminView.append("<tr>");
				adminView.append("<td align='center' colspan='2'>");
				adminView.append("Topic 명");
				adminView.append("</td>");
				adminView.append("</tr>");
			
				String bgcolor = "";
				for (short resultPos = 0; resultPos < listNode.size(); resultPos++) {
					bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";
					adminView.append("<tr " + bgcolor + ">");
					adminView.append("<td width='90%'>");
					adminView.append(listNode.get(resultPos).asText());
					adminView.append("</td>");
					adminView.append("<td align='center' width='10%'>");
					adminView.append("<a href=\"javascript:callAdminAction('" + listNode.get(resultPos).asText() + "')\">");
					adminView.append("삭제");
					adminView.append("</a>");
					adminView.append("</td>");
					adminView.append("</tr>");
					//logger.warn("topicListJsonResult ({}) = {}",resultPos, listNode.get(resultPos).asText());
				}
			
				adminView.append("</tbody>");
				adminView.append("</table");
			}
		
		} catch(Exception ex) {
			logger.warn("topicListJsonResult error : ", ex);
		}
		// Topic List View
		///////////////////////
		
		//adminView.append("</font>");
		adminView.append("</form>");

		adminView.append("<div><table><tr>");
		adminView.append("<td id='actionResult'>");
		adminView.append("</td></tr></table></div>");
		
		//logger.warn("makeAdminHtml={}", adminView.toString());
		return adminView.toString();
	}

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
		for (short resultPos = 0; resultPos < resultNode.size(); resultPos++) {
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
		deployView.append("</table>");

		//logger.warn("makeDeployViewHtml={}", deployView.toString());
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
		JsonNode offsetListNode = null;
		for (short resultPos = 0; resultPos < resultNode.size(); resultPos++) {
			offsetListNode = resultNode.get(resultPos).path("offsetList");
			bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";

			String topicName = null;
			for (short offsetListPos = 0; offsetListPos < offsetListNode.size(); offsetListPos++) {
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

		//logger.warn("makeTopicViewHtml={}", topicView.toString());
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
		JsonNode offsetListNode = null;
		for (short resultPos = 0; resultPos < resultNode.size(); resultPos++) {
			offsetListNode = resultNode.get(resultPos).path("offsetList");
			bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";

			String groupID = null;
			for (short offsetListPos = 0; offsetListPos < offsetListNode.size(); offsetListPos++) {
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

		//logger.warn("makeConsumerViewHtml={}", consumerView.toString());
		return consumerView.toString();
	}

	public String makeReportViewHtml(JsonNode resultNode) {
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

		JsonNode offsetListNode = null;
		String bgcolor = "";
		for (short resultPos = 0; resultPos < resultNode.size(); resultPos++) {
			String tdColor = "";
			bgcolor = (resultPos % 2 == 1) ? "bgcolor='#F0F0F0'" : "";

			reportView.append("<tr " + bgcolor + ">");
			reportView.append("<td " + tdColor + " >" );
			reportView.append("<a href='" + baseUrl + "&viewtype=reportChart&id=" + resultNode.get(resultPos).get("groupID").asText()
					+ "' style='text-decoration:none;color:black;'>");
			reportView.append(resultNode.get(resultPos).get("groupID").asText() + " (chart)");
			reportView.append("</a>");
			reportView.append("</td>");
			reportView.append("<td align='center' " + tdColor + " >"
					+ resultNode.get(resultPos).get("partition").asInt() + "</td>");
				
			offsetListNode = resultNode.get(resultPos).path("offsets");
			String lag = null;
			for (short offsetListPos = 0; offsetListPos < 10; offsetListPos++) {
				if (offsetListPos < offsetListNode.size()) {
					lag = String.format("%,10d", offsetListNode.get(offsetListPos).get("lag").asLong());
				} else {
					lag = "-";
				}

				reportView.append("<td align='right' " + tdColor + " >" + lag + "</td>");
			}
			reportView.append("</tr>");

		}

		reportView.append("</tbody>");
		reportView.append("</table>");

		//logger.warn("makeReportViewHtml={}", reportView.toString());
		return reportView.toString();
	}

	public String makeReportChartViewHtml(JsonNode resultNode) {
		StringBuilder reportChartView = new StringBuilder();

		short queueSize = 10;
			
		reportChartView.append("<div id='container' style='width: 40%; height: 30%;'>");
		reportChartView.append("<canvas id='offsetChart'></canvas>");
		reportChartView.append("</div>");
			
		reportChartView.append("<script>");
			
		reportChartView.append("var offsetCanvas = document.getElementById('offsetChart');");
		reportChartView.append("Chart.defaults.global.defaultFontFamily = 'Lato';");
		reportChartView.append("Chart.defaults.global.defaultFontSize = 10;");
		reportChartView.append("Chart.defaults.global.title.display = true;");
		reportChartView.append("Chart.defaults.global.title.position = 'bottom';");
		reportChartView.append("Chart.defaults.global.title.fontSize = 14;");
		reportChartView.append("Chart.defaults.global.title.text = 'Consumer Lag';");
		//reportChartView.append("Chart.defaults.global.animation.duration = 0;");
			
		String[] xDataVar = new String[resultNode.size()];
		JsonNode offsetListNode = null;
		for (short resultPos = 0; resultPos < resultNode.size(); resultPos++) {
			String yDataVar = "offsetData" + resultPos;
			xDataVar[resultPos] = yDataVar;
				
			reportChartView.append("var " + yDataVar + "={");
			reportChartView.append("label: '" + resultNode.path(resultPos).get("groupID").asText() + "-" + resultNode.path(resultPos).get("partition").asInt() + "',");
				
			reportChartView.append("data: [");
			offsetListNode = resultNode.get(resultPos).path("offsets");
			
			int lag = 0;
			for (short offsetListPos = 0; offsetListPos < queueSize; offsetListPos++) {
				if (offsetListPos < offsetListNode.size()) {
					lag = (int)offsetListNode.get(offsetListPos).get("lag").asLong();
				} else {
					lag = 0;
				}
				reportChartView.append(lag);
				if(offsetListPos < (queueSize-1)) {
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
		for(short qCnt = 0; qCnt < queueSize; qCnt++) {
			// >T+" + refreshSec * 2 + "
			if(qCnt == 0) {
				reportChartView.append("'T'");
			} else {
				reportChartView.append("'T+"+ refreshSec * qCnt +"'");
			}
			if(qCnt < (queueSize-1)) {
				reportChartView.append(",");
			}
		}
		reportChartView.append("],");
			
		reportChartView.append("datasets: [");
		for(short i = 0; i < xDataVar.length; i++) {
			reportChartView.append(xDataVar[i]);
			if(i < (xDataVar.length-1)) {
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

		//logger.warn("makeReportChartViewHtml={}", reportChartView.toString());
		return reportChartView.toString();
	}
	
%>

<%!// 메소드 정의
	public String makeCallUrl(String gubun, String clientid, String viewType, String id) {
		StringBuilder urlString = new StringBuilder();
		urlString.append(synapseURL);
		urlString.append(gubun);
		urlString.append("/");
		urlString.append(clientid);
		
		if(viewType != null && viewType.length() > 0) {
			urlString.append("/");
			urlString.append(viewType);
		}
		if (id != null && id.length() > 0) {
			urlString.append("/");
			urlString.append(id);
		}
		//logger.warn("makeCallUrl={}", urlString.toString());
		return urlString.toString();
	}

	public String call(String gubun, String clientid, String viewType, String id) {
		String responseBody = "";
		HttpURLConnection connection = null;
		try {

			URL url = new URL(makeCallUrl(gubun, clientid, viewType, (id == null) ? "none" : id));
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